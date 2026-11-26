import pathlib
import tempfile
import threading
import unittest

from clouddrain import evacuate,permit,restore
from clouddrain.store import one,open_db,rows,tx


class TargetContentionTest(unittest.TestCase):
    """Maintenance handoff must be one recoverable reservation.

    The plan is a promise, not a landing.  The landing is only valid when the
    target, the source identity and a fresh capacity check all agree at the
    moment the source workload would actually leave.
    """

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=str(pathlib.Path(self.temp.name)/"run.sqlite")
        self.db=open_db(self.path)
        self.reopen()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def reopen(self):
        """Simulate a process restart: a brand new connection to the same file."""
        if hasattr(self,"db"):
            try:self.db.close()
            except Exception:pass
        self.db=open_db(self.path)

    def node(self,node_id,uid,zone,cpu=4,mem=4):
        self.assertEqual(permit.node(self.db,node_id,uid,1,zone,cpu,mem),"accepted")

    def contended(self):
        """The exact reported counterexample.

        A/B/C each have 4 CPU and 4 memory.  w needs 3 of each on A.  After the
        ticket is approved A is unschedulable and w is still on A.  w is planned
        to B.  Then unrelated business x (3+3) is registered on B.  B's valid
        receipt for w used to return moved and leave B at 6/4.
        """
        self.node("a","uid-a","z1")
        self.node("b","uid-b","z2")
        self.node("c","uid-c","z3")
        self.assertEqual(permit.guard(self.db,"svc",1),"accepted")
        self.assertEqual(permit.guard(self.db,"other",1),"accepted")
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",3,3,"stateless","v1",1),"registered")
        self.assertEqual(permit.ticket(self.db,"t","a","uid-a",1,10),"draft")
        self.assertEqual(permit.approve(self.db,"t",2),"approved")
        self.assertEqual(one(self.db,"SELECT schedulable FROM nodes WHERE id='a'")["schedulable"],0)
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"a")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:b")
        self.assertEqual(one(self.db,"SELECT status FROM eviction WHERE id='e'")["status"],"planned")
        self.assertEqual(evacuate.workload(self.db,"x","other","b","z2",3,3,"stateless","v1",1),"registered")

    def place_x_on_c(self,at=4):
        self.assertEqual(restore.ownership(self.db,"x","tenant-x",1,20),"recorded")
        self.assertEqual(evacuate.relocate(self.db,"rx","x","c",owner_generation=1,at=at,
                                           tenant="tenant-x"),"relocating:c")
        self.assertEqual(evacuate.relocation_receipt(self.db,"rc","rx","uid-c",1,"v1",at=at+1),"moved")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"c")

    def usage(self,node_id):
        return one(self.db,"SELECT COALESCE(SUM(cpu),0) cpu,COALESCE(SUM(mem),0) mem "
                           "FROM workloads WHERE node_id=?",(node_id,))

    def assert_source_intact(self):
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"a")
        self.assertEqual(one(self.db,"SELECT status FROM tickets WHERE id='t'")["status"],"approved")
        self.assertEqual(one(self.db,"SELECT uid FROM nodes WHERE id='a'")["uid"],"uid-a")
        self.assertEqual(one(self.db,"SELECT workload_generation,version FROM source_binding "
                                      "WHERE ticket_id='t' AND workload_id='w'"),
                         {"workload_generation":1,"version":"v1"})

    def assert_no_overcommit(self):
        self.assertLessEqual(self.usage("a")["cpu"],4)
        self.assertLessEqual(self.usage("b")["cpu"],4)
        self.assertLessEqual(self.usage("c")["cpu"],4)
        self.assertLessEqual(self.usage("b")["mem"],4)

    def assert_single_active_promise(self,workload_id):
        active=rows(self.db,"SELECT * FROM target_promise WHERE workload_id=? AND status='active'",(workload_id,))
        self.assertLessEqual(len(active),1)

    # --- the reported counterexample ------------------------------------

    def test_receipt_for_overcommitted_target_keeps_w_on_source(self):
        self.contended()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.assert_source_intact()
        self.assert_no_overcommit()
        self.assertEqual(self.usage("b")["cpu"],3)
        # The promise survives: only a brand-new receipt may use it later.
        self.assert_single_active_promise("w")
        self.assertEqual(one(self.db,"SELECT status FROM eviction WHERE id='e'")["status"],"planned")
        self.assertEqual(one(self.db,"SELECT status FROM target_promise WHERE status='active'")["status"],"active")
        # The rejection is retained so it can be audited, not silently retried.
        rejected=one(self.db,"SELECT * FROM placement_attempt WHERE receipt_id='rb'")
        self.assertEqual((rejected["status"],rejected["reason"]),("rejected","target_overcommitted"))

    def test_rejected_receipt_cannot_be_resent_to_force_the_move(self):
        self.contended()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"receipt_already_rejected")
        for i in range(3):
            # A fresh receipt is re-checked every time and still finds B full.
            self.assertEqual(evacuate.receipt(self.db,"fresh-%d"%i,"e","uid-b",1,"v1"),"target_overcommitted")
            self.assertEqual(evacuate.receipt(self.db,"fresh-%d-again"%i,"e","uid-b",1,"v1"),"target_overcommitted")
            self.assert_source_intact()
            self.assert_no_overcommit()
            self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"b")
            # Retrying the same id never reopens a landing.
            self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"receipt_already_rejected")
        self.reopen()
        self.assert_source_intact()
        self.assert_no_overcommit()

    # --- disposition 1: x's owner approves, x -> C, then w -> B ----------

    def test_disposition_one_relocate_x_then_fresh_receipt_takes_w_to_b(self):
        self.contended()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        # The B promise is still active; moving x away frees real capacity for it.
        self.place_x_on_c(at=4)
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"c")
        self.assertEqual(self.usage("b")["cpu"],0)
        self.assertEqual(evacuate.retarget(self.db,"e",at=5),"planned:b")
        # The old B receipt was already answered and stays dead.
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"receipt_already_rejected")
        self.assert_source_intact()
        self.assertEqual(evacuate.receipt(self.db,"rb-new","e","uid-b",1,"v1",at=6),"moved")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"b")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"c")
        self.assert_no_overcommit()
        self.assert_single_active_promise("w")
        # A can be maintained: its snapshot workloads have all left.
        self.assertEqual(restore.inspect(self.db,"t"),"empty")
        self.assertEqual(one(self.db,"SELECT status FROM tickets WHERE id='t'")["status"],"approved")

    def test_disposition_one_never_moves_x_without_its_owner_grant(self):
        self.contended()
        self.assertEqual(evacuate.relocate(self.db,"rx","x","c",owner_generation=1,at=4,
                                           tenant="tenant-x"),"ownership_not_granted")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"b")
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.assert_source_intact()

    # --- disposition 2: x's owner refuses, w -> C -------------------------

    def test_disposition_two_retarget_moves_w_to_c_and_leaves_x_alone(self):
        self.contended()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.assertEqual(evacuate.retarget(self.db,"e",at=3),"planned:c")
        # An old B receipt -- resent under its old id or forged under a new one
        # -- must not be borrowed for C.
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"receipt_already_rejected")
        self.assertEqual(evacuate.receipt(self.db,"rb-forged","e","uid-b",1,"v1"),"stale_target_receipt")
        self.assert_source_intact()
        self.assertEqual(evacuate.receipt(self.db,"rc","e","uid-c",1,"v1",at=4),"moved")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"c")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"b")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"b")
        self.assert_no_overcommit()
        self.assert_single_active_promise("w")
        self.assertEqual(restore.inspect(self.db,"t"),"empty")

    def test_retarget_requires_a_target_to_confirm_its_own_identity(self):
        self.contended()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.assertEqual(evacuate.retarget(self.db,"e",at=3),"planned:c")
        self.assertEqual(evacuate.receipt(self.db,"rc","e","uid-b",1,"v1",at=4),"stale_target_receipt")
        self.assert_source_intact()
        # Even a correct UID from another node is not a C identity.
        self.assertEqual(evacuate.receipt(self.db,"rc2","e","uid-a",1,"v1",at=4),"stale_target_receipt")
        self.assertEqual(evacuate.receipt(self.db,"rc3","e","uid-c",2,"v1",at=4),"stale_target_receipt")
        self.assertEqual(evacuate.receipt(self.db,"rc4","e","uid-c",1,"v2",at=4),"stale_target_receipt")
        self.assert_source_intact()
        self.assertEqual(evacuate.receipt(self.db,"rc5","e","uid-c",1,"v1",at=4),"moved")

    # --- failure keeps the source -----------------------------------------

    def test_failed_confirmation_keeps_source_service_and_binding(self):
        self.contended()
        before=one(self.db,"SELECT * FROM workloads WHERE id='w'")
        for receipt_id,uid,generation,version in (
            ("bad-uid","uid-a",1,"v1"),
            ("bad-gen","uid-b",2,"v1"),
            ("bad-version","uid-b",1,"v2"),
        ):
            self.assertEqual(evacuate.receipt(self.db,receipt_id,"e",uid,generation,version),
                             "stale_target_receipt")
            self.assertEqual(one(self.db,"SELECT * FROM workloads WHERE id='w'"),before)
            self.assert_source_intact()
        self.assertEqual(evacuate.receipt(self.db,"over","e","uid-b",1,"v1"),"target_overcommitted")
        self.assertEqual(one(self.db,"SELECT * FROM workloads WHERE id='w'"),before)
        self.assert_source_intact()

    # --- concurrency -------------------------------------------------------

    def parallel(self,workers):
        barrier=threading.Barrier(len(workers))
        results=[]
        lock=threading.Lock()
        def run(fn):
            db=open_db(self.path)
            try:
                barrier.wait()
                value=fn(db)
            finally:
                db.close()
            with lock:results.append(value)
        threads=[threading.Thread(target=run,args=(fn,)) for fn in workers]
        for thread in threads:thread.start()
        for thread in threads:thread.join()
        return results

    def test_concurrent_receipts_produce_one_landing_only(self):
        self.contended()
        self.assertEqual(evacuate.retarget(self.db,"e",at=3),"planned:c")

        def send(receipt_id):
            return lambda db: evacuate.receipt(db,receipt_id,"e","uid-c",1,"v1",at=4)

        results=self.parallel([send("r-%d"%i) for i in range(6)])
        self.assertEqual(results.count("moved"),1,results)
        self.assertCountEqual([x for x in results if x!="moved"],
                              ["already_moved"]*(len(results)-1))
        self.reopen()
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"c")
        self.assert_no_overcommit()
        self.assert_single_active_promise("w")
        self.assertEqual(one(self.db,"SELECT COUNT(*) n FROM target_receipt")["n"],1)

    def test_concurrent_plans_for_one_workload_reserve_once(self):
        self.node("a","uid-a","z1")
        self.node("b","uid-b","z2")
        self.node("c","uid-c","z3")
        self.assertEqual(permit.guard(self.db,"svc",1),"accepted")
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",3,3,"stateless","v1",1),"registered")
        self.assertEqual(permit.ticket(self.db,"t","a","uid-a",1,10),"draft")
        self.assertEqual(permit.approve(self.db,"t",2),"approved")

        def make(plan_id):
            return lambda db: evacuate.plan(db,plan_id,"t","w")

        results=self.parallel([make("e-%d"%i) for i in range(6)])
        moved=[x for x in results if x.startswith("planned:")]
        self.assertEqual(len(moved),1,results)
        self.assertCountEqual([x for x in results if not x.startswith("planned:")],
                              ["workload_already_promised"]*(len(results)-1))
        self.reopen()
        self.assertEqual(one(self.db,"SELECT COUNT(*) n FROM target_promise WHERE status='active'")["n"],1)

    def test_concurrent_approvals_do_not_duplicate_source_binding(self):
        self.node("a","uid-a","z1")
        self.node("b","uid-b","z2")
        self.assertEqual(permit.guard(self.db,"svc",1),"accepted")
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",3,3,"stateless","v1",1),"registered")
        self.assertEqual(permit.ticket(self.db,"t","a","uid-a",1,10),"draft")

        results=self.parallel([lambda db: permit.approve(db,"t",2) for _ in range(6)])
        self.assertEqual(results,["approved"]*6)
        self.reopen()
        self.assertEqual(one(self.db,"SELECT COUNT(*) n FROM source_binding WHERE ticket_id='t'")["n"],1)
        self.assertEqual(one(self.db,"SELECT COUNT(*) n FROM snapshot WHERE ticket_id='t'")["n"],1)

    # --- crash / reopen ----------------------------------------------------

    def test_restart_after_conflict_needs_a_fresh_promise_and_receipt(self):
        self.contended()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.reopen()
        self.assert_source_intact()
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"receipt_already_rejected")
        self.assertEqual(evacuate.retarget(self.db,"e",at=3),"planned:c")
        self.reopen()
        self.assertEqual(evacuate.receipt(self.db,"rb-new","e","uid-b",1,"v1",at=4),"stale_target_receipt")
        self.assertEqual(evacuate.receipt(self.db,"rc","e","uid-c",1,"v1",at=4),"moved")
        self.reopen()
        self.assertEqual(evacuate.receipt(self.db,"rc","e","uid-c",1,"v1",at=4),"duplicate")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"c")
        self.assertEqual(one(self.db,"SELECT COUNT(*) n FROM target_receipt")["n"],1)

    def test_restart_mid_volume_handoff_keeps_source_until_barrier(self):
        self.node("a","uid-a","z1")
        self.node("b","uid-b","z2")
        self.node("c","uid-c","z3")
        self.assertEqual(permit.guard(self.db,"svc",1),"accepted")
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",2,2,"portable","v1",1,"vol"),"registered")
        self.assertEqual(permit.ticket(self.db,"t","a","uid-a",1,10),"draft")
        self.assertEqual(permit.approve(self.db,"t",2),"approved")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:b")
        self.assertEqual(evacuate.workload(self.db,"x","other","b","z2",3,3,"stateless","v1",1),"registered")
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"a")
        self.assertEqual(one(self.db,"SELECT owner_node FROM volume WHERE id='vol'")["owner_node"],"a")
        self.reopen()
        self.assertEqual(one(self.db,"SELECT target_node FROM volume WHERE id='vol'")["target_node"],"b")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"a")
        self.assertEqual(self.usage("b")["cpu"],3)

    # --- domain safety -----------------------------------------------------

    def test_volume_handoff_is_rechecked_at_the_final_move(self):
        self.node("a","uid-a","z1")
        self.node("b","uid-b","z2")
        self.node("c","uid-c","z3")
        self.assertEqual(permit.guard(self.db,"svc",1),"accepted")
        self.assertEqual(permit.guard(self.db,"other",1),"accepted")
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",3,3,"portable","v1",1,"vol"),"registered")
        self.assertEqual(permit.ticket(self.db,"t","a","uid-a",1,10),"draft")
        self.assertEqual(permit.approve(self.db,"t",2),"approved")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:b")
        self.assertEqual(evacuate.workload(self.db,"x","other","b","z2",3,3,"stateless","v1",1),"registered")
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        # The volume must not leave A while B is overcommitted.
        self.assertEqual(evacuate.volume_receipt(self.db,"det","e","source_detached","uid-a",1,1,3),"recorded")
        self.assertEqual(evacuate.volume_receipt(self.db,"bar","e","target_barrier","uid-b",1,1,4),"recorded")
        self.assertEqual(one(self.db,"SELECT status FROM eviction WHERE id='e'")["status"],"planned")
        self.assertEqual(evacuate.retarget(self.db,"e",at=5),"planned:c")
        self.assertEqual(evacuate.volume_receipt(self.db,"bar-b","e","target_barrier","uid-b",1,1,6),"stale_volume_receipt")
        self.assertEqual(evacuate.receipt(self.db,"rc","e","uid-c",1,"v1",at=7),"waiting_volume_handoff")
        self.assertEqual(evacuate.volume_receipt(self.db,"det-c","e","source_detached","uid-a",1,1,8),"recorded")
        self.assertEqual(evacuate.volume_receipt(self.db,"bar-c","e","target_barrier","uid-c",1,1,9),"recorded")
        self.assertEqual(evacuate.volume_handoff(self.db,"e",at=10),"write_owner_moved")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"c")
        self.assertEqual(one(self.db,"SELECT owner_node FROM volume WHERE id='vol'")["owner_node"],"c")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='x'")["node_id"],"b")
        self.assert_no_overcommit()
        self.assertEqual(restore.inspect(self.db,"t"),"empty")

    def test_target_zone_collision_is_refused_at_confirmation(self):
        self.node("a","uid-a","z1")
        self.node("b","uid-b","z2")
        self.assertEqual(permit.guard(self.db,"svc",2),"accepted")
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",1,1,"stateless","v1",1),"registered")
        self.assertEqual(permit.ticket(self.db,"t","a","uid-a",1,10),"draft")
        self.assertEqual(permit.approve(self.db,"t",2),"approved")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:b")
        # A second replica of svc lands in b's domain while the receipt is in
        # flight; the receipt must notice the domain clash, not just the UID.
        self.assertEqual(evacuate.workload(self.db,"w2","svc","b","z2",1,1,"stateless","v2",1),"registered")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"a")
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_domain_conflict")
        self.assertEqual(one(self.db,"SELECT node_id FROM workloads WHERE id='w'")["node_id"],"a")

    # --- accounting retains failures and windows ---------------------------

    def test_conflict_history_and_service_window_are_retained(self):
        self.contended()
        window=one(self.db,"SELECT start_ts,end_ts FROM tickets WHERE id='t'")
        self.assertEqual(evacuate.receipt(self.db,"rb","e","uid-b",1,"v1"),"target_overcommitted")
        self.place_x_on_c(at=4)
        self.assertEqual(evacuate.retarget(self.db,"e",at=5),"planned:b")
        self.assertEqual(evacuate.receipt(self.db,"rb2","e","uid-b",1,"v1",at=6),"moved")
        attempts=rows(self.db,"SELECT * FROM placement_attempt ORDER BY rowid")
        self.assertTrue(any(a["status"]=="rejected" and a["reason"]=="target_overcommitted" for a in attempts))
        self.assertTrue(any(a["status"]=="accepted" and a["reason"]=="moved" for a in attempts))
        relocations=rows(self.db,"SELECT * FROM relocation_attempt ORDER BY rowid")
        self.assertEqual([r["result"] for r in relocations],["relocating","moved"])
        promises=[p["status"] for p in rows(self.db,"SELECT status FROM target_promise ORDER BY rowid")]
        self.assertIn("released",promises)
        self.assertEqual(promises.count("committed"),2)  # x's relocation and w's handoff
        self.assertEqual(one(self.db,"SELECT start_ts,end_ts FROM tickets WHERE id='t'"),window)


if __name__=="__main__":
    unittest.main()
