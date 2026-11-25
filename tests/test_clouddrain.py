import pathlib
import tempfile
import threading
import unittest
import clouddrain
from clouddrain import permit,evacuate,restore
from clouddrain.cli import run
from clouddrain.store import open_db,one

class DrainTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.db=open_db(str(pathlib.Path(self.temp.name)/"run.sqlite"))
    def tearDown(self):
        self.db.close();self.temp.cleanup()
    def base(self,kind="stateless",volume_id=None):
        permit.node(self.db,"a","uid-a",1,"z1",4,4)
        permit.node(self.db,"b","uid-b",1,"z2",4,4)
        permit.guard(self.db,"svc",1)
        evacuate.workload(self.db,"w","svc","a","z1",1,1,kind,"v1",1,volume_id)
        permit.ticket(self.db,"t","a","uid-a",1,10)
        return permit.approve(self.db,"t",2)
    def test_imported_module_is_local(self):
        print("module_file",clouddrain.__file__)
        self.assertIn("solo11-0241",str(pathlib.Path(clouddrain.__file__).resolve()))
    def test_protection_budget_and_stale_node(self):
        self.assertEqual(self.base(),"approved")
        evacuate.workload(self.db,"w2","svc","b","z2",1,1,"stateless","v1",1)
        permit.ticket(self.db,"t2","b","uid-b",1,10)
        self.assertEqual(permit.approve(self.db,"t2",2),"protection_budget_exhausted")
        permit.node(self.db,"a","new-uid",2,"z1",4,4)
        self.assertEqual(permit.approve(self.db,"t",3),"stale_node_identity")

    def test_parallel_tickets_consume_one_protection_slot(self):
        permit.node(self.db,"a","ua",1,"z1",4,4)
        permit.node(self.db,"b","ub",1,"z2",4,4)
        permit.guard(self.db,"svc",1)
        evacuate.workload(self.db,"wa","svc","a","z1",1,1,"stateless","v1",1)
        evacuate.workload(self.db,"wb","svc","b","z2",1,1,"stateless","v1",1)
        permit.ticket(self.db,"ta","a","ua",1,10)
        permit.ticket(self.db,"tb","b","ub",1,10)
        path=one(self.db,"PRAGMA database_list")["file"]
        barrier=threading.Barrier(2)
        results=[]
        def approve(ticket_id):
            db=open_db(path)
            barrier.wait()
            results.append(permit.approve(db,ticket_id,2))
            db.close()
        workers=[threading.Thread(target=approve,args=(name,)) for name in ("ta","tb")]
        for worker in workers:worker.start()
        for worker in workers:worker.join()
        self.assertCountEqual(results,["approved","protection_budget_exhausted"])
    def test_emergency_requires_two_roles(self):
        permit.node(self.db,"a","u",1,"z1",4,4)
        permit.ticket(self.db,"t","a","u",1,10,True)
        permit.approval(self.db,"a1","t","operator","alice",10)
        self.assertEqual(permit.approve(self.db,"t",2),"missing_two_roles")
        permit.approval(self.db,"a2","t","manager","bob",10)
        self.assertEqual(permit.approve(self.db,"t",2),"approved")

    def test_window_domain_receipt_and_health_boundaries(self):
        permit.node(self.db,"a","ua",1,"z1",4,4)
        permit.node(self.db,"b","ub",1,"z1",4,4)
        permit.guard(self.db,"svc",1)
        evacuate.workload(self.db,"w","svc","a","z1",1,1,"stateless","v1",1)
        evacuate.workload(self.db,"w2","svc","b","z1",1,1,"stateless","v1",1)
        permit.ticket(self.db,"t","a","ua",2,10)
        self.assertEqual(permit.approve(self.db,"t",1),"outside_window")
        self.assertEqual(permit.approve(self.db,"t",3),"approved")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"no_safe_target")
        permit.node(self.db,"c","uc",1,"z3",4,4)
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:c")
        self.assertEqual(evacuate.receipt(self.db,"r","e","ub",1,"v1"),"stale_target_receipt")
        self.assertEqual(evacuate.receipt(self.db,"r2","e","uc",1,"v1"),"moved")
        self.assertEqual(restore.certify(self.db,"t",5),"health_incomplete:disk")
    def test_local_volume_block_and_write_handoff(self):
        self.base("portable","vol")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:b")
        self.assertEqual(evacuate.receipt(self.db,"r","e","uid-b",1,"v1"),"waiting_volume_handoff")
        self.assertEqual(restore.inspect(self.db,"t"),"stranded:1")
        self.assertEqual(evacuate.volume_receipt(self.db,"bar","e","target_barrier","uid-b",1,1,5),"recorded")
        self.assertEqual(evacuate.volume_handoff(self.db,"e"),"waiting_write_barrier")
        self.assertEqual(one(self.db,"SELECT owner_node FROM volume WHERE id='vol'")["owner_node"],"a")
        self.assertEqual(evacuate.volume_receipt(self.db,"bad","e","source_detached","wrong-uid",1,1,4),"stale_volume_receipt")
        self.assertEqual(evacuate.volume_receipt(self.db,"det","e","source_detached","uid-a",1,1,4),"recorded")
        self.assertEqual(evacuate.volume_receipt(self.db,"det","e","source_detached","uid-a",1,1,4),"duplicate")
        self.assertEqual(evacuate.volume_handoff(self.db,"e"),"write_owner_moved")
        self.assertEqual(evacuate.volume_handoff(self.db,"e"),"already_moved")
        self.assertEqual(one(self.db,"SELECT owner_node FROM volume WHERE id='vol'")["owner_node"],"b")
        self.assertEqual(restore.inspect(self.db,"t"),"empty")
    def test_orphan_blocks_healthy_node(self):
        self.base()
        evacuate.plan(self.db,"e","t","w")
        evacuate.receipt(self.db,"r","e","uid-b",1,"v1")
        evacuate.workload(self.db,"new","svc","a","z1",1,1,"stateless","v1",1)
        self.assertEqual(restore.orphan(self.db,"o","t","new"),"quarantined")
        for kind in ("disk","network","runtime"):
            restore.probe(self.db,kind,"a",1,kind,4,True)
        self.assertEqual(restore.certify(self.db,"t",5),"orphan_quarantined")

    def test_unregistered_load_cannot_be_hidden_by_skipping_inspection(self):
        self.base()
        evacuate.plan(self.db,"e","t","w")
        evacuate.receipt(self.db,"r","e","uid-b",1,"v1")
        evacuate.workload(self.db,"new","svc","a","z1",1,1,"stateless","v1",1)
        for kind in ("disk","network","runtime"):
            restore.probe(self.db,kind,"a",1,kind,4,True)
        self.assertEqual(restore.certify(self.db,"t",5),"orphan_unregistered")
        self.assertEqual(restore.inspect(self.db,"t"),"orphan:1")
        self.assertEqual(restore.certify(self.db,"t",5),"orphan_quarantined")
        self.assertEqual(restore.orphan(self.db,"bad","t","new","wrong",True),"quarantined")
        restore.ownership(self.db,"new","tenant-x",1,20)
        self.assertEqual(restore.orphan(self.db,"proof","t","new","tenant-x",True),"claimed")
        self.assertEqual(restore.certify(self.db,"t",5),"orphan_claimed_pending")

    def test_emergency_local_data_loss_requires_separate_waiver(self):
        permit.node(self.db,"a","uid-a",1,"z1",4,4)
        permit.node(self.db,"b","uid-b",1,"z2",4,4)
        evacuate.workload(self.db,"local","svc","a","z1",1,1,"local","v1",1,"vol")
        permit.ticket(self.db,"t","a","uid-a",1,10,True)
        permit.approval(self.db,"a1","t","operator","alice",10)
        permit.approval(self.db,"a2","t","manager","bob",10)
        self.assertEqual(permit.approve(self.db,"t",2),"approved")
        self.assertEqual(evacuate.plan(self.db,"e","t","local",True),"local_volume_block")
        self.assertEqual(permit.waiver(self.db,"t","local","carol","loss accepted by tenant"),"recorded")
        self.assertEqual(evacuate.plan(self.db,"e","t","local",True),"planned:b")

    def test_workload_and_eviction_replay_do_not_duplicate(self):
        self.base()
        self.assertEqual(evacuate.workload(self.db,"w","svc","a","z1",1,1,"stateless","v1",1),"exists")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"planned:b")
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"already_planned")
        self.assertEqual(evacuate.plan(self.db,"e","t","other"),"eviction_identity_conflict")

    def test_node_rebuild_invalidates_approved_eviction_source(self):
        self.base()
        permit.node(self.db,"a","rebuilt",2,"z1",4,4)
        self.assertEqual(evacuate.plan(self.db,"e","t","w"),"stale_source_node")
        for kind in ("disk","network","runtime"):
            restore.probe(self.db,kind,"a",2,kind,5,True)
        self.assertEqual(restore.certify(self.db,"t",6),"stale_node_identity")
    def test_six_fixtures(self):
        root=pathlib.Path(__file__).resolve().parents[1]
        expected={"permit_normal":"approved","permit_failure":"protection_budget_exhausted","evacuate_normal":"moved","evacuate_failure":"local_volume_block","restore_normal":"closed","restore_failure":"orphan_quarantined"}
        for name,want in expected.items():
            data=run(str(pathlib.Path(self.temp.name)/(name+".sqlite")),root/"fixtures"/(name+".json"))
            self.assertEqual(data["results"][-1]["result"],want,name)
            self.assertTrue(data["state"]["audit_replay"]["consistent"],name)

    def test_three_vertical_fixtures_replay_from_row_events(self):
        root=pathlib.Path(__file__).resolve().parents[1]
        cases={
            "path_budget_to_eviction":("protection_budget_exhausted","approved"),
            "path_volume_to_close":("stranded_workload","closed"),
            "path_orphan_to_close":("orphan_claimed_pending","closed"),
        }
        for name,(blocked,final) in cases.items():
            data=run(str(pathlib.Path(self.temp.name)/(name+".sqlite")),root/"fixtures"/(name+".json"))
            results=[x["result"] for x in data["results"]]
            self.assertIn(blocked,results,name)
            self.assertEqual(results[-1],final,name)
            self.assertTrue(data["state"]["audit_replay"]["consistent"],name)
            self.assertGreater(data["state"]["audit_replay"]["events"],5,name)

if __name__=="__main__":unittest.main()
