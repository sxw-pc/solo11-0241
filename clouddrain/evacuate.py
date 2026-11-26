from .store import one,rows,tx

def workload(db,workload_id,service,node_id,zone,cpu,mem,kind,version,generation,volume_id=None):
    with tx(db):
        old=one(db,"SELECT * FROM workloads WHERE id=?",(workload_id,))
        if old:
            current=(old["service"],old["node_id"],old["cpu"],old["mem"],old["kind"],old["volume_id"],old["version"],old["generation"])
            incoming=(service,node_id,cpu,mem,kind,volume_id,version,generation)
            return "exists" if current==incoming else "workload_identity_conflict"
        db.execute("INSERT INTO workloads VALUES(?,?,?,?,?,?,?,?,?,?)",(workload_id,service,node_id,zone,cpu,mem,kind,volume_id,version,generation))
        if volume_id:
            db.execute("INSERT OR IGNORE INTO volume(id,owner_node,generation) VALUES(?,?,?)",(volume_id,node_id,generation))
        return "registered"


def _bind_source(db,ticket,node):
    """Freeze the source identity a ticket is allowed to hand off.

    The binding is written once, when the ticket becomes approved (or when the
    first plan is made for a workload that was registered after approval).  A
    target receipt is only meaningful while the workload still matches it.
    """
    for w in rows(db,"SELECT * FROM workloads WHERE node_id=?",(node["id"],)):
        db.execute("INSERT OR IGNORE INTO source_binding VALUES(?,?,?,?,?,?)",
                   (ticket["id"],w["id"],node["id"],node["uid"],w["generation"],w["version"]))


def _source_binding(db,ticket_id,workload_id):
    return one(db,"SELECT * FROM source_binding WHERE ticket_id=? AND workload_id=?",(ticket_id,workload_id))


def _source_still_valid(db,ticket,workload):
    """Return the source node when the workload is still safely tied to it."""
    source=one(db,"SELECT * FROM nodes WHERE id=?",(ticket["node_id"],))
    if not source or source["uid"]!=ticket["node_uid"]:return None
    binding=_source_binding(db,ticket["id"],workload["id"])
    if binding:
        if (binding["node_id"]!=workload["node_id"] or binding["node_uid"]!=source["uid"]
                or binding["workload_generation"]!=workload["generation"]
                or binding["version"]!=workload["version"]):
            return None
    elif workload["node_id"]!=ticket["node_id"]:
        return None
    return source


def _active_promise(db,owner_kind,owner_id):
    return one(db,"SELECT * FROM target_promise WHERE owner_kind=? AND owner_id=? AND status='active'",(owner_kind,owner_id))


def _promise_for_workload(db,workload_id):
    return one(db,"SELECT * FROM target_promise WHERE workload_id=? AND status='active'",(workload_id,))


def _ensure_promise(db,eviction,at=0):
    """Recreate the reservation for a plan made before the promise table existed.

    Only an eviction that is still planned/ready may regain a promise; a
    released or moved one never resurrects a landing route.
    """
    if eviction["status"] not in ("planned","ready"):return None
    promise=_active_promise(db,"eviction",eviction["id"])
    if promise:return promise
    node=one(db,"SELECT * FROM nodes WHERE id=?",(eviction["target_id"],))
    if not node:return None
    revision=one(db,"SELECT COUNT(*) n FROM target_promise WHERE owner_kind='eviction' AND owner_id=?",
                 (eviction["id"],))["n"]
    db.execute("""INSERT INTO target_promise(id,owner_kind,owner_id,workload_id,target_id,target_uid,status,created_at,reason)
                  VALUES(?, 'eviction',?,?,?,?,'active',?,?)""",
               ("promise:"+eviction["id"]+":"+str(revision),eviction["id"],eviction["workload_id"],
                node["id"],node["uid"],at,"restored"))
    return _active_promise(db,"eviction",eviction["id"])


def _capacity_used(db,node_id):
    return one(db,"SELECT COALESCE(SUM(cpu),0) cpu,COALESCE(SUM(mem),0) mem FROM workloads WHERE node_id=?",(node_id,))


def _capacity_reserved(db,node_id,exclude_kind=None,exclude_id=None,exclude_workload=None):
    """Resource promised to unfinished handoffs landing on this node.

    A promise counts once its target is on the same node the promise names; the
    owning workload is only skipped for the handoff currently being confirmed.
    """
    clauses=["tp.status='active'","tp.target_id=?"]
    args=[node_id]
    if exclude_kind is not None:
        clauses.append("NOT (tp.owner_kind=? AND tp.owner_id=?)")
        args.extend([exclude_kind,exclude_id])
    if exclude_workload is not None:
        clauses.append("tp.workload_id<>?")
        args.append(exclude_workload)
    return one(db,"""SELECT COALESCE(SUM(w.cpu),0) cpu,COALESCE(SUM(w.mem),0) mem
        FROM target_promise tp JOIN workloads w ON w.id=tp.workload_id
        WHERE """+" AND ".join(clauses),tuple(args))


def _fits(db,node,workload,exclude_kind=None,exclude_id=None):
    """Real load + other unfinished reservations + this workload."""
    used=_capacity_used(db,node["id"])
    reserved=_capacity_reserved(db,node["id"],exclude_kind,exclude_id,workload["id"])
    return (node["cpu"]-used["cpu"]-reserved["cpu"]>=workload["cpu"]
            and node["mem"]-used["mem"]-reserved["mem"]>=workload["mem"])


def _domain_conflict(db,service,workload_id,zone,exclude_kind=None,exclude_id=None):
    """Replicas of one service must stay in different failure domains.

    Both workloads that are already serving and workloads whose active promise
    is about to land in a zone count, so two concurrent handoffs cannot quietly
    stack replicas into one domain.
    """
    arguments=[service,workload_id,zone]
    if one(db,"""SELECT w.id FROM workloads w JOIN nodes n ON n.id=w.node_id
        WHERE w.service=? AND w.id<>? AND n.zone=? LIMIT 1""",tuple(arguments)):
        return True
    clauses=["tp.status='active'","w.service=?","w.id<>?","n.zone=?"]
    arguments=[service,workload_id,zone]
    if exclude_kind is not None:
        clauses.append("NOT (tp.owner_kind=? AND tp.owner_id=?)")
        arguments.extend([exclude_kind,exclude_id])
    return one(db,"""SELECT w.id FROM target_promise tp JOIN workloads w ON w.id=tp.workload_id
        JOIN nodes n ON n.id=tp.target_id WHERE """+" AND ".join(clauses),tuple(arguments)) is not None


def _choose_target(db,source_id,workload,exclude_kind=None,exclude_id=None):
    occupied={x["zone"] for x in rows(db,"""SELECT n.zone FROM workloads w JOIN nodes n ON n.id=w.node_id
        WHERE w.service=? AND w.id<>?""",(workload["service"],workload["id"]))}
    clauses=["tp.status='active'","w.service=?"]
    args=[workload["service"]]
    if exclude_kind is not None:
        clauses.append("NOT (tp.owner_kind=? AND tp.owner_id=?)")
        args.extend([exclude_kind,exclude_id])
    occupied.update(x["zone"] for x in rows(db,"""SELECT n.zone FROM target_promise tp
        JOIN workloads w ON w.id=tp.workload_id JOIN nodes n ON n.id=tp.target_id
        WHERE """+" AND ".join(clauses),tuple(args)))
    for node in rows(db,"SELECT * FROM nodes WHERE id<>? AND schedulable=1 ORDER BY id",(source_id,)):
        if node["zone"] in occupied:continue
        if _fits(db,node,workload,exclude_kind,exclude_id):return node
    return None


def _record_attempt(db,receipt_id,owner_kind,owner_id,target_id,target_uid,
                    workload_generation,version,at,status,reason):
    """Persist one confirmation attempt; the first decision for a receipt wins.

    Re-sending the same receipt id never reopens a second valid landing: the
    original outcome is returned instead.  Returns the recorded row.
    """
    if receipt_id is not None:
        old=one(db,"""SELECT * FROM placement_attempt
            WHERE receipt_id=? AND owner_kind=? AND owner_id=?""",(receipt_id,owner_kind,owner_id))
        if old:return old
        attempt_id="rcpt:"+owner_kind+":"+owner_id+":"+receipt_id
    else:
        attempt_id=_generated_attempt_tag(db,"attempt:"+owner_kind+":"+owner_id)
    db.execute("""INSERT INTO placement_attempt(id,receipt_id,owner_kind,owner_id,target_id,target_uid,
        workload_generation,version,at,status,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
               (attempt_id,receipt_id,owner_kind,owner_id,target_id,target_uid,workload_generation,
                version,at,status,reason))
    return one(db,"SELECT * FROM placement_attempt WHERE id=?",(attempt_id,))


def _replay_receipt(db,receipt_id,owner_kind,owner_id,target_uid):
    """Return the original answer for a re-sent receipt, else None.

    A re-sent receipt can only ever complete the handoff it was addressed to.
    It cannot be lent to another eviction/target, and when that handoff moved
    on to a fresh promise the old receipt stays dead.
    """
    prior=one(db,"SELECT * FROM placement_attempt WHERE receipt_id=?",(receipt_id,))
    if not prior:return None
    same=(prior["owner_kind"]==owner_kind and prior["owner_id"]==owner_id
          and prior["target_uid"]==target_uid)
    if not same:return "receipt_identity_conflict"
    if prior["status"]=="accepted":
        if prior["reason"]=="moved":return "duplicate"
        return "duplicate_acknowledged"
    return "receipt_already_rejected"


def _promise_revision(db,owner_kind,owner_id):
    """Monotonic revision of the target promise; retargeting bumps it."""
    return one(db,"SELECT COUNT(*) n FROM target_promise WHERE owner_kind=? AND owner_id=?",
               (owner_kind,owner_id))["n"]


def _generated_attempt_tag(db,prefix):
    n=one(db,"SELECT COUNT(*) n FROM placement_attempt WHERE id LIKE ?",(prefix+":%",))["n"]
    return prefix+":"+str(n+1)


def plan(db,eviction_id,ticket_id,workload_id,allow_data_loss=False,at=0):
    with tx(db):
        old=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if old:
            return "already_planned" if old["ticket_id"]==ticket_id and old["workload_id"]==workload_id else "eviction_identity_conflict"
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        w=one(db,"SELECT * FROM workloads WHERE id=?",(workload_id,))
        if not t or t["status"]!="approved" or not w or w["node_id"]!=t["node_id"]:
            return "not_eligible_ticket"
        source=one(db,"SELECT * FROM nodes WHERE id=?",(t["node_id"],))
        if not source or source["uid"]!=t["node_uid"]:
            return "stale_source_node"
        if w["kind"]=="local":
            waiver=one(db,"SELECT * FROM data_loss_waiver WHERE ticket_id=? AND workload_id=?",(ticket_id,workload_id))
            if not allow_data_loss or not t["urgent"] or not waiver:
                return "local_volume_block"
        if _promise_for_workload(db,workload_id):return "workload_already_promised"
        _bind_source(db,t,source)
        n=_choose_target(db,t["node_id"],w)
        if not n:return "no_safe_target"
        db.execute("INSERT INTO eviction VALUES(?,?,?,?,?)",(eviction_id,ticket_id,workload_id,n["id"],"planned"))
        db.execute("""INSERT INTO target_promise(id,owner_kind,owner_id,workload_id,target_id,target_uid,status,created_at)
                      VALUES(?, 'eviction',?,?,?,?,'active',?)""",
                   ("promise:"+eviction_id+":"+str(at),eviction_id,workload_id,n["id"],n["uid"],at))
        if w["volume_id"]:
            db.execute("UPDATE volume SET target_node=? WHERE id=?",(n["id"],w["volume_id"]))
        return "planned:"+n["id"]


def receipt(db,receipt_id,eviction_id,target_uid,workload_generation,version,at=0):
    with tx(db):
        replay=_replay_receipt(db,receipt_id,"eviction",eviction_id,target_uid)
        if replay:return replay
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e:return "no_active_eviction"
        if e["status"]=="moved":return "already_moved"
        if e["status"] not in ("planned","ready"):return "no_active_eviction"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        t=one(db,"SELECT * FROM tickets WHERE id=?",(e["ticket_id"],))
        p=_ensure_promise(db,e,at)
        if not p:return "no_active_eviction"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(p["target_id"],))
        if not n or n["uid"]!=target_uid or p["target_uid"]!=target_uid:
            _record_attempt(db,receipt_id,"eviction",eviction_id,p["target_id"],target_uid,
                            workload_generation,version,at,"rejected","stale_target_receipt")
            return "stale_target_receipt"
        if not w or w["generation"]!=workload_generation or w["version"]!=version:
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","stale_target_receipt")
            return "stale_target_receipt"
        if not t or t["status"]!="approved":
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","ticket_not_active")
            return "ticket_not_active"
        if not _source_still_valid(db,t,w):
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","stale_source_identity")
            return "stale_source_identity"
        if not n["schedulable"]:
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","target_not_schedulable")
            return "target_not_schedulable"
        if _domain_conflict(db,w["service"],w["id"],n["zone"],"eviction",eviction_id):
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","target_domain_conflict")
            return "target_domain_conflict"
        # A plan that was legal when written is re-checked at the moment the
        # source workload would actually leave.  On failure nothing moves: the
        # source keeps serving, the target keeps its reservation, and only a
        # new readiness receipt (or an explicit retarget) can change the plan.
        if not _fits(db,n,w,"eviction",eviction_id):
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","target_overcommitted")
            return "target_overcommitted"
        if one(db,"SELECT id FROM target_receipt WHERE id=?",(receipt_id,)):return "duplicate"
        db.execute("INSERT INTO target_receipt VALUES(?,?,?,?,?)",(receipt_id,eviction_id,target_uid,workload_generation,version))
        if not w["volume_id"]:
            db.execute("UPDATE target_promise SET status='committed',ended_at=? WHERE id=?",(at,p["id"]))
            db.execute("UPDATE workloads SET node_id=?,zone=? WHERE id=?",(n["id"],n["zone"],w["id"]))
            db.execute("UPDATE eviction SET status='moved' WHERE id=?",(eviction_id,))
            _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                            workload_generation,version,at,"accepted","moved")
            return "moved"
        db.execute("UPDATE eviction SET status='ready' WHERE id=?",(eviction_id,))
        _record_attempt(db,receipt_id,"eviction",eviction_id,n["id"],target_uid,
                        workload_generation,version,at,"accepted","waiting_volume_handoff")
        return "waiting_volume_handoff"


def release(db,eviction_id,reason="released",at=0):
    """Withdraw an unfinished target promise without touching the source."""
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e:return "unknown_eviction"
        if e["status"]=="moved":return "already_moved"
        p=_active_promise(db,"eviction",eviction_id)
        if p:
            db.execute("UPDATE target_promise SET status='released',ended_at=?,reason=? WHERE id=?",(at,reason,p["id"]))
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if w and w["volume_id"]:
            db.execute("UPDATE volume SET target_node=NULL WHERE id=? AND target_node=?",(w["volume_id"],e["target_id"]))
        db.execute("UPDATE eviction SET status='released' WHERE id=?",(eviction_id,))
        return "released"


def retarget(db,eviction_id,at=0,target_id=None):
    """Revoke the old reservation and build a fresh promise for another target.

    The source workload and every old receipt stay exactly where they were;
    only a receipt naming the new target may complete the handoff.  The old
    promise is released in the same transaction that writes the new one, so
    the source is never unprotected and never double-promised.  When a target
    is named explicitly it must still pass the live capacity check, so an
    operator cannot pin an overcommitted node.
    """
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e:return "unknown_eviction"
        if e["status"]=="moved":return "already_moved"
        t=one(db,"SELECT * FROM tickets WHERE id=?",(e["ticket_id"],))
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not t or t["status"]!="approved" or not w or w["node_id"]!=t["node_id"]:
            return "not_eligible_ticket"
        source=_source_still_valid(db,t,w)
        if not source:return "stale_source_node"
        _bind_source(db,t,source)
        # Validate the new landing while the old promise is still visible, then
        # swap the promises in one transaction.  A failed validation changes
        # nothing, so the old reservation is not lost on a bad request.
        if target_id is not None:
            n=one(db,"SELECT * FROM nodes WHERE id=?",(target_id,))
            if not n or not n["schedulable"]:return "target_not_schedulable"
            if n["id"]==t["node_id"]:return "target_is_source"
            if _domain_conflict(db,w["service"],w["id"],n["zone"],"eviction",eviction_id):
                return "target_domain_conflict"
            if not _fits(db,n,w,"eviction",eviction_id):return "target_overcommitted"
        else:
            n=_choose_target(db,t["node_id"],w,"eviction",eviction_id)
            if not n:return "no_safe_target"
        old=_active_promise(db,"eviction",eviction_id)
        if old:
            db.execute("UPDATE target_promise SET status='released',ended_at=?,reason='retarget' WHERE id=?",(at,old["id"]))
        if w["volume_id"]:
            db.execute("UPDATE volume SET target_node=? WHERE id=?",(n["id"],w["volume_id"]))
        db.execute("UPDATE eviction SET status='planned',target_id=? WHERE id=?",(n["id"],eviction_id))
        revision=one(db,"SELECT COUNT(*) n FROM target_promise WHERE owner_kind='eviction' AND owner_id=?",
                     (eviction_id,))["n"]
        db.execute("""INSERT INTO target_promise(id,owner_kind,owner_id,workload_id,target_id,target_uid,status,created_at)
                      VALUES(?, 'eviction',?,?,?,?,'active',?)""",
                   ("promise:"+eviction_id+":"+str(revision),eviction_id,w["id"],n["id"],n["uid"],at))
        return "planned:"+n["id"]


def abandon_relocation(db,relocation_id,reason="owner_withdrew",at=0):
    """Withdraw an unfinished business relocation; the workload stays serving."""
    with tx(db):
        r=one(db,"SELECT * FROM relocation WHERE id=?",(relocation_id,))
        if not r:return "unknown_relocation"
        if r["status"]=="moved":return "already_moved"
        p=_active_promise(db,"relocation",relocation_id)
        if p:
            db.execute("UPDATE target_promise SET status='released',ended_at=?,reason=? WHERE id=?",(at,reason,p["id"]))
        db.execute("UPDATE relocation SET status='abandoned' WHERE id=?",(relocation_id,))
        return "abandoned"


def relocate(db,relocation_id,workload_id,target_id,owner_generation=None,at=0,
             tenant=None,owner_valid_until=None):
    """Move a workload that is already serving somewhere else.

    This is a business decision, not a maintenance one: it needs a live
    ownership grant and it only creates a promise.  The workload keeps serving
    from its current node until the new target confirms readiness.
    """
    with tx(db):
        old=one(db,"SELECT * FROM relocation WHERE id=?",(relocation_id,))
        if old:
            return "already_relocating" if old["workload_id"]==workload_id else "relocation_identity_conflict"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(workload_id,))
        if not w:return "unknown_workload"
        own=one(db,"SELECT * FROM ownership WHERE workload_id=?",(workload_id,))
        if not own:return "ownership_not_granted"
        if owner_generation is not None and own["generation"]!=owner_generation:
            return "ownership_mismatch"
        if tenant is not None and own["tenant"]!=tenant:return "ownership_mismatch"
        if owner_valid_until is not None and own["valid_until"]<owner_valid_until:
            return "ownership_expired"
        if at and own["valid_until"]<at:return "ownership_expired"
        source=one(db,"SELECT * FROM nodes WHERE id=?",(w["node_id"],))
        n=one(db,"SELECT * FROM nodes WHERE id=?",(target_id,))
        if not n:return "unknown_target"
        if n["id"]==w["node_id"]:return "target_is_source"
        if not n["schedulable"]:return "target_not_schedulable"
        if _promise_for_workload(db,workload_id):return "workload_already_promised"
        if _domain_conflict(db,w["service"],w["id"],n["zone"]):return "target_domain_conflict"
        if not _fits(db,n,w):return "target_overcommitted"
        db.execute("INSERT INTO relocation VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (relocation_id,workload_id,w["node_id"],source["uid"],n["id"],n["uid"],
                    own["generation"] if owner_generation is None else owner_generation,
                    own["valid_until"],"relocating",at))
        db.execute("""INSERT INTO target_promise(id,owner_kind,owner_id,workload_id,target_id,target_uid,status,created_at)
                      VALUES(?, 'relocation',?,?,?,?,'active',?)""",
                   ("promise:"+relocation_id+":"+str(at),relocation_id,workload_id,n["id"],n["uid"],at))
        db.execute("INSERT INTO relocation_attempt(id,relocation_id,workload_id,target_id,result,at) VALUES(?,?,?,?,?,?)",
                   ("relocation:"+relocation_id+":relocating:"+str(at),relocation_id,workload_id,n["id"],"relocating",at))
        return "relocating:"+n["id"]


def relocation_receipt(db,receipt_id,relocation_id,target_uid,workload_generation,version,at=0):
    with tx(db):
        replay=_replay_receipt(db,receipt_id,"relocation",relocation_id,target_uid)
        if replay:return replay
        r=one(db,"SELECT * FROM relocation WHERE id=?",(relocation_id,))
        if not r:return "no_active_relocation"
        if r["status"]=="moved":return "already_moved"
        if r["status"]!="relocating":return "no_active_relocation"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(r["workload_id"],))
        source=one(db,"SELECT * FROM nodes WHERE id=?",(r["source_id"],))
        n=one(db,"SELECT * FROM nodes WHERE id=?",(r["target_id"],))
        if not n or n["uid"]!=target_uid or r["target_uid"]!=target_uid:
            _record_attempt(db,receipt_id,"relocation",relocation_id,r["target_id"],target_uid,
                            workload_generation,version,at,"rejected","stale_target_receipt")
            return "stale_target_receipt"
        if not w or w["generation"]!=workload_generation or w["version"]!=version:
            _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","stale_target_receipt")
            return "stale_target_receipt"
        if not source or source["uid"]!=r["source_uid"] or w["node_id"]!=r["source_id"]:
            _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","stale_source_identity")
            return "stale_source_identity"
        own=one(db,"SELECT * FROM ownership WHERE workload_id=?",(w["id"],))
        if not own or own["generation"]!=r["owner_generation"] or own["valid_until"]<at:
            _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","ownership_not_granted")
            return "ownership_not_granted"
        if not n["schedulable"]:
            _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","target_not_schedulable")
            return "target_not_schedulable"
        if _domain_conflict(db,w["service"],w["id"],n["zone"],"relocation",relocation_id):
            _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","target_domain_conflict")
            return "target_domain_conflict"
        if not _fits(db,n,w,"relocation",relocation_id):
            _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                            workload_generation,version,at,"rejected","target_overcommitted")
            return "target_overcommitted"
        if one(db,"SELECT id FROM relocation_receipt WHERE id=?",(receipt_id,)):return "duplicate"
        db.execute("INSERT INTO relocation_receipt VALUES(?,?,?,?,?,?)",
                   (receipt_id,relocation_id,target_uid,workload_generation,version,at))
        db.execute("UPDATE workloads SET node_id=?,zone=? WHERE id=?",(n["id"],n["zone"],w["id"]))
        db.execute("UPDATE relocation SET status='moved' WHERE id=?",(relocation_id,))
        promise=_active_promise(db,"relocation",relocation_id)
        if promise:
            db.execute("UPDATE target_promise SET status='committed',ended_at=? WHERE id=?",(at,promise["id"]))
        db.execute("INSERT INTO relocation_attempt(id,relocation_id,workload_id,target_id,result,at) VALUES(?,?,?,?,?,?)",
                   ("relocation:"+relocation_id+":moved:"+str(at),relocation_id,w["id"],n["id"],"moved",at))
        _record_attempt(db,receipt_id,"relocation",relocation_id,n["id"],target_uid,
                        workload_generation,version,at,"accepted","moved")
        return "moved"


def volume_receipt(db,receipt_id,eviction_id,kind,node_uid,volume_generation,workload_generation,at):
    if kind not in ("source_detached","target_barrier"):return "unknown_receipt_kind"
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e or e["status"] not in ("planned","ready"):return "target_not_ready"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not w or not w["volume_id"]:return "no_volume"
        p=_active_promise(db,"eviction",eviction_id)
        target_id=e["target_id"] if not p else p["target_id"]
        expected_node=w["node_id"] if kind=="source_detached" else target_id
        v=one(db,"SELECT * FROM volume WHERE id=?",(w["volume_id"],))
        if not v:return "no_volume"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(expected_node,))
        if not n or node_uid!=n["uid"] or volume_generation!=v["generation"] or workload_generation!=w["generation"]:
            return "stale_volume_receipt"
        old=one(db,"SELECT * FROM volume_receipt WHERE id=?",(receipt_id,))
        if old:
            return "duplicate" if (old["volume_id"],old["kind"],old["node_uid"],old["at"])==(v["id"],kind,node_uid,at) else "receipt_identity_conflict"
        db.execute("INSERT INTO volume_receipt VALUES(?,?,?,?,?,?,?)",(receipt_id,v["id"],kind,node_uid,volume_generation,workload_generation,at))
        return "recorded"


def volume_handoff(db,eviction_id,at=0):
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e:return "unknown_eviction"
        if e["status"]=="moved":return "already_moved"
        if e["status"]!="ready":return "target_not_ready"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not w or not w["volume_id"]:return "no_volume"
        p=_active_promise(db,"eviction",eviction_id)
        if not p:return "no_active_eviction"
        t=one(db,"SELECT * FROM tickets WHERE id=?",(e["ticket_id"],))
        if not t or t["status"]!="approved":return "ticket_not_active"
        if not _source_still_valid(db,t,w):return "stale_source_identity"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(p["target_id"],))
        source=one(db,"SELECT * FROM nodes WHERE id=?",(t["node_id"],))
        if not n or not source:return "stale_source_identity"
        v=one(db,"SELECT * FROM volume WHERE id=?",(w["volume_id"],))
        source_receipt=one(db,"""SELECT * FROM volume_receipt WHERE volume_id=? AND kind='source_detached'
            AND node_uid=? AND volume_generation=? AND workload_generation=? ORDER BY at DESC LIMIT 1""",
            (v["id"],source["uid"],v["generation"],w["generation"]))
        barrier=one(db,"""SELECT * FROM volume_receipt WHERE volume_id=? AND kind='target_barrier'
            AND node_uid=? AND volume_generation=? AND workload_generation=? ORDER BY at DESC LIMIT 1""",
            (v["id"],n["uid"],v["generation"],w["generation"]))
        if not source_receipt or not barrier:return "waiting_write_barrier"
        if barrier["at"]<source_receipt["at"]:return "barrier_before_detach"
        if not n["schedulable"]:return "target_not_schedulable"
        if _domain_conflict(db,w["service"],w["id"],n["zone"],"eviction",eviction_id):return "target_domain_conflict"
        if not _fits(db,n,w,"eviction",eviction_id):
            _record_attempt(db,None,"eviction",eviction_id,n["id"],n["uid"],
                            w["generation"],w["version"],at,"rejected","target_overcommitted")
            return "target_overcommitted"
        db.execute("UPDATE volume SET owner_node=?,generation=generation+1 WHERE id=?",(n["id"],w["volume_id"]))
        db.execute("UPDATE workloads SET node_id=?,zone=? WHERE id=?",(n["id"],n["zone"],w["id"]))
        db.execute("UPDATE eviction SET status='moved' WHERE id=?",(eviction_id,))
        db.execute("UPDATE target_promise SET status='committed',ended_at=? WHERE id=?",(at,p["id"]))
        return "write_owner_moved"
