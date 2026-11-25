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

def plan(db,eviction_id,ticket_id,workload_id,allow_data_loss=False):
    with tx(db):
        old=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if old:
            return "already_planned" if old["ticket_id"]==ticket_id and old["workload_id"]==workload_id else "eviction_identity_conflict"
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        w=one(db,"SELECT * FROM workloads WHERE id=?",(workload_id,))
        if not t or t["status"]!="approved" or not w or w["node_id"]!=t["node_id"]:
            return "not_eligible_ticket"
        if w["kind"]=="local":
            waiver=one(db,"SELECT * FROM data_loss_waiver WHERE ticket_id=? AND workload_id=?",(ticket_id,workload_id))
            if not allow_data_loss or not t["urgent"] or not waiver:
                return "local_volume_block"
        occupied={x["zone"] for x in rows(db,"SELECT n.zone FROM workloads w JOIN nodes n ON n.id=w.node_id WHERE w.service=? AND w.id<>?",(w["service"],workload_id))}
        for n in rows(db,"SELECT * FROM nodes WHERE id<>? AND schedulable=1 ORDER BY id",(t["node_id"],)):
            if n["zone"] in occupied:continue
            used=one(db,"SELECT COALESCE(SUM(cpu),0) cpu,COALESCE(SUM(mem),0) mem FROM workloads WHERE node_id=?",(n["id"],))
            reserved=one(db,"""SELECT COALESCE(SUM(w.cpu),0) cpu,COALESCE(SUM(w.mem),0) mem FROM eviction e
                JOIN workloads w ON w.id=e.workload_id WHERE e.target_id=? AND e.status IN ('planned','ready')""",(n["id"],))
            if n["cpu"]-used["cpu"]-reserved["cpu"]>=w["cpu"] and n["mem"]-used["mem"]-reserved["mem"]>=w["mem"]:
                db.execute("INSERT INTO eviction VALUES(?,?,?,?,?)",(eviction_id,ticket_id,workload_id,n["id"],"planned"))
                if w["volume_id"]:
                    db.execute("UPDATE volume SET target_node=? WHERE id=?",(n["id"],w["volume_id"]))
                return "planned:"+n["id"]
        return "no_safe_target"

def receipt(db,receipt_id,eviction_id,target_uid,workload_generation,version):
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e or e["status"] not in ("planned","ready"):return "no_active_eviction"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(e["target_id"],))
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not n or n["uid"]!=target_uid or w["generation"]!=workload_generation or w["version"]!=version:
            return "stale_target_receipt"
        if one(db,"SELECT id FROM target_receipt WHERE id=?",(receipt_id,)):return "duplicate"
        db.execute("INSERT INTO target_receipt VALUES(?,?,?,?,?)",(receipt_id,eviction_id,target_uid,workload_generation,version))
        db.execute("UPDATE eviction SET status='ready' WHERE id=?",(eviction_id,))
        if not w["volume_id"]:
            db.execute("UPDATE workloads SET node_id=?,zone=? WHERE id=?",(n["id"],n["zone"],w["id"]))
            db.execute("UPDATE eviction SET status='moved' WHERE id=?",(eviction_id,))
            return "moved"
        return "waiting_volume_handoff"

def volume_receipt(db,receipt_id,eviction_id,kind,node_uid,volume_generation,workload_generation,at):
    if kind not in ("source_detached","target_barrier"):return "unknown_receipt_kind"
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e or e["status"]!="ready":return "target_not_ready"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not w["volume_id"]:return "no_volume"
        v=one(db,"SELECT * FROM volume WHERE id=?",(w["volume_id"],))
        expected_node=v["owner_node"] if kind=="source_detached" else e["target_id"]
        n=one(db,"SELECT * FROM nodes WHERE id=?",(expected_node,))
        if not n or node_uid!=n["uid"] or volume_generation!=v["generation"] or workload_generation!=w["generation"]:
            return "stale_volume_receipt"
        old=one(db,"SELECT * FROM volume_receipt WHERE id=?",(receipt_id,))
        if old:
            return "duplicate" if (old["volume_id"],old["kind"],old["node_uid"],old["at"])==(v["id"],kind,node_uid,at) else "receipt_identity_conflict"
        db.execute("INSERT INTO volume_receipt VALUES(?,?,?,?,?,?,?)",(receipt_id,v["id"],kind,node_uid,volume_generation,workload_generation,at))
        return "recorded"

def volume_handoff(db,eviction_id):
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e:return "unknown_eviction"
        if e["status"]=="moved":return "already_moved"
        if e["status"]!="ready":return "target_not_ready"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not w["volume_id"]:return "no_volume"
        v=one(db,"SELECT * FROM volume WHERE id=?",(w["volume_id"],))
        source=one(db,"SELECT * FROM volume_receipt WHERE volume_id=? AND kind='source_detached' ORDER BY at DESC LIMIT 1",(v["id"],))
        barrier=one(db,"SELECT * FROM volume_receipt WHERE volume_id=? AND kind='target_barrier' ORDER BY at DESC LIMIT 1",(v["id"],))
        if not source or not barrier:return "waiting_write_barrier"
        if barrier["at"]<source["at"]:return "barrier_before_detach"
        source_node=one(db,"SELECT * FROM nodes WHERE id=?",(v["owner_node"],))
        n=one(db,"SELECT * FROM nodes WHERE id=?",(e["target_id"],))
        if source["node_uid"]!=source_node["uid"] or barrier["node_uid"]!=n["uid"] or source["volume_generation"]!=v["generation"] or barrier["volume_generation"]!=v["generation"]:
            return "stale_volume_receipt"
        db.execute("UPDATE volume SET owner_node=?,generation=generation+1 WHERE id=?",(e["target_id"],w["volume_id"]))
        db.execute("UPDATE workloads SET node_id=?,zone=? WHERE id=?",(n["id"],n["zone"],w["id"]))
        db.execute("UPDATE eviction SET status='moved' WHERE id=?",(eviction_id,))
        return "write_owner_moved"
