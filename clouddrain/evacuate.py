from .store import one,rows,tx

def workload(db,workload_id,service,node_id,zone,cpu,mem,kind,version,generation,volume_id=None):
    with tx(db):
        db.execute("INSERT INTO workloads VALUES(?,?,?,?,?,?,?,?,?,?)",(workload_id,service,node_id,zone,cpu,mem,kind,volume_id,version,generation))
        if volume_id:
            db.execute("INSERT OR IGNORE INTO volume(id,owner_node) VALUES(?,?)",(volume_id,node_id))
        return "registered"

def plan(db,eviction_id,ticket_id,workload_id,allow_data_loss=False):
    with tx(db):
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

def volume_handoff(db,eviction_id,source_detached=False,target_barrier=False):
    with tx(db):
        e=one(db,"SELECT * FROM eviction WHERE id=?",(eviction_id,))
        if not e:return "unknown_eviction"
        w=one(db,"SELECT * FROM workloads WHERE id=?",(e["workload_id"],))
        if not w["volume_id"]:return "no_volume"
        if e["status"] not in ("ready","moved"):return "target_not_ready"
        v=one(db,"SELECT * FROM volume WHERE id=?",(w["volume_id"],))
        detached=int(v["source_detached"] or source_detached)
        barrier=int(v["target_barrier"] or target_barrier)
        db.execute("UPDATE volume SET source_detached=?,target_barrier=? WHERE id=?",(detached,barrier,w["volume_id"]))
        if not detached or not barrier:return "waiting_write_barrier"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(e["target_id"],))
        db.execute("UPDATE volume SET owner_node=? WHERE id=?",(e["target_id"],w["volume_id"]))
        db.execute("UPDATE workloads SET node_id=?,zone=? WHERE id=?",(n["id"],n["zone"],w["id"]))
        db.execute("UPDATE eviction SET status='moved' WHERE id=?",(eviction_id,))
        return "write_owner_moved"
