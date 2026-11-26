import argparse
import json
from pathlib import Path
from . import permit,evacuate,restore,witness_capture,audit_release
from .store import open_db,report

COMMANDS={f"permit.{n}":getattr(permit,n) for n in ["node","guard","approval","ticket","approve","waiver"]}
COMMANDS.update({f"evacuate.{n}":getattr(evacuate,n) for n in ["workload","plan","receipt","volume_receipt","volume_handoff"]})
COMMANDS.update({f"restore.{n}":getattr(restore,n) for n in ["probe","inspect","orphan","certify","ownership"]})
COMMANDS.update({f"witness.{n}":getattr(witness_capture,n) for n in ["import_page","register_witness","revoke_signature"]})
COMMANDS.update({f"audit.{n}":getattr(audit_release,n) for n in ["grant_authority","adjudicate","publish"]})

def run(db_path,fixture,subsystem=None):
    db=open_db(db_path)
    out=[]
    for action in json.loads(Path(fixture).read_text(encoding="utf8"))["actions"]:
        op=action["op"]
        if op not in COMMANDS:raise ValueError("unknown operation "+op)
        out.append({"op":op,"result":COMMANDS[op](db,**action.get("args",{}))})
    if subsystem and not any(x["op"].startswith(subsystem+".") for x in out):
        raise ValueError("fixture has no "+subsystem+" operation")
    result={"results":out,"state":report(db)}
    db.close()
    return result

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("subsystem",choices=["permit","evacuate","restore","witness","audit","all"])
    parser.add_argument("fixture")
    parser.add_argument("--db",default="clouddrain.sqlite")
    a=parser.parse_args()
    print(json.dumps(run(a.db,a.fixture,None if a.subsystem=="all" else a.subsystem),ensure_ascii=False,indent=2))

if __name__=="__main__":main()
