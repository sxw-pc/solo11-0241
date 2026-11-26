"""Capture, preserve and verify offline audit checkpoints from witnesses.

The module keeps three kinds of evidence immutable:

* ``witness_pages`` / ``witness_rows`` - exactly the claims a witness signed.
  A row is never overwritten or deleted.  Re-sending the same page is a
  replay; sending different content under the same page id is rejected.
* ``witness_evidence`` - the event history and business table state a root
  commits to, copied from a console.  Evidence is content addressed and
  append-only, so two consoles that used the same sequence number for
  different histories leave two originals behind.
* ``witness_revocations`` - a later withdrawal of a signature.  Revocation
  adds a fact; it never removes a witness row or an already released report.

Root verification states
------------------------
``evidence``          the root recomputes exactly from the retained per-event
                      content and business table snapshot.
``local_match``       no evidence packet was supplied, but the root equals the
                      root recomputed from this console's own event prefix.
``genesis_anchor``    a seq-0 root: there is no preceding event content to
                      recompute, so it is recorded as a trust anchor only.
``local_mismatch``    the root contradicts the root recomputed from this
                      console's event prefix at the same sequence number.  Both
                      roots are preserved and the contradiction is reported.
``unverifiable``      the root cannot be recomputed from any retained content.

A locally consistent replay on its own is not a root and is never treated as
one.
"""
import hashlib
import hmac
import json

from . import store

WITNESS_SCHEMA = """
CREATE TABLE IF NOT EXISTS witness_pages(id TEXT PRIMARY KEY,digest TEXT NOT NULL,received_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS witness_rows(page_id TEXT,line_no INTEGER,branch TEXT,seq INTEGER,root TEXT,witness TEXT,signed_at INTEGER,signature TEXT,evidence_digest TEXT,node_id TEXT,ticket_id TEXT,generation INTEGER,raw TEXT,PRIMARY KEY(page_id,line_no));
CREATE TABLE IF NOT EXISTS witness_evidence(digest TEXT PRIMARY KEY,branch TEXT NOT NULL,seq INTEGER NOT NULL,root TEXT NOT NULL,events TEXT NOT NULL,tables_json TEXT NOT NULL,received_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS witness_keys(witness TEXT PRIMARY KEY,secret TEXT NOT NULL,registered_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS witness_revocations(id INTEGER PRIMARY KEY AUTOINCREMENT,witness TEXT NOT NULL,page_id TEXT,line_no INTEGER,at INTEGER NOT NULL,reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS report_notes(id INTEGER PRIMARY KEY AUTOINCREMENT,export_id INTEGER,at INTEGER NOT NULL,note TEXT NOT NULL);
"""

REQUIRED_ROW_KEYS = {'branch', 'seq', 'root', 'witness', 'signed_at'}
OPTIONAL_ROW_KEYS = {'signature', 'evidence', 'node_id', 'ticket_id', 'generation'}
APPEND_ONLY = ["witness_pages", "witness_rows", "witness_evidence",
               "witness_revocations", "report_notes"]


def _append_only(db, tables):
    if hasattr(store, "_append_only"):
        store._append_only(db, tables)
        return
    for table in tables:
        for op in ("UPDATE", "DELETE"):
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS {table}_no_{op.lower()} BEFORE {op} ON {table}
              BEGIN SELECT RAISE(ABORT,'{table}_append_only'); END""")


def _ensure(db):
    db.executescript(WITNESS_SCHEMA)
    cols = {r["name"] for r in db.execute("PRAGMA table_info(witness_rows)")}
    for name, decl in (("signature", "TEXT"), ("evidence_digest", "TEXT"),
                       ("node_id", "TEXT"), ("ticket_id", "TEXT"),
                       ("generation", "INTEGER"), ("raw", "TEXT")):
        if name not in cols:
            db.execute(f"ALTER TABLE witness_rows ADD COLUMN {name} {decl}")
    _append_only(db, APPEND_ONLY)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ signing

def signing_payload(row):
    body = {k: v for k, v in row.items() if k != "signature"}
    return _canonical(body)


def sign_row(row, secret):
    """Return a copy of ``row`` carrying a real HMAC-SHA256 signature."""
    body = signing_payload(row)
    return {**row, "signature": hmac.new(secret.encode("utf-8"), body.encode("utf-8"),
                                         hashlib.sha256).hexdigest()}


def make_row(branch, seq, root, witness, signed_at, evidence=None, secret=None):
    """Build a witness row, signing it when a witness secret is supplied."""
    row = {"branch": branch, "seq": seq, "root": root, "witness": witness,
           "signed_at": signed_at}
    if evidence is not None:
        row["evidence"] = evidence
    return sign_row(row, secret) if secret is not None else row


def register_witness(db, witness, secret, at=0):
    _ensure(db)
    with db:
        db.execute("INSERT OR IGNORE INTO witness_keys VALUES (?,?,?)", (witness, secret, at))
    return "registered"


def _signature_status(db, row):
    revocations = db.execute(
        "SELECT id FROM witness_revocations WHERE (page_id=? AND line_no=?) OR "
        "(witness=? AND page_id IS NULL)", (row["page_id"], row["line_no"], row["witness"])).fetchone()
    if revocations:
        return "revoked"
    signature = row.get("signature")
    if not signature:
        return "unsigned"
    key = db.execute("SELECT secret FROM witness_keys WHERE witness=?", (row["witness"],)).fetchone()
    if not key:
        return "unknown_key"
    raw = row.get("raw")
    if raw:
        try:
            signed = json.loads(raw)
        except ValueError:
            return "invalid"
        signed.pop("signature", None)
        body = _canonical(signed)
    else:
        body = signing_payload({k: row[k] for k in
                                ("branch", "seq", "root", "witness", "signed_at")
                                if k in row})
    expected = hmac.new(key[0].encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return "valid" if hmac.compare_digest(expected, signature) else "invalid"


# ----------------------------------------------------------------- evidence

def _normalize_events(events):
    out = []
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("invalid_evidence_events")
        if set(event) != {"seq", "table_name", "row_id", "action", "payload"}:
            raise ValueError("invalid_evidence_events")
        if not isinstance(event["seq"], int) or not isinstance(event["row_id"], int):
            raise ValueError("invalid_evidence_events")
        if event["table_name"] not in store.BUSINESS_TABLES:
            raise ValueError("invalid_evidence_events")
        if event["action"] not in ("insert", "update", "delete"):
            raise ValueError("invalid_evidence_events")
        payload = event["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload) if payload else {}
        if not isinstance(payload, dict):
            raise ValueError("invalid_evidence_events")
        out.append({**event, "payload": payload})
    return sorted(out, key=lambda e: e["seq"])


def _normalize_tables(tables):
    if not isinstance(tables, dict):
        raise ValueError("invalid_evidence_tables")
    out = {}
    for name in store.BUSINESS_TABLES:
        claimed = tables.get(name) or {}
        if not isinstance(claimed, dict):
            raise ValueError("invalid_evidence_tables")
        out[name] = {}
        for row_id, row in claimed.items():
            if not isinstance(row, dict):
                raise ValueError("invalid_evidence_tables")
            out[name][str(row_id)] = row
    return out


def evidence_verification(packet):
    """Recompute a declared root from an evidence packet.

    The packet must carry per-event content, the business table snapshot the
    console claims, the sequence number and the root.  The root is accepted
    only when the events are contiguous, link back to the same node and
    ticket, replay to exactly the claimed tables, and hash to the given root.
    """
    try:
        events = _normalize_events(packet.get("events") or [])
        tables = _normalize_tables(packet.get("tables") or {})
        seq = int(packet.get("seq"))
        root = packet.get("root")
        branch = packet.get("branch")
    except (ValueError, TypeError, AttributeError):
        return {"consistent": False, "reason": "invalid_evidence", "recomputed_root": None,
                "digest": None}
    history = store.verify_event_history(events, tables)
    recomputed = store.root_from_events(events, seq) if history["consistent"] else None
    ok = bool(history["consistent"] and recomputed is not None and root is not None
              and recomputed == root)
    return {"consistent": ok,
            "reason": None if ok else ("root_mismatch" if history["consistent"] else "history_invalid"),
            "recomputed_root": recomputed, "history": history,
            "digest": _sha(_canonical({"branch": branch, "seq": seq, "root": root,
                                       "events": events, "tables": tables}))}


def export_evidence(db, branch):
    """Snapshot a console's complete event history and table state."""
    events = store.audit_events(db)
    seq = store.current_seq(db)
    return {"branch": branch, "seq": seq, "root": store.root_from_events(events, seq),
            "events": events, "tables": store.snapshot_tables(db)}


def import_evidence(db, packet, received_at):
    """Preserve an evidence packet from another console without merging it."""
    _ensure(db)
    verification = evidence_verification(packet)
    branch = packet.get("branch")
    seq = packet.get("seq")
    root = packet.get("root")
    digest = verification["digest"]
    if (not isinstance(branch, str) or not branch or not isinstance(seq, int)
            or not isinstance(root, str) or not root or digest is None):
        raise ValueError("invalid_evidence")
    events = _normalize_events(packet.get("events") or [])
    tables = _normalize_tables(packet.get("tables") or {})
    with db:
        db.execute("INSERT OR IGNORE INTO witness_evidence VALUES (?,?,?,?,?,?,?)",
                   (digest, branch, seq, root, _canonical(events), _canonical(tables), received_at))
    return "captured" if verification["consistent"] else "captured_unverified"


# -------------------------------------------------------------- witness pages

def _validate_rows(rows):
    if not isinstance(rows, list):
        raise ValueError("invalid_witness_page")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid_witness_page")
        if not REQUIRED_ROW_KEYS.issubset(row) or not set(row).issubset(REQUIRED_ROW_KEYS | OPTIONAL_ROW_KEYS):
            raise ValueError("invalid_witness_page")
        if not isinstance(row["branch"], str) or not row["branch"]:
            raise ValueError("invalid_witness_page")
        if not isinstance(row["root"], str) or not row["root"]:
            raise ValueError("invalid_witness_page")
        if not isinstance(row["witness"], str) or not row["witness"]:
            raise ValueError("invalid_witness_page")
        if not isinstance(row["seq"], int) or row["seq"] < 0:
            raise ValueError("invalid_witness_page")
        if not isinstance(row["signed_at"], int):
            raise ValueError("invalid_witness_page")
        if "signature" in row and not isinstance(row["signature"], str):
            raise ValueError("invalid_witness_page")
        if "evidence" in row and not isinstance(row["evidence"], dict):
            raise ValueError("invalid_witness_page")
        if "generation" in row and not isinstance(row["generation"], int):
            raise ValueError("invalid_witness_page")


def import_page(db, page_id, rows, received_at):
    _ensure(db)
    _validate_rows(rows)
    digest = _sha(json.dumps(rows, sort_keys=True, separators=(",", ":")))
    old = db.execute("SELECT digest FROM witness_pages WHERE id=?", (page_id,)).fetchone()
    if old:
        if old[0] != digest:
            raise ValueError("witness_page_conflict")
        return "replayed"
    prepared = []
    for i, row in enumerate(rows, 1):
        evidence_digest = None
        if "evidence" in row:
            evidence_digest = evidence_verification(row["evidence"])["digest"]
            import_evidence(db, row["evidence"], received_at)
        prepared.append((page_id, i, row["branch"], row["seq"], row["root"], row["witness"],
                         row["signed_at"], row.get("signature"), evidence_digest,
                         row.get("node_id"), row.get("ticket_id"), row.get("generation"),
                         _canonical(row)))
    with db:
        db.execute("INSERT INTO witness_pages VALUES (?,?,?)", (page_id, digest, received_at))
        for item in prepared:
            db.execute("INSERT INTO witness_rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", item)
    return "captured"


def _witness_row(db, row):
    data = dict(row)
    if data.get("raw"):
        try:
            raw = json.loads(data["raw"])
            data.setdefault("evidence", raw.get("evidence"))
        except ValueError:
            pass
    data["signature_status"] = _signature_status(db, data)
    return data


def witness_rows(db):
    _ensure(db)
    return [_witness_row(db, r) for r in
            db.execute("SELECT * FROM witness_rows ORDER BY page_id,line_no").fetchall()]


def evidence_bundles(db):
    _ensure(db)
    return [{"digest": r["digest"], "branch": r["branch"], "seq": r["seq"], "root": r["root"],
             "received_at": r["received_at"], "events": json.loads(r["events"]),
             "tables": json.loads(r["tables_json"])}
            for r in db.execute("SELECT * FROM witness_evidence ORDER BY digest").fetchall()]


def _bundle(db, digest):
    if not digest:
        return None
    return db.execute("SELECT * FROM witness_evidence WHERE digest=?", (digest,)).fetchone()


# --------------------------------------------------------------- revocation

def revoke_signature(db, page_id, line_no, at, reason):
    _ensure(db)
    row = db.execute("SELECT * FROM witness_rows WHERE page_id=? AND line_no=?",
                     (page_id, line_no)).fetchone()
    if not row:
        return "unknown_witness_row"
    with db:
        db.execute("INSERT INTO witness_revocations(witness,page_id,line_no,at,reason) VALUES (?,?,?,?,?)",
                   (row["witness"], page_id, line_no, at, reason))
    _attach_revocation_notes(db, row["witness"], page_id, line_no, at, reason)
    return "revoked"


def revoke_witness(db, witness, at, reason):
    _ensure(db)
    rows = db.execute("SELECT page_id,line_no FROM witness_rows WHERE witness=?", (witness,)).fetchall()
    if not rows:
        return "unknown_witness"
    with db:
        for row in rows:
            db.execute("INSERT INTO witness_revocations(witness,page_id,line_no,at,reason) VALUES (?,?,?,?,?)",
                       (witness, row["page_id"], row["line_no"], at, reason))
    _attach_revocation_notes(db, witness, None, None, at, reason)
    return "revoked"


def revocations(db):
    _ensure(db)
    return [dict(r) for r in db.execute("SELECT * FROM witness_revocations ORDER BY id").fetchall()]


def _attach_revocation_notes(db, witness, page_id, line_no, at, reason):
    has_exports = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_exports'").fetchone()
    if not has_exports:
        return
    note = ("witness_signature_revoked: witness=%s page_id=%s line_no=%s at=%s reason=%s"
            % (witness, page_id, line_no, at, reason))
    ids = [r["id"] for r in db.execute("SELECT rowid id FROM audit_exports").fetchall()]
    if not ids:
        return
    with db:
        for export_id in ids:
            db.execute("INSERT INTO report_notes(export_id,at,note) VALUES (?,?,?)",
                       (export_id, at, note))


# ------------------------------------------------------------------ analysis

def _node_entries(tables):
    if not tables:
        return []
    nodes = (tables.get("nodes") or {}).values()
    tickets = list((tables.get("tickets") or {}).values())
    entries = []
    for node in nodes:
        node_id = node.get("id")
        if node_id is None:
            continue
        owned = [{"ticket_id": t.get("id"), "node_uid": t.get("node_uid")}
                 for t in tickets if t.get("node_id") == node_id]
        entries.append({"node_id": node_id, "generation": node.get("generation"),
                        "node_uid": node.get("uid"), "tickets": owned})
    return entries


def _prefix_tables(db, seq):
    events = [e for e in store.audit_events(db) if e["seq"] <= seq]
    return store.replay_events(events)


def _claim(row, branch, seq, root, tables, integrity, source, evidence_digest=None):
    claims = []
    entries = _node_entries(tables)
    if not entries and row.get("node_id"):
        entries = [{"node_id": row["node_id"], "generation": row.get("generation"),
                    "node_uid": row.get("node_uid"),
                    "tickets": [{"ticket_id": row.get("ticket_id"), "node_uid": row.get("node_uid")}]
                    if row.get("ticket_id") else []}]
    if not entries:
        entries = [{"node_id": None, "generation": None, "node_uid": None, "tickets": []}]
    for entry in entries:
        claims.append({
            "branch": branch, "seq": seq, "root": root,
            "node_id": entry["node_id"], "generation": entry["generation"],
            "node_uid": entry["node_uid"], "tickets": entry["tickets"],
            "content_verified": integrity in ("evidence", "local_match"),
            "integrity": integrity, "source": source,
            "evidence_digest": evidence_digest,
            "witness": row.get("witness"), "signature_status": row.get("signature_status"),
            "page_id": row.get("page_id"), "line_no": row.get("line_no"),
        })
    return claims


def _root_conflicts_with_db(root_conflicts, db, root, branch, witness, seq, page_id, line_no, signature_status):
    """Record a witness root that contradicts this console at the same seq."""
    seq = int(seq)
    local_seq = store.current_seq(db)
    if seq <= 0 or seq > local_seq:
        return None
    local_root = store.root_at(db, seq)
    if local_root is None or local_root == root:
        return None
    conflict = {
        "kind": "root_conflict", "seq": seq,
        "local_root": local_root, "witness_root": root,
        "branch": branch, "witness": witness,
        "page_id": page_id, "line_no": line_no,
        "signature_status": signature_status,
    }
    if conflict not in root_conflicts:
        root_conflicts.append(conflict)
    return conflict


def _claims_and_conflicts(db, branch=None):
    _ensure(db)
    events = store.audit_events(db)
    local_seq = store.current_seq(db)
    claims = []
    root_conflicts = []
    if events:
        local_root = store.root_from_events(events, local_seq)
        claims.extend(_claim({}, branch or "__local__", local_seq, local_root,
                             store.replay_events(events), "local_match", "local_history"))
    for raw in witness_rows(db):
        row = dict(raw)
        seq = row["seq"]
        if row.get("evidence_digest"):
            bundle = _bundle(db, row["evidence_digest"])
            if bundle:
                tables = json.loads(bundle["tables_json"])
                packet = {"branch": bundle["branch"], "seq": bundle["seq"],
                          "root": bundle["root"], "events": json.loads(bundle["events"]),
                          "tables": tables}
                verification = evidence_verification(packet)
                verified = (verification["consistent"] and bundle["branch"] == row["branch"]
                            and int(bundle["seq"]) == int(seq) and bundle["root"] == row["root"])
                claims.extend(_claim(row, row["branch"], seq, row["root"], tables,
                                     "evidence" if verified else "unverifiable",
                                     "evidence", row["evidence_digest"]))
                _root_conflicts_with_db(root_conflicts, db, row["root"], row["branch"],
                                        row["witness"], seq, row["page_id"], row["line_no"],
                                        row["signature_status"])
                continue
        if seq == 0:
            claims.extend(_claim(row, row["branch"], seq, row["root"], None,
                                 "genesis_anchor", "witness"))
        elif seq <= local_seq and store.root_at(db, seq) == row["root"]:
            claims.extend(_claim(row, row["branch"], seq, row["root"],
                                 _prefix_tables(db, seq), "local_match", "local"))
        elif seq <= local_seq:
            claims.extend(_claim(row, row["branch"], seq, row["root"], None,
                                 "local_mismatch", "witness"))
            _root_conflicts_with_db(root_conflicts, db, row["root"], row["branch"],
                                    row["witness"], seq, row["page_id"], row["line_no"],
                                    row["signature_status"])
        else:
            claims.extend(_claim(row, row["branch"], seq, row["root"], None,
                                 "unverifiable", "witness"))
    deduped = {}
    for claim in claims:
        key = (claim["branch"], claim["seq"], claim["root"], claim["node_id"],
               claim["generation"], claim["node_uid"])
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = claim
            continue
        existing["content_verified"] = existing["content_verified"] or claim["content_verified"]
        # Prefer an external witness/evidence origin over the console's own
        # unreported local history when they are the same root and identity.
        if existing["source"] == "local_history" and claim["source"] != "local_history":
            existing["source"] = claim["source"]
            existing["integrity"] = claim["integrity"]
            existing["witness"] = claim["witness"]
            existing["signature_status"] = claim["signature_status"]
            existing["page_id"] = claim["page_id"]
            existing["line_no"] = claim["line_no"]
            existing["evidence_digest"] = claim["evidence_digest"]
    return sorted(deduped.values(), key=lambda c: (str(c["branch"]), c["seq"], str(c["node_id"]),
                                                   str(c["node_uid"]), c["root"])), root_conflicts


def claims(db, branch=None):
    """All preserved claims, each traced to a console branch and node identity."""
    return _claims_and_conflicts(db, branch)[0]


def _independent_witnesses(items):
    """Distinct witness opinions; repeated sends by one witness count once."""
    opinions = {}
    for claim in items:
        witness = claim.get("witness")
        if not witness:
            continue
        entry = opinions.setdefault(witness, {"witness": witness, "roots": set(), "statuses": set()})
        entry["roots"].add((claim["branch"], claim["seq"], claim["root"]))
        entry["statuses"].add(claim.get("signature_status"))
    result = []
    for entry in sorted(opinions.values(), key=lambda e: e["witness"]):
        result.append({"witness": entry["witness"],
                       "roots": sorted("%s@%s:%s" % r for r in entry["roots"]),
                       "signature_statuses": sorted(str(s) for s in entry["statuses"]),
                       "equivocating": len(entry["roots"]) > 1})
    return result


def detect_forks(db):
    """Group claims by node identity and report contradictory available history.

    Only roots that can be recomputed (``content_verified``) or that contradict
    this console at the same sequence (``local_mismatch``) take part.  An
    ``unverifiable`` claim - for example tampered evidence, or a sequence this
    console has not reached - is reported as unprovable, never as a competing
    history.
    """
    grouped = {}
    for claim in claims(db):
        if claim["node_id"] is None:
            continue
        if claim["integrity"] not in ("evidence", "local_match", "local_mismatch", "genesis_anchor"):
            continue
        grouped.setdefault((claim["node_id"], claim["generation"]), []).append(claim)
    forks = []
    for (node_id, generation), items in sorted(grouped.items(),
                                               key=lambda kv: (str(kv[0][0]), str(kv[0][1]))):
        verified = [c for c in items if c["content_verified"]]
        records = {}
        for claim in items:
            key = (claim["branch"], claim["seq"], claim["root"], claim["node_uid"])
            record = records.setdefault(key, {
                "branch": claim["branch"], "seq": claim["seq"], "root": claim["root"],
                "node_uid": claim["node_uid"], "content_verified": claim["content_verified"],
                "source": claim["source"], "evidence_digest": claim["evidence_digest"],
                "witness": claim["witness"], "signature_status": claim["signature_status"],
                "page_id": claim["page_id"], "line_no": claim["line_no"]})
            record["content_verified"] = record["content_verified"] or claim["content_verified"]
        root_records = sorted(records.values(), key=lambda r: (str(r["branch"]), r["seq"], r["root"]))
        verified_uids = sorted({c["node_uid"] for c in verified if c["node_uid"]})
        asserted_uids = sorted({c["node_uid"] for c in items if c["node_uid"]})
        verified_roots = {(c["branch"], c["seq"], c["root"]) for c in verified}
        all_roots = {(c["branch"], c["seq"], c["root"]) for c in items}
        ticket_ids = sorted({t["ticket_id"] for c in items for t in c["tickets"] if t.get("ticket_id")})
        ticket_identity = {}
        for ticket_id in ticket_ids:
            ticket_identity[ticket_id] = sorted({
                "%s:%s" % (c["branch"], t["node_uid"])
                for c in items for t in c["tickets"]
                if t.get("ticket_id") == ticket_id and t.get("node_uid")})
        kinds = []
        if len(verified_uids) > 1:
            kinds.append("node_identity_conflict")
        if len({(b, s, r) for b, s, r in verified_roots if b != "__local__"}) > 1:
            kinds.append("root_conflict")
        if len({(b, s, r) for b, s, r in all_roots if b != "__local__"}) > 1:
            if "root_conflict" not in kinds:
                kinds.append("asserted_root_conflict")
        if any(len(v) > 1 for v in ticket_identity.values()) and len(verified_uids) > 1:
            kinds.append("ticket_identity_conflict")
        if len(all_roots) > 1 and len(asserted_uids) > 1 and "node_identity_conflict" not in kinds:
            kinds.append("asserted_conflict")
        if not kinds:
            continue
        proven = all(kind in ("node_identity_conflict", "root_conflict", "ticket_identity_conflict")
                     for kind in kinds)
        forks.append({
            "key": {"node_id": node_id, "generation": generation, "ticket_ids": ticket_ids},
            "kinds": kinds, "proven": proven,
            "branches": sorted({record["branch"] for record in root_records}),
            "roots": root_records,
            "node_uids": {"verified": verified_uids, "asserted": asserted_uids},
            "ticket_identity": ticket_identity,
            "independent_witnesses": _independent_witnesses(items),
        })
    return forks


def detect_root_conflicts(db):
    return _claims_and_conflicts(db)[1]


def analysis(db):
    rows = witness_rows(db)
    statuses = {}
    for row in rows:
        statuses[row["signature_status"]] = statuses.get(row["signature_status"], 0) + 1
    return {"forks": detect_forks(db),
            "root_conflicts": detect_root_conflicts(db),
            "witness_rows": rows,
            "signature_statuses": statuses,
            "revocations": revocations(db),
            "evidence": evidence_bundles(db),
            "claims": claims(db)}
