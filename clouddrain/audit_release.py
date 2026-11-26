"""Customer audit reports that separate local consistency from shared history.

A local replay proves only that *this* console's tables match *this* console's
event log.  It does not prove that the log is the only history: two consoles
restored from the same backup can each replay consistently while recording a
different physical node for the same ticket.

``publish`` returns ``published`` when a customer deliverable was issued and
``hold`` when no honest deliverable can be made.  The report's ``category``
carries the actual claim:

``published``  no preserved conflict; witnesses in scope verify.  The release
               is still explicitly limited to the cited branch/console and
               states that cross-site uniqueness is unproven.
``disputed``   conflicting roots or physical identities are preserved.  The
               notice lists every root and does *not* claim the ticket took
               effect on any physical node.
``certified``  a customer-authorised adjudicator resolved the conflict on
               physical evidence.  The report includes the rejected branch and
               what remains unprovable.
``hold``       nothing could be honestly released (no evidence, or local
               replay broken).

``publish`` never returns ``published`` with a clean category unless the
witness roots in scope can be traced to event content and table state (or are
explicit seq-0 anchors).  Released reports and witness originals are
append-only; later contradictions, revocations, or reports are added as new
rows/notes, and earlier statements are never rewritten or deleted.
"""
import json

from . import store
from . import witness_capture as wc

PUBLISHED = "published"
DISPUTED = "disputed"
CERTIFIED = "certified"
HOLD = "hold"
CATEGORIES = (PUBLISHED, DISPUTED, CERTIFIED, HOLD)
DELIVERABLE = (PUBLISHED, DISPUTED, CERTIFIED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_exports(branch TEXT NOT NULL,at INTEGER NOT NULL,decision TEXT NOT NULL,category TEXT,report TEXT,supersedes TEXT);
CREATE TABLE IF NOT EXISTS audit_authority(adjudicator TEXT NOT NULL,scope TEXT NOT NULL,granted_at INTEGER NOT NULL,PRIMARY KEY(adjudicator,scope));
CREATE TABLE IF NOT EXISTS audit_adjudications(id INTEGER PRIMARY KEY AUTOINCREMENT,node_id TEXT NOT NULL,generation INTEGER NOT NULL,ticket_id TEXT,node_uid TEXT NOT NULL,adjudicator TEXT NOT NULL,basis TEXT NOT NULL,at INTEGER NOT NULL);
"""
APPEND_ONLY = ["audit_exports", "audit_adjudications"]


def _append_only(db, tables):
    for table in tables:
        for op in ("UPDATE", "DELETE"):
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS {table}_no_{op.lower()} BEFORE {op} ON {table}
              BEGIN SELECT RAISE(ABORT,'{table}_append_only'); END""")


def _ensure(db):
    db.executescript(SCHEMA)
    cols = {r["name"] for r in db.execute("PRAGMA table_info(audit_exports)")}
    for name, decl in (("category", "TEXT"), ("report", "TEXT"), ("supersedes", "TEXT")):
        if name not in cols:
            db.execute(f"ALTER TABLE audit_exports ADD COLUMN {name} {decl}")
    _append_only(db, APPEND_ONLY)


# ------------------------------------------------------------------ authority

def grant_authority(db, adjudicator, scope, granted_at):
    """Record a customer authorisation for an adjudicator and decision scope."""
    _ensure(db)
    if not adjudicator or not scope:
        return "invalid_authority"
    with db:
        db.execute("INSERT OR IGNORE INTO audit_authority VALUES (?,?,?)",
                   (adjudicator, scope, granted_at))
    return "granted"


def authority(db, adjudicator, scope="physical_identity"):
    _ensure(db)
    row = db.execute("SELECT * FROM audit_authority WHERE adjudicator=? AND scope IN (?, '*')",
                     (adjudicator, scope)).fetchone()
    return dict(row) if row else None


def adjudicate(db, node_id, generation, ticket_id, node_uid, adjudicator, basis, at):
    """Record a resolution of a physical-identity fork.

    Requires customer authorisation and physical evidence.  The chosen UID must
    already be supported by at least one content-verified claim for the same
    node/generation/ticket.  This deliberately does not require the disputed
    branch to be known locally: an offline console may hold only its own
    history when the adjudicator rules on the physical device.
    """
    _ensure(db)
    if not authority(db, adjudicator):
        return "unauthorized_adjudicator"
    if not isinstance(basis, dict):
        return "insufficient_basis"
    consoles = basis.get("consoles") or []
    witnesses = basis.get("witnesses") or []
    if not basis.get("physical_inspection") or len(consoles) < 2 or not witnesses:
        return "insufficient_basis"
    supported = [c for c in wc.claims(db)
                 if c["node_id"] == node_id and c["generation"] == generation
                 and c["integrity"] in ("evidence", "local_match")
                 and c["node_uid"] == node_uid
                 and (ticket_id is None or any(t.get("ticket_id") == ticket_id
                                               for t in c["tickets"]))]
    if not supported:
        return "uid_not_supported_by_verified_history"
    with db:
        db.execute("INSERT INTO audit_adjudications(node_id,generation,ticket_id,node_uid,adjudicator,basis,at) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (node_id, generation, ticket_id, node_uid, adjudicator,
                    json.dumps(basis, sort_keys=True), at))
    return "recorded"


def adjudications(db):
    _ensure(db)
    return [{**dict(r), "basis": json.loads(r["basis"])}
            for r in db.execute("SELECT * FROM audit_adjudications ORDER BY id").fetchall()]


# -------------------------------------------------------------------- reports

def _fork_key(fork):
    return fork["key"]["node_id"], fork["key"]["generation"]


def _verified_uids(fork):
    return set(fork["node_uids"]["verified"])


def _matching_resolution(db, fork):
    node_id, generation = _fork_key(fork)
    tickets = set(fork["key"]["ticket_ids"])
    verified = _verified_uids(fork)
    for adj in reversed(adjudications(db)):
        if adj["node_id"] != node_id or adj["generation"] != generation:
            continue
        if adj["node_uid"] not in verified:
            continue
        if adj["ticket_id"] is not None and tickets and adj["ticket_id"] not in tickets:
            continue
        if not authority(db, adj["adjudicator"]):
            continue
        return adj
    return None


def _unprovable(db, forks, root_conflicts, resolution=None, published=False):
    facts = []
    for fork in forks:
        node_id, generation = _fork_key(fork)
        for record in fork["roots"]:
            if not record["content_verified"]:
                facts.append("branch %s root %s (seq %s) cannot be recomputed from event content "
                             "and table state" % (record["branch"], record["root"], record["seq"]))
        if resolution:
            for record in fork["roots"]:
                if record.get("node_uid") and record["node_uid"] != resolution["node_uid"]:
                    facts.append("branch %s records node %s generation %s as uid %s; whether that "
                                 "device state ever existed on the physical node cannot be proven"
                                 % (record["branch"], node_id, generation, record["node_uid"]))
        for record in fork["roots"]:
            if record.get("signature_status") in ("revoked", "invalid", "unknown_key"):
                facts.append("witness signature for branch %s root %s is %s"
                             % (record["branch"], record["root"], record["signature_status"]))
    for conflict in root_conflicts:
        facts.append("witness root %s for seq %s contradicts this console's root %s for the same "
                     "sequence; which event content the witness actually saw cannot be proven from "
                     "the local chain" % (conflict["witness_root"], conflict["seq"], conflict["local_root"]))
    if published:
        facts.append("absence of offline forks outside this console's evidence cannot be proven")
    for revocation in wc.revocations(db):
        facts.append("witness %s revoked signature at %s: %s"
                     % (revocation["witness"], revocation["at"], revocation["reason"]))
    seen = []
    for fact in facts:
        if fact not in seen:
            seen.append(fact)
    return seen


def _roots(db, claims):
    roots = []
    for claim in claims:
        if not _witness_backed(claim) or claim["root"] is None:
            continue
        roots.append({"node_id": claim["node_id"], "generation": claim["generation"],
                      "branch": claim["branch"], "seq": claim["seq"],
                      "root": claim["root"], "node_uid": claim["node_uid"],
                      "content_verified": claim["content_verified"],
                      "witness": claim.get("witness"),
                      "signature_status": claim.get("signature_status")})
    return roots


def _disposition_from_claims(claims, selected_uid, local_branch=None):
    """One row per preserved branch/root, accepted or rejected.

    The console's own history is labelled with ``local_branch`` when supplied,
    so it lines up with the witness row that commits to the same root instead of
    appearing twice.
    """
    seen = {}
    for claim in claims:
        if not _witness_backed(claim):
            continue
        branch = claim["branch"]
        if claim["source"] == "local_history" and local_branch:
            branch = local_branch
        key = (branch, claim["seq"], claim["root"], claim["node_uid"])
        entry = seen.setdefault(key, {
            "branch": branch, "seq": claim["seq"], "root": claim["root"],
            "node_uid": claim["node_uid"], "generation": claim["generation"],
            "content_verified": claim["content_verified"], "source": claim["source"],
            "witness": claim.get("witness"),
            "signature_status": claim.get("signature_status"),
        })
        entry["content_verified"] = entry["content_verified"] or claim["content_verified"]
        if entry["source"] == "local_history" and claim["source"] != "local_history":
            entry["source"] = claim["source"]
            entry["witness"] = claim.get("witness")
            entry["signature_status"] = claim.get("signature_status")
        if selected_uid is None:
            entry["outcome"] = "preserved"
        elif entry["node_uid"] == selected_uid:
            entry["outcome"] = "accepted"
        else:
            entry["outcome"] = "rejected"
            entry["reason"] = ("physical identity rejected by adjudication: branch records %s, "
                               "authorised identity is %s" % (entry["node_uid"], selected_uid))
    return sorted(seen.values(), key=lambda e: (str(e["branch"]), e["seq"], e["root"]))


def _prior_exports(db):
    _ensure(db)
    return [{"export_id": r["id"], "branch": r["branch"], "at": r["at"],
             "category": r["category"] or r["decision"]}
            for r in db.execute("SELECT rowid id,branch,at,decision,category "
                                "FROM audit_exports ORDER BY rowid").fetchall()]


def _report(db, branch, at, category, replay, claims, forks, root_conflicts=None,
            resolution=None, reason=None, adjudication_error=None):
    root_conflicts = root_conflicts or []
    prior = _prior_exports(db)
    roots = _roots(db, claims)
    limitations = []
    if category == PUBLISHED:
        limitations = ["release is limited to the branch/console cited in this report",
                       "cross-site uniqueness of this history is unproven"]
        if all(c["integrity"] == "genesis_anchor" for c in claims if _witness_backed(c)):
            limitations.append("the only witness root is a seq-0 genesis anchor; it commits to no "
                               "preceding event content")
    report = {
        "schema": "clouddrain-audit-report/v1",
        "category": category,
        "delivery": category,
        "branch": branch,
        "at": at,
        "local_replay_consistent": bool(replay.get("consistent")),
        "local_replay": replay,
        "roots": roots,
        "root_conflicts": root_conflicts,
        "forks": forks,
        "claims": claims,
        "prior_exports": prior,
        "limitations": limitations,
        "unprovable": _unprovable(db, forks, root_conflicts, resolution,
                                  published=(category == PUBLISHED)),
        "revocation_notices": wc.revocations(db),
        "attestation": None,
    }
    if reason:
        report["reason"] = reason
    if adjudication_error:
        report["adjudication_error"] = adjudication_error
        report["adjudication_errors"] = [adjudication_error]
    if not replay.get("consistent"):
        report["local_integrity_failures"] = replay.get("problems", [])
        report["statement_integrity"] = ("this console's own audit log no longer replays to its "
                                         "business tables; the failure is preserved and is itself "
                                         "part of the dispute")
    if category == PUBLISHED:
        report["scope"] = {"ticket_effective_identity": "single_branch_only",
                           "cross_site_uniqueness": "unproven"}
        report["conclusion"] = None
        report["disposition"] = _disposition_from_claims(claims, None, branch)
    elif category == DISPUTED:
        report["scope"] = {"ticket_effective_identity": "unresolved"}
        report["conclusion"] = None
        report["claim"] = {"ticket_effective_on_physical_node": False}
        report["disposition"] = _disposition_from_claims(claims, None, branch)
        if not replay.get("consistent"):
            # A broken local log is itself part of the dispute; record it
            # explicitly so the notice never hides it.
            report["disposition"].append({
                "branch": "__local__", "outcome": "local_replay_failed",
                "content_verified": False, "source": "local_history",
                "problems": sorted({p.get("kind") for p in replay.get("problems", [])}),
            })
        if root_conflicts:
            report["statement"] = ("a witness root contradicts this console's event history at the "
                                   "same sequence number. Both the local history and the witness "
                                   "root are preserved here; this notice does NOT assert which "
                                   "history is authoritative or that the ticket took effect on "
                                   "any physical node.")
            report["required_actions"] = ["retain the local history and the witness original",
                                          "obtain independent evidence of the witnessed sequence",
                                          "customer remediation and follow-up investigation"]
        else:
            report["statement"] = ("conflicting roots / physical identities for the same node, "
                                   "generation and ticket are preserved here. This notice does NOT "
                                   "assert that the ticket took effect on any physical node.")
            report["required_actions"] = ["retain both originals",
                                          "obtain both consoles, the physical node and independent witnesses",
                                          "obtain a customer-authorised adjudication before any identity conclusion",
                                          "customer remediation and follow-up investigation"]
    if category == CERTIFIED and resolution:
        selected = resolution["node_uid"]
        node_id, generation = resolution["node_id"], resolution["generation"]
        tickets = sorted({
            t["ticket_id"]
            for claim in claims
            if claim["node_id"] == node_id and claim["generation"] == generation
            and claim["node_uid"] == selected
            for t in claim["tickets"] if t.get("ticket_id")})
        for fork in forks:
            if _fork_key(fork) == (node_id, generation):
                tickets = sorted(set(tickets) | set(fork["key"]["ticket_ids"]))
        report["scope"] = {"ticket_effective_identity": "resolved_by_authorised_adjudicator"}
        report["conclusion"] = {
            "node_id": node_id, "generation": generation,
            "authorized_node_uid": selected, "ticket_ids": tickets,
            "adjudicator": resolution["adjudicator"],
            "adjudicated_at": resolution["at"],
            "basis": resolution["basis"],
            "statement": ("ticket(s) %s authorise maintenance of node %s generation %s, "
                          "physical uid %s" % (tickets, node_id, generation, selected)),
        }
        report["disposition"] = _disposition_from_claims(claims, selected, branch)
        report["attestation"] = {
            "conclusion": "maintenance_authorization_completed_per_policy",
            "dispute_handled": True,
            "handled_by": {"adjudicator": resolution["adjudicator"],
                           "authority_scope": "physical_identity",
                           "at": resolution["at"], "basis": resolution["basis"]},
            "authorized_node_uid": selected,
            "unprovable": report["unprovable"],
        }
    elif category in (HOLD,):
        report["scope"] = {"ticket_effective_identity": "not_determined"}
        report["conclusion"] = None
        report["claim"] = {"ticket_effective_on_physical_node": False}
    return report


def _insert(db, branch, at, category, report):
    prior = _prior_exports(db)
    supersedes = json.dumps([p["export_id"] for p in prior])
    with db:
        db.execute("INSERT INTO audit_exports(branch,at,decision,category,report,supersedes) "
                   "VALUES (?,?,?,?,?,?)",
                   (branch, at, category, category, json.dumps(report, sort_keys=True), supersedes))
    return db.execute("SELECT MAX(rowid) id FROM audit_exports").fetchone()["id"]

def _witness_backed(claim):
    # Display scope: reports list the local root alongside any externally
    # witnessed roots.
    return claim["source"] in ("local", "evidence", "witness", "local_history")


def _is_witness_root(claim):
    # Roots that can support a release must come from a witness or evidence,
    # never from the console's own unreported history.
    return claim["source"] in ("local", "evidence", "witness")


def _in_scope(claim, branch):
    if claim["source"] == "local_history":
        return True
    if claim["branch"] == branch:
        return True
    return branch in (None, "local", "__local__")


def _releasable(claim):
    """A root may support a clean release only when its integrity is proven.

    ``evidence`` and ``local_match`` recompute the root from event content and
    table state.  ``genesis_anchor`` is an explicit seq-0 trust anchor: there is
    no preceding event content to recompute, so it stands only as an anchor and
    cannot carry a cross-site uniqueness claim on its own.
    """
    return claim["integrity"] in ("evidence", "local_match", "genesis_anchor")


def publish(db, branch, at, adjudication=None):
    """Deliver a customer report for ``branch`` at time ``at``.

    Returns ``published`` when a deliverable (``published``, ``disputed`` or
    ``certified`` category) was issued and ``hold`` when it could not be.
    A clean ``published`` category requires witness roots in scope that trace
    to event content and table state; a locally consistent replay alone is
    never enough.  ``adjudication`` is an optional dict with the kwargs of
    :func:`adjudicate`; it is validated and recorded before the release.
    Without a valid resolution a preserved conflict is always delivered as a
    ``disputed`` notice.
    """
    _ensure(db)
    replay = store.verify_replay(db)
    # Label the local history with the release branch so a clean release always
    # has a branch-scoped, recomputable claim to verify.
    claims = wc.claims(db, branch)
    forks = wc.detect_forks(db)
    root_conflicts = wc.detect_root_conflicts(db)

    adjudication_error = None
    if adjudication is not None:
        result = adjudicate(db, **adjudication)
        if result != "recorded":
            adjudication_error = result

    resolved = {}
    for index, fork in enumerate(forks):
        found = _matching_resolution(db, fork)
        if found:
            resolved[index] = found
    resolution = resolved.get(0) if resolved and len(resolved) == len(forks) else None

    scoped = [c for c in claims if _in_scope(c, branch)]
    scoped_witness = [c for c in scoped if _is_witness_root(c)]
    verified_witness = [c for c in scoped_witness if _releasable(c)]

    if forks:
        # A conflict is either certified by an authorised adjudication that
        # covers every fork, or honestly delivered as a disputed notice.
        category = CERTIFIED if resolution else DISPUTED
        if resolution is None and resolved:
            adjudication_error = adjudication_error or "unresolved_forks_remain"
    elif root_conflicts:
        category = DISPUTED
    elif not claims:
        category = HOLD
    elif not replay.get("consistent"):
        # A console whose own sealed history no longer replays must not claim a
        # clean result.  If any preserved witness evidence survives, the failure
        # itself is a dispute and is delivered as such rather than withheld.
        category = DISPUTED if any(_is_witness_root(c) for c in claims) else HOLD
    elif not scoped_witness:
        category = HOLD
    elif not verified_witness:
        category = HOLD
    elif len(verified_witness) != len(scoped_witness):
        category = HOLD
    elif adjudication_error:
        # A claimed resolution that could not be recorded must not quietly
        # become a clean release.
        category = HOLD
    else:
        category = PUBLISHED

    reason = None
    if category == HOLD:
        if adjudication_error:
            reason = "adjudication_" + adjudication_error
        elif not replay.get("consistent"):
            reason = "local_replay_inconsistent"
        elif not claims:
            reason = "no_history_or_evidence"
        elif not scoped_witness:
            reason = "no_witness_for_branch"
        elif not verified_witness:
            reason = "witness_root_not_recomputable_from_content"
        else:
            reason = "unverified_witness_in_scope"
    report = _report(db, branch, at, category, replay, claims, forks,
                     root_conflicts=root_conflicts,
                     resolution=resolution, reason=reason,
                     adjudication_error=adjudication_error)
    _insert(db, branch, at, category, report)
    return PUBLISHED if category in DELIVERABLE else HOLD


# ------------------------------------------------------------------ retrieval

def exports(db, branch=None):
    _ensure(db)
    sql = "SELECT rowid id,branch,at,decision,category FROM audit_exports"
    args = ()
    if branch is not None:
        sql += " WHERE branch=?"
        args = (branch,)
    return [{"export_id": r["id"], "branch": r["branch"], "at": r["at"],
             "category": r["category"] or r["decision"], "decision": r["decision"]}
            for r in db.execute(sql + " ORDER BY rowid", args).fetchall()]


def export_report(db, export_id):
    """Return a released report with later notes attached, never modified."""
    _ensure(db)
    row = db.execute("SELECT rowid id,* FROM audit_exports WHERE rowid=?", (export_id,)).fetchone()
    if not row or not row["report"]:
        return None
    report = json.loads(row["report"])
    report["export_id"] = row["id"]
    report["stored_category"] = report.get("category")
    later = db.execute("SELECT id,at,note FROM report_notes WHERE export_id=? ORDER BY id",
                       (export_id,)).fetchall()
    report["later_notes"] = [dict(n) for n in later]
    superseded = []
    for other in db.execute("SELECT rowid id,supersedes,category FROM audit_exports "
                            "WHERE rowid>? ORDER BY rowid", (export_id,)).fetchall():
        try:
            refs = json.loads(other["supersedes"] or "[]")
        except ValueError:
            refs = []
        if export_id in refs:
            superseded.append({"export_id": other["id"], "category": other["category"]})
    report["superseded_by"] = superseded
    return report


def latest_export(db, branch=None):
    rows = exports(db, branch)
    return rows[-1] if rows else None


def customer_identity_conclusion(db, export_id):
    """What a released report allows the customer to conclude about identity."""
    report = export_report(db, export_id)
    if report is None:
        return None
    if report.get("category") == CERTIFIED and report.get("conclusion"):
        return report["conclusion"].get("authorized_node_uid")
    return None
