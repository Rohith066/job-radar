"""Read-only observation snapshot for the Phase 2 measurement period.

Prints a machine-readable JSON blob plus a short human summary. Opens nothing
for writing: safe to run on a schedule while workflows are committing state.

Reports only what is locally observable. It does not query the GitHub API, and
it never fabricates workflow-run data it cannot see.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from .priority import APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW
from .queue import STATUSES, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED, SHORTLISTED

BANDS = (APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW)


def snapshot(db, *, days: int | None = None, surfaced_limit: int = 4000) -> dict:
    """Collect one observation point. Read-only."""
    c = db._conn
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).isoformat() if days else None

    def one(sql, params=()):
        try:
            return c.execute(sql, params).fetchone()[0]
        except Exception:
            return None

    out: dict = {
        "timestamp": now.isoformat(),
        "window_days": days,
        "discovery_db": db.path,
        "user_state_db": str(db.user_state_path) if db.user_state_path else None,
        "jobs": one("SELECT COUNT(*) FROM main.jobs"),
        "boards": one("SELECT COUNT(*) FROM main.boards"),
        "applications": one("SELECT COUNT(*) FROM userstate.applications") if db.has_user_state else None,
        "user_actions": one("SELECT COUNT(*) FROM userstate.user_feedback") if db.has_user_state else None,
        "legacy_feedback": one("SELECT COUNT(*) FROM main.feedback"),
    }

    # Surfaced-by-band, from the discovery DB's stored Phase 1 priority.
    out["surfaced_screening"] = {
        r[0] or "(unscored)": r[1]
        for r in c.execute("SELECT priority, COUNT(*) FROM main.jobs GROUP BY priority")
    }

    if not db.has_user_state:
        out["note"] = "user-state database unavailable; user funnel not observable"
        return out

    where = "WHERE updated_at >= ?" if since else ""
    params = (since,) if since else ()
    counts = {s: 0 for s in STATUSES}
    for r in c.execute(
        f"SELECT status, COUNT(*) FROM userstate.applications {where} GROUP BY status", params
    ):
        counts[r[0]] = r[1]
    out["user_actions_by_status"] = counts

    applied_like = (APPLIED, INTERVIEW, REJECTED, NO_RESPONSE)
    total_applied = sum(counts[s] for s in applied_like)
    out["funnel"] = {
        "shortlisted": counts[SHORTLISTED],
        "applied": total_applied,
        "skipped": counts[SKIPPED],
        "interview": counts[INTERVIEW],
        "rejected": counts[REJECTED],
        "no_response": counts[NO_RESPONSE],
    }

    # Conversion by the band recorded at decision time.
    by_band: dict = {}
    for r in c.execute(
        """SELECT CASE WHEN priority_at_decision >= 88 THEN 'APPLY_FIRST'
                       WHEN priority_at_decision >= 78 THEN 'HIGH'
                       WHEN priority_at_decision >= 66 THEN 'MEDIUM'
                       WHEN priority_at_decision >= 55 THEN 'REVIEW'
                       ELSE 'LOW' END AS band,
                  COUNT(*),
                  SUM(CASE WHEN status IN ('APPLIED','INTERVIEW','REJECTED','NO_RESPONSE') THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='SKIPPED' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='INTERVIEW' THEN 1 ELSE 0 END)
           FROM userstate.applications WHERE priority_at_decision IS NOT NULL GROUP BY band"""
    ):
        by_band[r[0]] = {"decided": r[1], "applied": r[2] or 0,
                         "skipped": r[3] or 0, "interview": r[4] or 0}
    out["conversion_by_band"] = by_band

    by_fam: dict = {}
    for r in c.execute(
        """SELECT COALESCE(NULLIF(role_family,''),'unknown') fam, COUNT(*),
                  SUM(CASE WHEN status IN ('APPLIED','INTERVIEW','REJECTED','NO_RESPONSE') THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='SKIPPED' THEN 1 ELSE 0 END),
                  SUM(CASE WHEN status='INTERVIEW' THEN 1 ELSE 0 END)
           FROM userstate.applications GROUP BY fam"""
    ):
        by_fam[r[0]] = {"decided": r[1], "applied": r[2] or 0,
                        "skipped": r[3] or 0, "interview": r[4] or 0}
    out["conversion_by_role_family"] = by_fam

    out["integrity"] = {
        "orphan_applications": one(
            "SELECT COUNT(*) FROM userstate.applications a "
            "LEFT JOIN main.jobs j ON j.key=a.job_key WHERE j.key IS NULL"),
        "illegal_status": one(
            "SELECT COUNT(*) FROM userstate.applications WHERE status NOT IN "
            "('NEW','SHORTLISTED','APPLIED','INTERVIEW','REJECTED','NO_RESPONSE','SKIPPED')"),
        "applied_without_applied_at": one(
            "SELECT COUNT(*) FROM userstate.applications WHERE status='APPLIED' AND applied_at IS NULL"),
        "outcome_without_outcome_at": one(
            "SELECT COUNT(*) FROM userstate.applications "
            "WHERE status IN ('INTERVIEW','REJECTED','NO_RESPONSE') AND outcome_at IS NULL"),
        "duplicate_applications": one(
            "SELECT COUNT(*) FROM (SELECT job_key FROM userstate.applications "
            "GROUP BY job_key HAVING COUNT(*)>1)"),
        "missing_profile_version": one(
            "SELECT COUNT(*) FROM userstate.applications WHERE profile_version=''"),
        "duplicate_user_feedback": one(
            "SELECT COUNT(*) FROM (SELECT job_key,action FROM userstate.user_feedback "
            "GROUP BY job_key,action HAVING COUNT(*)>1)"),
    }
    # An orphan is expected and healthy once a posting ages out of discovery —
    # the snapshot is what keeps it interpretable.
    out["integrity"]["orphan_applications_note"] = (
        "orphans are expected when a posting leaves the discovery DB; "
        "the application snapshot retains company/title/url/role_family")
    return out


def render(snap: dict) -> str:
    lines = ["", f"OBSERVATION SNAPSHOT — {snap['timestamp'][:19]}Z", ""]
    lines.append(f"  discovery   : {snap['discovery_db']}  ({snap['jobs']} jobs, {snap['boards']} boards)")
    lines.append(f"  user state  : {snap['user_state_db']}")
    if snap.get("note"):
        lines += ["", f"  {snap['note']}", ""]
        return "\n".join(lines)
    lines.append(f"  applications: {snap['applications']}   user actions: {snap['user_actions']}"
                 f"   legacy feedback: {snap['legacy_feedback']}")
    lines += ["", "  Surfaced (Phase 1 stored priority):"]
    for k, v in sorted(snap["surfaced_screening"].items(), key=lambda kv: -kv[1]):
        lines.append(f"    {k:<12}{v:>7}")
    f = snap["funnel"]
    lines += ["", "  User funnel:",
              f"    shortlisted {f['shortlisted']}   applied {f['applied']}   skipped {f['skipped']}",
              f"    interview {f['interview']}   rejected {f['rejected']}   no response {f['no_response']}"]
    if snap["conversion_by_band"]:
        lines += ["", "  Conversion by priority band at decision time:"]
        for b in BANDS:
            d = snap["conversion_by_band"].get(b)
            if d:
                lines.append(f"    {b:<12} decided {d['decided']:>3}  applied {d['applied']:>3}"
                             f"  skipped {d['skipped']:>3}  interview {d['interview']:>3}")
    if snap["conversion_by_role_family"]:
        lines += ["", "  Conversion by role family:"]
        for fam, d in sorted(snap["conversion_by_role_family"].items()):
            lines.append(f"    {fam:<24} decided {d['decided']:>3}  applied {d['applied']:>3}"
                         f"  skipped {d['skipped']:>3}  interview {d['interview']:>3}")
    bad = {k: v for k, v in snap["integrity"].items()
           if isinstance(v, int) and v and k != "orphan_applications"}
    lines += ["", f"  Integrity: {'clean' if not bad else 'ISSUES ' + json.dumps(bad)}"]
    orph = snap["integrity"].get("orphan_applications") or 0
    if orph:
        lines.append(f"    {orph} application(s) reference a job no longer in discovery (expected)")
    lines.append("")
    return "\n".join(lines)
