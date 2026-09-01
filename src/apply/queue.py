"""Application queue — status, transitions and outcome metrics.

Built *on top of* the existing `feedback` table rather than replacing it. That
table is already an append-only event log with an established vocabulary
(applied / dismissed / interested / followed_up / responded / rejected /
offer), and three things depend on it: the ML re-scorer's training data, the
local dashboard, and `get_followup_due`. Breaking it to introduce a second
status system would cost more than it bought.

So every transition writes both:

  * a `feedback` event   — preserves history and everything that reads it
  * an `applications` row — the current derived status plus the score snapshot
                            taken at the moment the user decided

The snapshot is the point. Recording what the system thought at decision time
is what will later let us ask whether application_priority actually predicted
interviews, which a mutable join back to `jobs` could not answer.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

# ── Status vocabulary ──────────────────────────────────────────────────────
NEW          = "NEW"
SHORTLISTED  = "SHORTLISTED"
APPLIED      = "APPLIED"
INTERVIEW    = "INTERVIEW"
REJECTED     = "REJECTED"
NO_RESPONSE  = "NO_RESPONSE"
SKIPPED      = "SKIPPED"

STATUSES = (NEW, SHORTLISTED, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED)

# Statuses that take a job out of the actionable queue.
TERMINAL_STATUSES = (APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED)

# Each status maps onto the existing feedback vocabulary so the ML re-scorer,
# the dashboard and the follow-up reminder keep working unchanged.
_FEEDBACK_ACTION = {
    SHORTLISTED: "interested",
    APPLIED:     "applied",
    INTERVIEW:   "responded",
    REJECTED:    "rejected",
    SKIPPED:     "dismissed",
}

# Legal transitions. An applied job must not silently become NEW again: losing
# that fact would corrupt the outcome data this phase exists to collect.
_ALLOWED: dict[str, set[str]] = {
    NEW:         {SHORTLISTED, APPLIED, SKIPPED},
    SHORTLISTED: {APPLIED, SKIPPED, NEW},
    APPLIED:     {INTERVIEW, REJECTED, NO_RESPONSE},
    INTERVIEW:   {REJECTED, NO_RESPONSE},
    REJECTED:    set(),
    NO_RESPONSE: {INTERVIEW, REJECTED},
    SKIPPED:     {NEW, SHORTLISTED},
}


class IllegalTransition(ValueError):
    """Raised when a status change would lose information already recorded."""


@dataclass(frozen=True)
class Application:
    job_key: str
    status: str
    priority_at_decision: Optional[int] = None
    fit_at_decision: Optional[int] = None
    screening_at_decision: Optional[int] = None
    profile_version: str = ""
    note: str = ""
    company: str = ""
    title: str = ""
    url: str = ""
    role_family: str = ""
    shortlisted_at: Optional[str] = None
    applied_at: Optional[str] = None
    outcome_at: Optional[str] = None
    updated_at: Optional[str] = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ApplicationQueue:
    """Thin service over the `applications` table and the `feedback` log."""

    def __init__(self, db) -> None:
        self.db = db
        self._conn = db._conn

    # ── Reads ──────────────────────────────────────────────────────────────
    def get(self, job_key: str) -> Optional[Application]:
        row = self._conn.execute(
            "SELECT * FROM userstate.applications WHERE job_key=?", (job_key,)
        ).fetchone()
        return self._row_to_app(row) if row else None

    def status_of(self, job_key: str) -> str:
        app = self.get(job_key)
        return app.status if app else NEW

    def statuses_for(self, job_keys) -> dict[str, str]:
        """Bulk status lookup — one query rather than one per job."""
        keys = list(job_keys)
        if not keys:
            return {}
        out: dict[str, str] = {}
        for i in range(0, len(keys), 500):
            chunk = keys[i:i + 500]
            q = f"SELECT job_key, status FROM userstate.applications WHERE job_key IN ({','.join('?' * len(chunk))})"
            for r in self._conn.execute(q, chunk):
                out[r["job_key"]] = r["status"]
        return out

    def list(self, status: Optional[str] = None, limit: int = 100) -> list[Application]:
        if status:
            rows = self._conn.execute(
                "SELECT * FROM userstate.applications WHERE status=? ORDER BY updated_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM userstate.applications ORDER BY updated_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_app(r) for r in rows]

    def excluded_keys(self) -> set[str]:
        """Job keys that should not appear in the actionable shortlist."""
        rows = self._conn.execute(
            f"SELECT job_key FROM userstate.applications WHERE status IN "
            f"({','.join('?' * len(TERMINAL_STATUSES))})",
            TERMINAL_STATUSES,
        ).fetchall()
        return {r["job_key"] for r in rows}

    # ── Writes ─────────────────────────────────────────────────────────────
    def set_status(
        self,
        job_key: str,
        status: str,
        *,
        priority: Optional[int] = None,
        fit: Optional[int] = None,
        screening: Optional[int] = None,
        note: str = "",
        profile_version: str = "",
        force: bool = False,
    ) -> Application:
        """Record a user decision. Writes only to the user-state database."""
        if status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, got {status!r}")

        current = self.get(job_key)
        current_status = current.status if current else NEW
        if not force and status != current_status and status not in _ALLOWED[current_status]:
            raise IllegalTransition(
                f"{current_status} -> {status} is not allowed. "
                f"Legal next states: {sorted(_ALLOWED[current_status]) or 'none (terminal)'}"
            )

        # Snapshot the job's identity so the application stays interpretable
        # after the posting leaves the discovery database. Deliberately minimal:
        # no description, no scores that would go stale.
        cols = {r[1] for r in self._conn.execute("PRAGMA main.table_info(jobs)")}
        wanted = [c for c in ("company", "title", "url", "role_family") if c in cols]
        snap = None
        if wanted:
            snap = self._conn.execute(
                f"SELECT {','.join(wanted)} FROM main.jobs WHERE key=?", (job_key,)
            ).fetchone()

        def _snap(field: str) -> str:
            prior = getattr(current, field, "") if current else ""
            if prior:
                return prior
            if snap is not None and field in wanted:
                return snap[field] or ""
            return ""

        company, title = _snap("company"), _snap("title")
        url, role_family = _snap("url"), _snap("role_family")

        now = _now()
        shortlisted_at = current.shortlisted_at if current else None
        applied_at = current.applied_at if current else None
        outcome_at = current.outcome_at if current else None
        if status == SHORTLISTED and not shortlisted_at:
            shortlisted_at = now
        if status == APPLIED and not applied_at:
            applied_at = now
        if status in (INTERVIEW, REJECTED, NO_RESPONSE):
            outcome_at = now

        # Snapshots are written once, at the first decision, and never
        # overwritten by a later status change — they record what the system
        # believed when the user acted, not what it believes now.
        priority  = current.priority_at_decision  if current and current.priority_at_decision  is not None and priority  is None else priority
        fit       = current.fit_at_decision       if current and current.fit_at_decision       is not None and fit       is None else fit
        screening = current.screening_at_decision if current and current.screening_at_decision is not None and screening is None else screening
        note = note or (current.note if current else "")
        profile_version = profile_version or (current.profile_version if current else "")

        with self.db._tx() as conn:
            conn.execute(
                """
                INSERT INTO userstate.applications(job_key,status,priority_at_decision,fit_at_decision,
                                         screening_at_decision,profile_version,note,
                                         company,title,url,role_family,
                                         shortlisted_at,applied_at,outcome_at,
                                         created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(job_key) DO UPDATE SET
                    status=excluded.status,
                    priority_at_decision=COALESCE(applications.priority_at_decision, excluded.priority_at_decision),
                    fit_at_decision=COALESCE(applications.fit_at_decision, excluded.fit_at_decision),
                    screening_at_decision=COALESCE(applications.screening_at_decision, excluded.screening_at_decision),
                    profile_version=COALESCE(NULLIF(applications.profile_version,''), excluded.profile_version),
                    note=excluded.note,
                    company=COALESCE(NULLIF(applications.company,''), excluded.company),
                    title=COALESCE(NULLIF(applications.title,''), excluded.title),
                    url=COALESCE(NULLIF(applications.url,''), excluded.url),
                    role_family=COALESCE(NULLIF(applications.role_family,''), excluded.role_family),
                    shortlisted_at=excluded.shortlisted_at,
                    applied_at=excluded.applied_at,
                    outcome_at=excluded.outcome_at,
                    updated_at=excluded.updated_at
                """,
                (job_key, status, priority, fit, screening, profile_version, note,
                 company, title, url, role_family,
                 shortlisted_at, applied_at, outcome_at, now, now),
            )

        # Write the feedback event only when the status actually changed, and
        # only once per (job, action). Re-running `applied` three times used to
        # append three identical rows, which the ML re-scorer would then treat
        # as three separate training samples and overweight that one job.
        action = _FEEDBACK_ACTION.get(status)
        if action and status != current_status:
            # Written to user state, not the discovery DB: a user action must
            # never dirty state/jobs.db. The UNIQUE(job_key, action) constraint
            # plus this guard keep one action to exactly one preference sample.
            already = self._conn.execute(
                "SELECT 1 FROM userstate.user_feedback WHERE job_key=? AND action=? LIMIT 1",
                (job_key, action),
            ).fetchone()
            if not already:
                try:
                    with self.db._tx() as conn:
                        conn.execute(
                            "INSERT OR IGNORE INTO userstate.user_feedback"
                            "(job_key,action,created_at,notes,origin) VALUES(?,?,?,?,'user')",
                            (job_key, action, now, note),
                        )
                except Exception:  # secondary log; never block a status change
                    pass

        return self.get(job_key)

    # ── Metrics ────────────────────────────────────────────────────────────
    def metrics(self, days: Optional[int] = None) -> dict:
        """Outcome counts and rates. Collection only — no model training here."""
        where, params = "", []
        if days:
            cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
            where = "WHERE updated_at >= ?"
            params = [datetime.fromtimestamp(cutoff, timezone.utc).isoformat()]

        counts = {s: 0 for s in STATUSES}
        for r in self._conn.execute(
            f"SELECT status, COUNT(*) c FROM userstate.applications {where} GROUP BY status", params
        ):
            counts[r["status"]] = r["c"]

        applied_total = counts[APPLIED] + counts[INTERVIEW] + counts[REJECTED] + counts[NO_RESPONSE]
        interviews = counts[INTERVIEW]

        by_family = {}
        for r in self._conn.execute(
            """SELECT a.role_family fam, COUNT(*) c,
                      SUM(CASE WHEN a.status='INTERVIEW' THEN 1 ELSE 0 END) iv
               FROM userstate.applications a
               WHERE a.status IN ('APPLIED','INTERVIEW','REJECTED','NO_RESPONSE')
               GROUP BY a.role_family"""
        ):
            by_family[r["fam"] or "unknown"] = {"applications": r["c"], "interviews": r["iv"] or 0}

        by_band = {}
        for r in self._conn.execute(
            """SELECT CASE
                        WHEN priority_at_decision >= 88 THEN 'APPLY_FIRST'
                        WHEN priority_at_decision >= 78 THEN 'HIGH'
                        WHEN priority_at_decision >= 66 THEN 'MEDIUM'
                        WHEN priority_at_decision >= 55 THEN 'REVIEW'
                        ELSE 'LOW' END AS band,
                      COUNT(*) c,
                      SUM(CASE WHEN status='INTERVIEW' THEN 1 ELSE 0 END) iv
               FROM userstate.applications
               WHERE status IN ('APPLIED','INTERVIEW','REJECTED','NO_RESPONSE')
                 AND priority_at_decision IS NOT NULL
               GROUP BY band"""
        ):
            by_band[r["band"]] = {"applications": r["c"], "interviews": r["iv"] or 0}

        avg = self._conn.execute(
            """SELECT AVG(priority_at_decision) a FROM userstate.applications
               WHERE status IN ('APPLIED','INTERVIEW','REJECTED','NO_RESPONSE')
                 AND priority_at_decision IS NOT NULL"""
        ).fetchone()["a"]

        return {
            "counts": counts,
            "applications_total": applied_total,
            "interviews": interviews,
            "interview_rate": round(interviews / applied_total, 3) if applied_total else None,
            "by_role_family": by_family,
            "by_priority_band": by_band,
            "avg_priority_at_decision": round(avg, 1) if avg is not None else None,
        }

    # ── Helpers ────────────────────────────────────────────────────────────
    @staticmethod
    def _row_to_app(row) -> Application:
        return Application(
            job_key=row["job_key"], status=row["status"],
            priority_at_decision=row["priority_at_decision"],
            fit_at_decision=row["fit_at_decision"],
            screening_at_decision=row["screening_at_decision"],
            profile_version=(row["profile_version"] if "profile_version" in row.keys() else "") or "",
            note=row["note"] or "",
            company=row["company"] if "company" in row.keys() else "",
            title=row["title"] if "title" in row.keys() else "",
            url=row["url"] if "url" in row.keys() else "",
            role_family=row["role_family"] if "role_family" in row.keys() else "",
            shortlisted_at=row["shortlisted_at"], applied_at=row["applied_at"],
            outcome_at=row["outcome_at"], updated_at=row["updated_at"],
        )
