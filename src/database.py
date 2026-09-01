"""SQLite-backed state management replacing the original JSON file approach.

Schema:
  jobs     — every job ever seen, with score/label and timestamps
  boards   — ATS board registry with health tracking
  cursors  — pagination state (replaces boards_cursor.json)
"""
from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional

log = logging.getLogger(__name__)


# Job descriptions dominate DB size (47 MB of 57 MB when this was added).
# They are only needed while a posting is live — long enough to score, alert,
# and run --tailor. Override with DESCRIPTION_RETENTION_DAYS.
DESCRIPTION_RETENTION_DAYS = int(os.environ.get("DESCRIPTION_RETENTION_DAYS", 30))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


CREATE_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs (
    key          TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    company      TEXT NOT NULL,
    title        TEXT NOT NULL,
    location     TEXT NOT NULL DEFAULT '',
    url          TEXT NOT NULL DEFAULT '',
    posted       TEXT NOT NULL DEFAULT '',
    score        INTEGER NOT NULL DEFAULT 0,
    label        TEXT NOT NULL DEFAULT 'no',
    work_type    TEXT NOT NULL DEFAULT '',
    salary       TEXT NOT NULL DEFAULT '',
    resume_match INTEGER NOT NULL DEFAULT 0,
    description  TEXT NOT NULL DEFAULT '',
    opportunity_score      INTEGER NOT NULL DEFAULT 0,
    priority               TEXT NOT NULL DEFAULT '',
    location_class         TEXT NOT NULL DEFAULT '',
    seniority              TEXT NOT NULL DEFAULT '',
    role_family            TEXT NOT NULL DEFAULT '',
    experience_min         INTEGER,
    experience_max         INTEGER,
    classification_reasons TEXT NOT NULL DEFAULT '',
    first_seen   TEXT NOT NULL,
    last_seen    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS boards (
    board_id     TEXT PRIMARY KEY,
    platform     TEXT NOT NULL,
    company      TEXT NOT NULL DEFAULT '',
    url          TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'active',
    last_checked TEXT,
    job_count    INTEGER NOT NULL DEFAULT 0,
    fail_count   INTEGER NOT NULL DEFAULT 0,
    fail_reason  TEXT NOT NULL DEFAULT '',
    first_seen   TEXT NOT NULL,
    last_seen    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cursors (
    name   TEXT PRIMARY KEY,
    value  TEXT NOT NULL DEFAULT '0'
);

-- Feedback table: stores user actions on individual jobs (applied / dismissed)
-- Used by the ML scoring layer to learn user preferences over time.
CREATE TABLE IF NOT EXISTS feedback (
    job_key    TEXT NOT NULL,
    action     TEXT NOT NULL,          -- 'applied' | 'dismissed' | 'interested'
    created_at TEXT NOT NULL,
    notes      TEXT NOT NULL DEFAULT ''
);

-- Phase 2 application queue. Deliberately separate from `feedback`: that
-- table stays the append-only event log (and the ML re-scorer's training
-- data), while this one holds the current derived status plus the score
-- snapshot taken when the user decided.
CREATE TABLE IF NOT EXISTS applications (
    job_key               TEXT PRIMARY KEY,
    status                TEXT NOT NULL DEFAULT 'NEW',
    priority_at_decision  INTEGER,
    fit_at_decision       INTEGER,
    screening_at_decision INTEGER,
    note                  TEXT NOT NULL DEFAULT '',
    -- Which candidate configuration produced the scores above. Resumes change;
    -- without this a snapshot cannot be compared to a later recomputation.
    profile_version       TEXT NOT NULL DEFAULT '',
    shortlisted_at        TEXT,
    applied_at            TEXT,
    outcome_at            TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_jobs_label  ON jobs(label);
CREATE INDEX IF NOT EXISTS idx_boards_platform ON boards(platform);
CREATE INDEX IF NOT EXISTS idx_boards_status   ON boards(status);
CREATE INDEX IF NOT EXISTS idx_feedback_key ON feedback(job_key);
CREATE INDEX IF NOT EXISTS idx_feedback_action ON feedback(action);
"""


class Database:
    def __init__(self, path: str, *, user_state: str | None = None,
                 attach_user_state: bool = True, readonly: bool = False) -> None:
        """Open the discovery database.

        `readonly=True` opens it via a mode=ro URI and skips schema creation and
        migration, so the file is not touched at all. The application CLI uses
        this: a user decision must never dirty state/jobs.db, and merely opening
        it read-write would, because migration and WAL both rewrite the file.
        An attached user-state database stays writable.
        """
        self.path = path
        self.readonly = readonly
        if readonly:
            # immutable=1 also suppresses -wal/-shm sidecar creation.
            uri = f"file:{os.path.abspath(path)}?mode=ro&immutable=1"
            self._conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        else:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self._conn = sqlite3.connect(path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.executescript(CREATE_SQL)
            self._migrate()
            self._conn.commit()

        # User-owned state lives in a separate file outside the repository, so
        # CI replacing state/jobs.db cannot destroy application history. It is
        # ATTACHed rather than opened separately so the queue and follow-up
        # queries can still JOIN discovery jobs against user status.
        self.user_state_path = None
        if attach_user_state:
            try:
                from .apply.user_state import attach as _attach, import_legacy
                self.user_state_path = _attach(self._conn, user_state)
                import_legacy(self._conn)   # one-time, non-destructive copy
            except Exception as exc:      # never block discovery on user state
                log.warning("User-state database unavailable (%s) — "
                            "application features disabled this run", exc)
                self.user_state_path = None
        log.debug("Database opened: %s (user state: %s)", path, self.user_state_path)

    @property
    def has_user_state(self) -> bool:
        return self.user_state_path is not None

    def _migrate(self) -> None:
        """Add new columns to existing databases (safe — uses ALTER TABLE IF NOT EXISTS pattern)."""
        existing = {
            row[1] for row in self._conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        additions = {
            "work_type":    "TEXT NOT NULL DEFAULT ''",
            "salary":       "TEXT NOT NULL DEFAULT ''",
            "resume_match": "INTEGER NOT NULL DEFAULT 0",
            "description":  "TEXT NOT NULL DEFAULT ''",
            # ── Phase 1 screening ────────────────────────────────────────────
            # All nullable-or-defaulted, so a database written by the previous
            # version opens unchanged and rows written before this migration
            # keep working with an empty priority / zero score.
            "opportunity_score":      "INTEGER NOT NULL DEFAULT 0",
            "priority":               "TEXT NOT NULL DEFAULT ''",
            "location_class":         "TEXT NOT NULL DEFAULT ''",
            "seniority":              "TEXT NOT NULL DEFAULT ''",
            "role_family":            "TEXT NOT NULL DEFAULT ''",
            "experience_min":         "INTEGER",
            "experience_max":         "INTEGER",
            # Reason codes, stored as a compact comma-separated list. Codes are
            # short and bounded; the JD text itself is deliberately NOT
            # duplicated here — description storage is already the dominant
            # contributor to DB size and this file is committed to git hourly.
            "classification_reasons": "TEXT NOT NULL DEFAULT ''",
        }
        for col, definition in additions.items():
            if col not in existing:
                self._conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {definition}")
                log.debug("DB migration: added column jobs.%s", col)

        # Phase 2: application queue. CREATE TABLE IF NOT EXISTS in CREATE_SQL
        # already handles both new and existing databases, so only the indexes
        # need adding here — and they must come after the table exists.
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(status)"
        )
        # Additive column for databases created before provenance was recorded.
        app_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(applications)")}
        if "profile_version" not in app_cols:
            self._conn.execute(
                "ALTER TABLE applications ADD COLUMN profile_version TEXT NOT NULL DEFAULT ''"
            )

        # Indexes on migrated columns must be created after the ALTER TABLEs.
        # On a pre-existing database the CREATE TABLE in CREATE_SQL is a no-op,
        # so an index declared there would reference a column that does not
        # exist yet and abort the whole script.
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority)"
        )

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # -------------------------------------------------------------------------
    # Job tracking
    # -------------------------------------------------------------------------

    def is_new_job(self, key: str) -> bool:
        """Return True if this job key has never been seen before."""
        row = self._conn.execute("SELECT 1 FROM jobs WHERE key=?", (key,)).fetchone()
        return row is None

    def mark_job_seen(
        self,
        *,
        key: str,
        source: str,
        company: str,
        title: str,
        location: str,
        url: str,
        posted: str,
        score: int,
        label: str,
        work_type: str = "",
        salary: str = "",
        resume_match: int = 0,
        description: str = "",
        opportunity_score: int = 0,
        priority: str = "",
        location_class: str = "",
        seniority: str = "",
        role_family: str = "",
        experience_min: int | None = None,
        experience_max: int | None = None,
        classification_reasons: list[str] | None = None,
    ) -> None:
        now = _now()
        reasons_csv = ",".join(classification_reasons or [])
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO jobs(key,source,company,title,location,url,posted,score,label,
                                 work_type,salary,resume_match,description,
                                 opportunity_score,priority,location_class,seniority,
                                 role_family,experience_min,experience_max,
                                 classification_reasons,first_seen,last_seen)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                    title=excluded.title,
                    location=excluded.location,
                    url=excluded.url,
                    posted=excluded.posted,
                    score=excluded.score,
                    label=excluded.label,
                    work_type=excluded.work_type,
                    salary=excluded.salary,
                    resume_match=excluded.resume_match,
                    description=excluded.description,
                    opportunity_score=excluded.opportunity_score,
                    priority=excluded.priority,
                    location_class=excluded.location_class,
                    seniority=excluded.seniority,
                    role_family=excluded.role_family,
                    experience_min=excluded.experience_min,
                    experience_max=excluded.experience_max,
                    classification_reasons=excluded.classification_reasons,
                    last_seen=excluded.last_seen
                """,
                (key, source, company, title, location, url, posted, score, label,
                 work_type, salary, resume_match, description,
                 opportunity_score, priority, location_class, seniority,
                 role_family, experience_min, experience_max, reasons_csv, now, now),
            )

    def source_is_bootstrapped(self, source: str) -> bool:
        """Return True if we have at least one job from this source (not first run)."""
        row = self._conn.execute(
            "SELECT 1 FROM jobs WHERE source=? LIMIT 1", (source,)
        ).fetchone()
        return row is not None

    def get_seen_keys(self, source: Optional[str] = None) -> set[str]:
        """Return all seen job keys, optionally filtered by source."""
        if source:
            rows = self._conn.execute("SELECT key FROM jobs WHERE source=?", (source,)).fetchall()
        else:
            rows = self._conn.execute("SELECT key FROM jobs").fetchall()
        return {r["key"] for r in rows}

    def job_count(self, source: Optional[str] = None) -> int:
        if source:
            return self._conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE source=?", (source,)
            ).fetchone()[0]
        return self._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # -------------------------------------------------------------------------
    # Board registry
    # -------------------------------------------------------------------------

    def is_board_dead(self, board_id: str) -> bool:
        row = self._conn.execute(
            "SELECT status FROM boards WHERE board_id=?", (board_id,)
        ).fetchone()
        return row is not None and row["status"] == "dead"

    def is_board_bootstrapped(self, board_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM boards WHERE board_id=?", (board_id,)
        ).fetchone()
        return row is not None

    def upsert_board(
        self,
        *,
        board_id: str,
        platform: str,
        company: str,
        url: str,
        status: str = "active",
        job_count: int = 0,
        fail_reason: str = "",
    ) -> None:
        now = _now()
        with self._tx() as conn:
            existing = conn.execute(
                "SELECT fail_count FROM boards WHERE board_id=?", (board_id,)
            ).fetchone()
            fail_count = (existing["fail_count"] + 1) if (existing and status == "dead") else 0
            conn.execute(
                """
                INSERT INTO boards(board_id,platform,company,url,status,last_checked,job_count,fail_count,fail_reason,first_seen,last_seen)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(board_id) DO UPDATE SET
                    status=excluded.status,
                    last_checked=excluded.last_checked,
                    job_count=excluded.job_count,
                    fail_count=excluded.fail_count,
                    fail_reason=excluded.fail_reason,
                    last_seen=excluded.last_seen
                """,
                (board_id, platform, company, url, status, now, job_count, fail_count, fail_reason, now, now),
            )

    def get_dead_boards(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM boards WHERE status='dead' ORDER BY board_id"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_board_stats(self) -> dict:
        total = self._conn.execute("SELECT COUNT(*) FROM boards").fetchone()[0]
        dead = self._conn.execute("SELECT COUNT(*) FROM boards WHERE status='dead'").fetchone()[0]
        active = self._conn.execute("SELECT COUNT(*) FROM boards WHERE status='active'").fetchone()[0]
        return {"total": total, "active": active, "dead": dead}

    # -------------------------------------------------------------------------
    # Cursors (pagination state)
    # -------------------------------------------------------------------------

    def get_cursor(self, name: str) -> int:
        row = self._conn.execute("SELECT value FROM cursors WHERE name=?", (name,)).fetchone()
        if row is None:
            return 0
        try:
            return max(int(row["value"]), 0)
        except ValueError:
            return 0

    def set_cursor(self, name: str, value: int) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO cursors(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                (name, str(max(value, 0))),
            )

    # -------------------------------------------------------------------------
    # Reporting helpers
    # -------------------------------------------------------------------------

    def expire_old_jobs(self, days: int = 60) -> int:
        """Delete jobs not seen within the last `days` days. Returns count deleted."""
        with self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM jobs WHERE last_seen < datetime('now', ?)",
                (f"-{days} days",),
            )
            deleted = cur.rowcount
        if deleted:
            log.info("Expired %d stale job(s) older than %d days", deleted, days)
        return deleted

    def prune_old_descriptions(self, days: int = DESCRIPTION_RETENTION_DAYS) -> tuple[int, int]:
        """Blank stored JD text for jobs not seen within the last `days` days.

        Job descriptions dominate database size — they were 47 MB of a 57 MB
        file — and they are only needed while a posting is live: long enough to
        score it, alert on it, and run `--tailor` against it. Once a job has
        aged out, its score and metadata are retained but the raw text is dead
        weight that gets recommitted to git on every workflow run.

        The row itself is kept (dedup and `count_company_posts` still need it);
        only `description` is cleared. Returns ``(rows_pruned, bytes_freed)``.
        """
        row = self._conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(description)), 0) FROM jobs "
            "WHERE description != '' AND last_seen < datetime('now', ?)",
            (f"-{days} days",),
        ).fetchone()
        n, freed = (row[0], row[1]) if row else (0, 0)
        if not n:
            return 0, 0
        with self._tx() as conn:
            conn.execute(
                "UPDATE jobs SET description = '' "
                "WHERE description != '' AND last_seen < datetime('now', ?)",
                (f"-{days} days",),
            )
        log.info("Pruned descriptions from %d job(s) older than %d days (%.1f MB)",
                 n, days, freed / 1e6)
        return n, freed

    def vacuum(self) -> int:
        """Reclaim free pages so the file actually shrinks on disk.

        DELETE/UPDATE only mark pages free; without VACUUM the file never
        shrinks and git keeps committing the same size. Must run outside a
        transaction. Returns bytes reclaimed.
        """
        before = os.path.getsize(self.path) if os.path.exists(self.path) else 0
        try:
            self._conn.execute("VACUUM")
        except Exception as e:
            log.warning("VACUUM failed: %s", e)
            return 0
        after = os.path.getsize(self.path) if os.path.exists(self.path) else 0
        freed = max(0, before - after)
        if freed:
            log.info("VACUUM reclaimed %.1f MB (%.1f -> %.1f MB)",
                     freed / 1e6, before / 1e6, after / 1e6)
        return freed

    def is_duplicate_title(self, company: str, title: str) -> bool:
        """Return True if we already have a job with the same company+title in the DB."""
        row = self._conn.execute(
            "SELECT 1 FROM jobs WHERE lower(company)=lower(?) AND lower(title)=lower(?) LIMIT 1",
            (company.strip(), title.strip()),
        ).fetchone()
        return row is not None

    def get_stats(self) -> dict:
        """Return summary statistics used by the weekly health-check email."""
        total = self._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        new_24h = self._conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE first_seen >= datetime('now','-1 day')"
        ).fetchone()[0]
        new_7d = self._conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE first_seen >= datetime('now','-7 days')"
        ).fetchone()[0]
        yes_count = self._conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE label='yes'"
        ).fetchone()[0]
        maybe_count = self._conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE label='maybe'"
        ).fetchone()[0]
        last_activity = self._conn.execute(
            "SELECT MAX(last_seen) FROM jobs"
        ).fetchone()[0] or "never"
        board_stats = self.get_board_stats()
        return {
            "total_jobs": total,
            "new_24h": new_24h,
            "new_7d": new_7d,
            "yes_count": yes_count,
            "maybe_count": maybe_count,
            "last_activity": last_activity,
            "boards": board_stats,
        }

    # -------------------------------------------------------------------------
    # Feedback (ML training signal)
    # -------------------------------------------------------------------------

    def count_company_posts(self, company: str, title: str) -> int:
        """Count how many times we have seen a similar title from this company.
        Used by the ghost detector to flag evergreen / re-posted listings."""
        title_prefix = (title or "")[:18].lower()
        row = self._conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE lower(company)=lower(?) AND lower(title) LIKE ?",
            (company.strip(), f"{title_prefix}%"),
        ).fetchone()
        return row[0] if row else 0

    def get_followup_due(self, days: int = 7) -> list[dict]:
        """Return jobs applied to >= days ago that still need a follow-up.

        Eligibility has two paths, because the application queue was added
        after this method:

        * **Job has an `applications` row** — that status is authoritative.
          Only APPLIED can be due. This is what stops a job the user explicitly
          marked NO_RESPONSE from being suggested for follow-up forever: the
          queue records that outcome, but writing a feedback event for it would
          mean inventing an action the preference model does not understand.

        * **No `applications` row** (historical jobs) — the original
          feedback-only rule applies unchanged: due unless a 'followed_up',
          'responded', 'rejected' or 'offer' event followed the 'applied' one.

        Read-only: this method never writes a feedback event, so ML training
        semantics are untouched.
        """
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        # Applied events now live in user state; legacy ones remain in the
        # discovery DB. The union covers both without double-counting.
        union = self._feedback_union_sql()
        rows = self._conn.execute(
            f"""
            SELECT f.job_key,
                   f.created_at   AS applied_at,
                   j.company,
                   j.title,
                   j.url,
                   j.location,
                   j.work_type,
                   j.salary
            FROM   ({union}) f
            JOIN   jobs j ON j.key = f.job_key
            LEFT   JOIN userstate.applications a ON a.job_key = f.job_key
            WHERE  f.action = 'applied'
              AND  f.created_at <= ?
              AND  CASE
                       WHEN a.status IS NOT NULL THEN a.status = 'APPLIED'
                       ELSE NOT EXISTS (
                                SELECT 1 FROM ({union}) f2
                                WHERE  f2.job_key = f.job_key
                                  AND  f2.action IN ('followed_up','responded','rejected','offer')
                                  AND  f2.created_at > f.created_at
                            )
                   END
            ORDER  BY f.created_at ASC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    def record_feedback(self, job_key: str, action: str, notes: str = "",
                        origin: str = "user") -> bool:
        """Store a feedback event.

        `origin` decides where it lands, and that routing is the whole point of
        the Phase 2.1 split:

        * ``"user"``   — an explicit human decision. Written to the durable
          user-state database, where the preference model reads its labels.
        * ``"system"`` — a machine-generated signal, currently only the
          auto-seed in `_dispatch_results` that fires when a resume match clears
          AUTO_INTERESTED_THRESHOLD. Written to the discovery database, which
          keeps existing behaviour working, and is never read as a preference
          label. Without this a prediction would train the model that produced
          it.

        Returns True if the job_key exists in the jobs table.
        """
        valid_actions = {
            "applied", "dismissed", "interested",
            "followed_up", "responded", "rejected", "offer",
        }
        if action not in valid_actions:
            raise ValueError(f"action must be one of {valid_actions}, got {action!r}")
        # Verify job exists (warn but still record so feedback isn't lost)
        exists = self._conn.execute("SELECT 1 FROM jobs WHERE key=?", (job_key,)).fetchone() is not None
        if origin == "user" and self.has_user_state:
            with self._tx() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO userstate.user_feedback"
                    "(job_key, action, created_at, notes, origin) VALUES(?,?,?,?,'user')",
                    (job_key, action, _now(), notes),
                )
            return exists
        # System signals, and the no-user-state fallback, stay in discovery.
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO feedback(job_key, action, created_at, notes) VALUES(?,?,?,?)",
                (job_key, action, _now(), notes),
            )
        return exists

    def _feedback_union_sql(self) -> str:
        """The authoritative source of USER preference events.

        After the Phase 2.1 split this is user state alone, not a union with
        discovery. A running union would keep pulling in future
        `discovery.feedback` rows, and those are now exclusively
        system-generated `interested` events from the auto-seed — letting the
        preference model train on its own predictions.

        Pre-split history is brought across once by
        `user_state.import_legacy`, so nothing is lost. The discovery table
        remains readable for system signals and is never consulted here.
        """
        if not self.has_user_state:
            # No user state (e.g. a discovery-only utility connection): fall
            # back to legacy history, minus the ambiguous auto-seed action.
            return ("SELECT job_key, action, created_at, notes FROM main.feedback "
                    "WHERE action <> 'interested'")
        return "SELECT job_key, action, created_at, notes FROM userstate.user_feedback"

    def system_signal_stats(self) -> dict:
        """Counts of machine-generated signals, reported separately from user
        behaviour so the two are never conflated."""
        return {r["action"]: r["cnt"] for r in self._conn.execute(
            "SELECT action, COUNT(*) cnt FROM main.feedback GROUP BY action")}

    def get_feedback_stats(self) -> dict:
        """Return counts of each feedback action (legacy + user state, deduped)."""
        rows = self._conn.execute(
            f"SELECT action, COUNT(*) as cnt FROM ({self._feedback_union_sql()}) "
            "GROUP BY action"
        ).fetchall()
        stats = {"applied": 0, "dismissed": 0, "interested": 0, "total": 0}
        for r in rows:
            stats[r["action"]] = r["cnt"]
        stats["total"] = sum(v for k, v in stats.items() if k != "total")
        return stats

    def get_feedback_jobs(self, action: str | None = None) -> list[dict]:
        """Return feedback entries (legacy + user state, deduped), optionally
        filtered by action. This is what the preference model trains on."""
        union = self._feedback_union_sql()
        base = (f"SELECT f.job_key, f.action, f.created_at, f.notes, "
                f"       j.company, j.title, j.url, j.score, j.label, j.source, j.work_type "
                f"FROM ({union}) f LEFT JOIN jobs j ON j.key = f.job_key ")
        if action:
            rows = self._conn.execute(
                base + "WHERE f.action = ? ORDER BY f.created_at DESC", (action,)
            ).fetchall()
        else:
            rows = self._conn.execute(base + "ORDER BY f.created_at DESC").fetchall()
        return [dict(r) for r in rows]

    def export_dead_boards_csv(self, out_path: str) -> None:
        import csv

        rows = self.get_dead_boards()
        if not rows or not out_path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        fieldnames = ["board_id", "platform", "company", "url", "fail_count", "fail_reason", "last_checked"]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log.info("Exported %d dead boards to %s", len(rows), out_path)
