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

CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_jobs_label  ON jobs(label);
CREATE INDEX IF NOT EXISTS idx_boards_platform ON boards(platform);
CREATE INDEX IF NOT EXISTS idx_boards_status   ON boards(status);
CREATE INDEX IF NOT EXISTS idx_feedback_key ON feedback(job_key);
CREATE INDEX IF NOT EXISTS idx_feedback_action ON feedback(action);
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(CREATE_SQL)
        self._migrate()
        self._conn.commit()
        log.debug("Database opened: %s", path)

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
        }
        for col, definition in additions.items():
            if col not in existing:
                self._conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {definition}")
                log.debug("DB migration: added column jobs.%s", col)

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
    ) -> None:
        now = _now()
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO jobs(key,source,company,title,location,url,posted,score,label,
                                 work_type,salary,resume_match,description,first_seen,last_seen)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                    last_seen=excluded.last_seen
                """,
                (key, source, company, title, location, url, posted, score, label,
                 work_type, salary, resume_match, description, now, now),
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

    def record_feedback(self, job_key: str, action: str, notes: str = "") -> bool:
        """Store user feedback for a job. action: 'applied' | 'dismissed' | 'interested'.
        Returns True if the job_key exists in the jobs table, False otherwise.
        """
        valid_actions = {"applied", "dismissed", "interested"}
        if action not in valid_actions:
            raise ValueError(f"action must be one of {valid_actions}, got {action!r}")
        # Verify job exists (warn but still record so feedback isn't lost)
        exists = self._conn.execute("SELECT 1 FROM jobs WHERE key=?", (job_key,)).fetchone() is not None
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO feedback(job_key, action, created_at, notes) VALUES(?,?,?,?)",
                (job_key, action, _now(), notes),
            )
        return exists

    def get_feedback_stats(self) -> dict:
        """Return counts of each feedback action."""
        rows = self._conn.execute(
            "SELECT action, COUNT(*) as cnt FROM feedback GROUP BY action"
        ).fetchall()
        stats = {"applied": 0, "dismissed": 0, "interested": 0, "total": 0}
        for r in rows:
            stats[r["action"]] = r["cnt"]
        stats["total"] = sum(v for k, v in stats.items() if k != "total")
        return stats

    def get_feedback_jobs(self, action: str | None = None) -> list[dict]:
        """Return all feedback entries, optionally filtered by action."""
        if action:
            rows = self._conn.execute(
                """SELECT f.job_key, f.action, f.created_at, f.notes,
                          j.company, j.title, j.url, j.score, j.label
                   FROM feedback f LEFT JOIN jobs j ON j.key = f.job_key
                   WHERE f.action = ? ORDER BY f.created_at DESC""",
                (action,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT f.job_key, f.action, f.created_at, f.notes,
                          j.company, j.title, j.url, j.score, j.label
                   FROM feedback f LEFT JOIN jobs j ON j.key = f.job_key
                   ORDER BY f.created_at DESC"""
            ).fetchall()
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
