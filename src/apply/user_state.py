"""Durable user-owned state, separate from CI-owned discovery state.

`state/jobs.db` is a tracked binary file that the scheduled workflows rewrite
and push 25-30 times a day. Application decisions used to live in that same
file, so a routine `git pull` could destroy them — which would have made the
Phase 2 measurement period unmeasurable.

Ownership after the split:

    state/jobs.db              jobs, boards, cursors, legacy feedback
                               owned by CI; replaced freely
    ~/.job-system/user_state.db  applications, user actions
                               owned only by explicit user commands

The user database is ATTACHed to the discovery connection rather than opened
separately, so the queue and follow-up queries can still JOIN across the two in
one statement. Writes are directed at `userstate.*` tables, which land in the
external file; the discovery file is never modified by a user action.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

ENV_VAR = "JOB_USER_STATE_DB"
DEFAULT_DIR = Path.home() / ".job-system"
DEFAULT_NAME = "user_state.db"
SCHEMA_ALIAS = "userstate"

# Feedback actions that represent a real user decision. `interested` is also
# written automatically by the auto-seed in main.py when a resume match clears
# AUTO_INTERESTED_THRESHOLD, so it is system-generated there and user-generated
# from the dashboard — see `legacy_user_feedback_actions` below.
USER_ACTIONS = ("applied", "dismissed", "interested",
                "followed_up", "responded", "rejected", "offer")

# Actions safe to import from pre-split discovery history.
#
# 'interested' is deliberately excluded: in the legacy schema it is ambiguous —
# the dashboard writes it as a user action, and `_dispatch_results` writes it
# automatically whenever a resume match clears AUTO_INTERESTED_THRESHOLD. There
# is no origin column to tell them apart, and importing a system prediction as a
# positive preference label would let the model train on its own output.
# Production carries zero 'interested' rows, so nothing real is lost.
LEGACY_IMPORT_ACTIONS = ("applied", "dismissed", "followed_up",
                         "responded", "rejected", "offer")
IMPORT_MARKER = "legacy_import_at"

USER_STATE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_ALIAS}.applications (
    job_key               TEXT PRIMARY KEY,
    status                TEXT NOT NULL DEFAULT 'NEW',
    priority_at_decision  INTEGER,
    fit_at_decision       INTEGER,
    screening_at_decision INTEGER,
    profile_version       TEXT NOT NULL DEFAULT '',
    note                  TEXT NOT NULL DEFAULT '',
    -- Minimal job identity, so an application stays interpretable after the
    -- posting disappears from the discovery DB. Deliberately not the full row:
    -- no description, no scores that would go stale.
    company               TEXT NOT NULL DEFAULT '',
    title                 TEXT NOT NULL DEFAULT '',
    url                   TEXT NOT NULL DEFAULT '',
    role_family           TEXT NOT NULL DEFAULT '',
    shortlisted_at        TEXT,
    applied_at            TEXT,
    outcome_at            TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS {SCHEMA_ALIAS}.idx_us_applications_status
    ON applications(status);

-- User action events. Mirrors the shape of the legacy `feedback` table so the
-- preference model can read both with one union.
CREATE TABLE IF NOT EXISTS {SCHEMA_ALIAS}.user_feedback (
    job_key    TEXT NOT NULL,
    action     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    notes      TEXT NOT NULL DEFAULT '',
    origin     TEXT NOT NULL DEFAULT 'user',
    UNIQUE(job_key, action)
);

CREATE INDEX IF NOT EXISTS {SCHEMA_ALIAS}.idx_us_feedback_action
    ON user_feedback(action);

CREATE TABLE IF NOT EXISTS {SCHEMA_ALIAS}.meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def user_state_path(override: str | None = None) -> Path:
    """Resolve the user-state DB path.

    Precedence: explicit argument, then $JOB_USER_STATE_DB, then
    ~/.job-system/user_state.db. Never a hard-coded machine path.
    """
    raw = override or os.environ.get(ENV_VAR, "")
    if raw:
        return Path(os.path.expanduser(raw)).resolve()
    return (DEFAULT_DIR / DEFAULT_NAME).resolve()


def attach(conn: sqlite3.Connection, path: str | os.PathLike | None = None) -> Path:
    """Create if needed, ATTACH, and migrate the user-state database.

    Idempotent: safe to call on every connection open.
    """
    p = Path(path) if path else user_state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn.execute("ATTACH DATABASE ? AS " + SCHEMA_ALIAS, (str(p),))
    conn.executescript(USER_STATE_SCHEMA)
    _migrate(conn)
    conn.commit()
    return p


def detach(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("DETACH DATABASE " + SCHEMA_ALIAS)
    except sqlite3.Error:
        pass


def _migrate(conn: sqlite3.Connection) -> None:
    """Additive migrations for user-state databases created by older versions."""
    cols = {r[1] for r in conn.execute(f"PRAGMA {SCHEMA_ALIAS}.table_info(applications)")}
    for col, ddl in (("company", "TEXT NOT NULL DEFAULT ''"),
                     ("title", "TEXT NOT NULL DEFAULT ''"),
                     ("url", "TEXT NOT NULL DEFAULT ''"),
                     ("role_family", "TEXT NOT NULL DEFAULT ''"),
                     ("profile_version", "TEXT NOT NULL DEFAULT ''")):
        if col not in cols:
            conn.execute(f"ALTER TABLE {SCHEMA_ALIAS}.applications ADD COLUMN {col} {ddl}")


def legacy_user_feedback_actions() -> tuple[str, ...]:
    """Legacy actions provably attributable to a user decision."""
    return LEGACY_IMPORT_ACTIONS


def import_complete(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        f"SELECT value FROM {SCHEMA_ALIAS}.meta WHERE key=?", (IMPORT_MARKER,)
    ).fetchone()
    return row is not None


def import_legacy(conn: sqlite3.Connection, *, dry_run: bool = False,
                  force: bool = False) -> dict:
    """Bounded, one-time, non-destructive copy of pre-split history.

    Runs once per user-state database and then records a marker in `meta`, so
    later discovery feedback — which after the split is only ever
    system-generated — can never flow into the preference dataset. Copies rather
    than moves; the discovery database is untouched.
    """
    stats = {"feedback_copied": 0, "applications_copied": 0,
             "already_present": 0, "skipped_already_imported": False}

    if not force and import_complete(conn):
        stats["skipped_already_imported"] = True
        return stats

    rows = conn.execute(
        "SELECT job_key, action, MIN(created_at) AS created_at, MAX(notes) AS notes "
        "FROM main.feedback WHERE action IN "
        f"({','.join('?' * len(LEGACY_IMPORT_ACTIONS))}) GROUP BY job_key, action",
        LEGACY_IMPORT_ACTIONS,
    ).fetchall()
    for r in rows:
        exists = conn.execute(
            f"SELECT 1 FROM {SCHEMA_ALIAS}.user_feedback WHERE job_key=? AND action=?",
            (r[0], r[1]),
        ).fetchone()
        if exists:
            stats["already_present"] += 1
            continue
        if not dry_run:
            conn.execute(
                f"INSERT OR IGNORE INTO {SCHEMA_ALIAS}.user_feedback"
                "(job_key,action,created_at,notes,origin) VALUES(?,?,?,?,'legacy')",
                (r[0], r[1], r[2], r[3] or ""),
            )
        stats["feedback_copied"] += 1

    # A pre-split `applications` table may exist in the discovery DB.
    has_apps = conn.execute(
        "SELECT COUNT(*) FROM main.sqlite_master WHERE type='table' AND name='applications'"
    ).fetchone()[0]
    if has_apps:
        for r in conn.execute("SELECT * FROM main.applications").fetchall():
            d = dict(zip([c[0] for c in conn.execute(
                "SELECT * FROM main.applications LIMIT 0").description], r))
            if conn.execute(
                f"SELECT 1 FROM {SCHEMA_ALIAS}.applications WHERE job_key=?", (d["job_key"],)
            ).fetchone():
                continue
            if not dry_run:
                conn.execute(
                    f"INSERT INTO {SCHEMA_ALIAS}.applications"
                    "(job_key,status,priority_at_decision,fit_at_decision,screening_at_decision,"
                    " profile_version,note,shortlisted_at,applied_at,outcome_at,created_at,updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (d["job_key"], d["status"], d.get("priority_at_decision"),
                     d.get("fit_at_decision"), d.get("screening_at_decision"),
                     d.get("profile_version", ""), d.get("note", ""),
                     d.get("shortlisted_at"), d.get("applied_at"), d.get("outcome_at"),
                     d.get("created_at") or _now(), d.get("updated_at") or _now()),
                )
            stats["applications_copied"] += 1

    if not dry_run:
        conn.execute(
            f"INSERT OR REPLACE INTO {SCHEMA_ALIAS}.meta(key,value) VALUES('legacy_import_at',?)",
            (_now(),),
        )
        conn.commit()
    return stats


def backup(path: str | os.PathLike | None = None, dest_dir: str | os.PathLike | None = None) -> Path:
    """Timestamped backup via SQLite's backup API (consistent under concurrent writes)."""
    src = Path(path) if path else user_state_path()
    if not src.exists():
        raise FileNotFoundError(f"No user-state database at {src}")
    out_dir = Path(dest_dir) if dest_dir else src.parent / "backups"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_dir / f"user_state-{stamp}.db"
    source = sqlite3.connect(str(src))
    target = sqlite3.connect(str(out))
    try:
        source.backup(target)      # not a raw file copy — safe while writes occur
    finally:
        target.close(); source.close()
    return out


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
