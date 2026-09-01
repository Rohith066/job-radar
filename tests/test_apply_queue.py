"""Application-queue tests — migration, transitions, suppression, metrics."""
from __future__ import annotations

import sqlite3

import pytest

from src.database import Database
from src.apply.queue import (
    ApplicationQueue, IllegalTransition,
    NEW, SHORTLISTED, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED,
    STATUSES, TERMINAL_STATUSES,
)

LEGACY_SCHEMA = """
CREATE TABLE jobs (
    key TEXT PRIMARY KEY, source TEXT NOT NULL, company TEXT NOT NULL,
    title TEXT NOT NULL, location TEXT NOT NULL DEFAULT '', url TEXT NOT NULL DEFAULT '',
    posted TEXT NOT NULL DEFAULT '', score INTEGER NOT NULL DEFAULT 0,
    label TEXT NOT NULL DEFAULT 'no', work_type TEXT NOT NULL DEFAULT '',
    salary TEXT NOT NULL DEFAULT '', resume_match INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '', first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
);
CREATE TABLE boards (board_id TEXT PRIMARY KEY, platform TEXT NOT NULL,
    company TEXT NOT NULL DEFAULT '', url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active', last_checked TEXT,
    job_count INTEGER NOT NULL DEFAULT 0, fail_count INTEGER NOT NULL DEFAULT 0,
    fail_reason TEXT NOT NULL DEFAULT '', first_seen TEXT NOT NULL, last_seen TEXT NOT NULL);
CREATE TABLE cursors (name TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '0');
CREATE TABLE feedback (job_key TEXT NOT NULL, action TEXT NOT NULL,
    created_at TEXT NOT NULL, notes TEXT NOT NULL DEFAULT '');
"""


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "t.db"
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_SCHEMA)
    conn.execute("INSERT INTO jobs(key,source,company,title,first_seen,last_seen) "
                 "VALUES('k1','greenhouse','Acme','Data Engineer I','2026-08-01','2026-08-01')")
    conn.execute("INSERT INTO jobs(key,source,company,title,first_seen,last_seen) "
                 "VALUES('k2','greenhouse','Beta','Software Engineer I','2026-08-01','2026-08-01')")
    conn.execute("INSERT INTO feedback VALUES('k1','applied','2026-08-01','')")
    conn.execute("INSERT INTO cursors VALUES('boards_main','120')")
    conn.commit(); conn.close()
    d = Database(str(path))
    yield d
    d.close()


# ── Migration ─────────────────────────────────────────────────────────────
def test_applications_table_is_created_on_a_legacy_db(db):
    cols = {r[1] for r in db._conn.execute("PRAGMA table_info(applications)")}
    for c in ("job_key", "status", "priority_at_decision", "fit_at_decision",
              "screening_at_decision", "note", "shortlisted_at", "applied_at",
              "outcome_at", "created_at", "updated_at"):
        assert c in cols


def test_existing_data_survives(db):
    assert db._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2
    assert db._conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == 1
    assert db.get_cursor("boards_main") == 120


def test_migration_is_idempotent(tmp_path, db):
    path = db.path
    db.close()
    d2 = Database(path); d2.close()
    d3 = Database(path)
    assert d3._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2
    d3.close()


# ── Transitions ───────────────────────────────────────────────────────────
def test_default_status_is_new(db):
    assert ApplicationQueue(db).status_of("k1") == NEW


def test_happy_path(db):
    q = ApplicationQueue(db)
    q.set_status("k1", SHORTLISTED, priority=91, fit=84, screening=95)
    a = q.set_status("k1", APPLIED)
    assert a.status == APPLIED
    assert a.shortlisted_at and a.applied_at
    assert (a.priority_at_decision, a.fit_at_decision, a.screening_at_decision) == (91, 84, 95)


def test_applied_cannot_silently_revert_to_new(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED)
    with pytest.raises(IllegalTransition):
        q.set_status("k1", NEW)


def test_rejected_is_terminal(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED); q.set_status("k1", REJECTED)
    with pytest.raises(IllegalTransition):
        q.set_status("k1", INTERVIEW)


def test_force_overrides_for_corrections(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED)
    assert q.set_status("k1", NEW, force=True).status == NEW


def test_invalid_status_rejected(db):
    with pytest.raises(ValueError):
        ApplicationQueue(db).set_status("k1", "BOGUS")


def test_snapshot_is_not_overwritten_by_later_transitions(db):
    q = ApplicationQueue(db)
    q.set_status("k1", SHORTLISTED, priority=91, fit=84, screening=95)
    q.set_status("k1", APPLIED, priority=10, fit=10, screening=10)
    a = q.get("k1")
    assert (a.priority_at_decision, a.fit_at_decision) == (91, 84)


# ── Feedback interop ──────────────────────────────────────────────────────
def test_status_change_also_writes_the_feedback_event(db):
    """The preference model must still see the event — now via user state."""
    ApplicationQueue(db).set_status("k2", APPLIED)
    actions = [r[0] for r in db._conn.execute(
        f"SELECT action FROM ({db._feedback_union_sql()}) WHERE job_key='k2'")]
    assert "applied" in actions
    assert db._conn.execute(
        "SELECT COUNT(*) FROM main.feedback WHERE job_key='k2'").fetchone()[0] == 0, \
        "a user action must not write to the discovery database"


# ── Shortlist suppression ─────────────────────────────────────────────────
def test_acted_on_jobs_are_excluded(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED)
    q.set_status("k2", SKIPPED)
    assert q.excluded_keys() == {"k1", "k2"}


def test_shortlisted_jobs_still_appear(db):
    q = ApplicationQueue(db)
    q.set_status("k1", SHORTLISTED)
    assert "k1" not in q.excluded_keys(), "shortlisting is not acting on a job"


def test_bulk_status_lookup(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED)
    assert q.statuses_for(["k1", "k2"]) == {"k1": APPLIED}


# ── Metrics ───────────────────────────────────────────────────────────────
def test_metrics_shape(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    q.set_status("k1", INTERVIEW)
    q.set_status("k2", APPLIED, priority=70, fit=60, screening=72)
    m = q.metrics()
    assert m["applications_total"] == 2
    assert m["interviews"] == 1
    assert m["interview_rate"] == 0.5
    assert m["avg_priority_at_decision"] is not None
    assert m["by_priority_band"]


def test_metrics_empty_is_safe(db):
    m = ApplicationQueue(db).metrics()
    assert m["applications_total"] == 0
    assert m["interview_rate"] is None


def test_terminal_statuses_declared_consistently():
    for s in TERMINAL_STATUSES:
        assert s in STATUSES
