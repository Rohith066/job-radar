"""Follow-up eligibility must respect the application queue.

Before this, `APPLIED -> NO_RESPONSE` left the job in `get_followup_due()`
forever: the queue recorded the outcome but wrote no feedback event, and the
legacy query only looked at feedback. Application status is now authoritative
when a row exists; historical jobs without one keep the original behaviour.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.database import Database
from src.apply.queue import (
    ApplicationQueue, NEW, SHORTLISTED, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED,
)

OLD = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
RECENT = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "t.db"))
    for k in ("j1", "j2", "j3", "j4", "j5", "j6", "legacy1", "legacy2"):
        d.mark_job_seen(key=k, source="gh", company="Acme", title="Data Engineer I",
                        location="Remote - US", url=f"u/{k}", posted="", score=90, label="yes")
    yield d
    d.close()


def _age_applied(db, key, when=OLD):
    """Backdate the 'applied' feedback event so the age threshold is crossed."""
    db._conn.execute("UPDATE feedback SET created_at=? WHERE job_key=? AND action='applied'",
                     (when, key))
    db._conn.commit()


def _due(db, days=7):
    return {r["job_key"] for r in db.get_followup_due(days=days)}


def _feedback(db, key):
    return [r[0] for r in db._conn.execute(
        "SELECT action FROM feedback WHERE job_key=? ORDER BY created_at", (key,))]


# ── Queue-backed jobs: status is authoritative ────────────────────────────
def test_applied_and_old_enough_is_due(db):
    ApplicationQueue(db).set_status("j1", APPLIED)
    _age_applied(db, "j1")
    assert "j1" in _due(db)


def test_applied_but_too_recent_is_not_due(db):
    ApplicationQueue(db).set_status("j2", APPLIED)
    _age_applied(db, "j2", RECENT)
    assert "j2" not in _due(db)


def test_no_response_is_never_due(db):
    """The defect this change fixes."""
    q = ApplicationQueue(db)
    q.set_status("j3", APPLIED)
    _age_applied(db, "j3")
    assert "j3" in _due(db), "precondition: it was due while still APPLIED"
    q.set_status("j3", NO_RESPONSE)
    assert "j3" not in _due(db)


def test_interview_is_never_due(db):
    q = ApplicationQueue(db)
    q.set_status("j4", APPLIED); _age_applied(db, "j4")
    q.set_status("j4", INTERVIEW)
    assert "j4" not in _due(db)


def test_rejected_is_never_due(db):
    q = ApplicationQueue(db)
    q.set_status("j5", APPLIED); _age_applied(db, "j5")
    q.set_status("j5", REJECTED)
    assert "j5" not in _due(db)


def test_shortlisted_is_never_due(db):
    ApplicationQueue(db).set_status("j6", SHORTLISTED)
    assert "j6" not in _due(db)


def test_skipped_is_never_due(db):
    q = ApplicationQueue(db)
    q.set_status("j1", APPLIED); _age_applied(db, "j1")
    q.set_status("j1", NEW, force=True)
    q.set_status("j1", SKIPPED)
    assert "j1" not in _due(db)


def test_new_is_never_due(db):
    q = ApplicationQueue(db)
    q.set_status("j2", APPLIED); _age_applied(db, "j2")
    q.set_status("j2", NEW, force=True)
    assert "j2" not in _due(db)


# ── Legacy jobs: no applications row, original behaviour preserved ────────
def test_legacy_applied_feedback_without_an_applications_row_is_still_due(db):
    db.record_feedback("legacy1", "applied")
    _age_applied(db, "legacy1")
    assert db._conn.execute(
        "SELECT COUNT(*) FROM applications WHERE job_key='legacy1'").fetchone()[0] == 0
    assert "legacy1" in _due(db), "pre-Phase-2 follow-up behaviour must be preserved"


def test_legacy_responded_event_still_suppresses_follow_up(db):
    db.record_feedback("legacy2", "applied")
    _age_applied(db, "legacy2")
    db.record_feedback("legacy2", "responded")
    assert "legacy2" not in _due(db)


def test_legacy_and_queue_jobs_coexist(db):
    db.record_feedback("legacy1", "applied"); _age_applied(db, "legacy1")
    q = ApplicationQueue(db)
    q.set_status("j1", APPLIED); _age_applied(db, "j1")
    q.set_status("j2", APPLIED); _age_applied(db, "j2"); q.set_status("j2", NO_RESPONSE)
    assert _due(db) == {"legacy1", "j1"}


# ── The fix must not touch ML semantics ───────────────────────────────────
def test_no_response_creates_no_feedback_event(db):
    q = ApplicationQueue(db)
    q.set_status("j3", APPLIED)
    before = _feedback(db, "j3")
    q.set_status("j3", NO_RESPONSE)
    assert _feedback(db, "j3") == before == ["applied"]


def test_follow_up_query_writes_nothing(db):
    q = ApplicationQueue(db)
    q.set_status("j4", APPLIED); _age_applied(db, "j4")
    before = db._conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
    db.get_followup_due(); db.get_followup_due()
    assert db._conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == before


def test_follow_up_does_not_alter_the_job_assessment(db):
    q = ApplicationQueue(db)
    q.set_status("j5", APPLIED); _age_applied(db, "j5")
    before = dict(db._conn.execute("SELECT * FROM jobs WHERE key='j5'").fetchone())
    q.set_status("j5", NO_RESPONSE)
    db.get_followup_due()
    after = dict(db._conn.execute("SELECT * FROM jobs WHERE key='j5'").fetchone())
    for f in ("score", "label", "priority", "opportunity_score", "resume_match"):
        assert before[f] == after[f]
