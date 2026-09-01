"""The actionable queue must use the same age policy as Phase 1 alerting.

Before this, `build_queue` ranked the entire stored job history while
`_dispatch_results` enforced MAX_JOB_AGE_DAYS, so the application interface and
the alert stream described different eligibility populations — 7 of the 15
default queue slots held postings Phase 1 would never have surfaced.

These tests pin consistency, not the policy value: they derive expectations
from MAX_JOB_AGE_DAYS rather than hard-coding 3.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.database import Database
from src.main import MAX_JOB_AGE_DAYS, _is_too_old
from src.apply.queue import ApplicationQueue, APPLIED, INTERVIEW
from src.apply.shortlist import build_queue

NOW = datetime.now(timezone.utc)
JD = ("Requirements:\n- 0-2 years of professional experience\n- Python\n- SQL\n"
      "Preferred qualifications:\n- Kubernetes\n")


def _posted(days=None, hours=None):
    if days is None and hours is None:
        return ""                                   # undated
    delta = timedelta(days=days or 0, hours=hours or 0)
    return (NOW - delta).strftime("%Y-%m-%dT%H:%M:%S+0000")


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "disc.db"), user_state=str(tmp_path / "us.db"))
    yield d
    d.close()


def _add(db, key, title, posted, *, jd=JD, location="Remote - US"):
    db.mark_job_seen(key=key, source="gh", company=f"Co-{key}", title=title,
                     location=location, url=f"http://x/{key}", posted=posted,
                     score=95, label="yes", description=jd,
                     priority="APPLY_NOW", opportunity_score=95,
                     role_family="data_engineering", location_class="US_REMOTE")


def _keys(db, **kw):
    return {e.job["key"] for e in build_queue(db, limit=100, max_per_company=0, **kw)}


# ── Eligibility boundary ──────────────────────────────────────────────────
def test_one_day_old_job_appears(db):
    _add(db, "fresh", "Data Engineer I", _posted(days=1))
    assert "fresh" in _keys(db)


def test_job_at_the_policy_boundary_appears(db):
    """Exactly at MAX_JOB_AGE_DAYS, minus an hour — still inside."""
    _add(db, "edge", "Data Engineer I", _posted(days=MAX_JOB_AGE_DAYS, hours=-1))
    assert not _is_too_old(_posted(days=MAX_JOB_AGE_DAYS, hours=-1))
    assert "edge" in _keys(db)


def test_job_just_past_the_boundary_is_excluded(db):
    """One hour past the policy — outside, by the production calculation."""
    p = _posted(days=MAX_JOB_AGE_DAYS, hours=1)
    _add(db, "past", "Data Engineer I", p)
    assert _is_too_old(p)
    assert "past" not in _keys(db)


def test_four_day_old_apply_first_is_excluded(db):
    _add(db, "d4", "New Grad Data Engineer", _posted(days=4))
    assert "d4" not in _keys(db)


def test_seventeen_day_old_top_scoring_job_is_excluded(db):
    _add(db, "d17", "Junior Data Engineer", _posted(days=17))
    assert "d17" not in _keys(db)


def test_undated_jobs_follow_the_existing_policy(db):
    """`_is_too_old` keeps undated postings; the queue must do the same, not
    invent a stricter rule."""
    _add(db, "undated", "Data Engineer I", "")
    assert not _is_too_old("")
    assert ("undated" in _keys(db)) is True


def test_fresh_jobs_are_no_longer_crowded_out(db):
    for i in range(20):
        _add(db, f"old{i}", "Junior Data Engineer", _posted(days=10 + i))
    _add(db, "fresh", "Data Engineer I", _posted(hours=2))
    top = build_queue(db, limit=5, max_per_company=0)
    assert "fresh" in {e.job["key"] for e in top}
    assert all(not _is_too_old(e.job["posted"]) for e in top)


def test_shadow_mode_can_still_see_old_jobs(db):
    """The age experiment needs an explicit opt-out; the default must not use it."""
    _add(db, "d17", "Junior Data Engineer", _posted(days=17))
    assert "d17" not in _keys(db)
    assert "d17" in _keys(db, enforce_age=False)


# ── Historical records must be unaffected ─────────────────────────────────
def test_old_applied_job_remains_in_metrics(db):
    _add(db, "old", "Data Engineer I", _posted(days=30))
    q = ApplicationQueue(db)
    q.set_status("old", APPLIED, priority=91, fit=84, screening=95)
    assert "old" not in _keys(db)                     # not actionable any more
    assert q.metrics()["applications_total"] == 1     # but still counted
    assert q.get("old").status == APPLIED


def test_old_applied_job_remains_transitionable(db):
    _add(db, "old", "Data Engineer I", _posted(days=30))
    q = ApplicationQueue(db)
    q.set_status("old", APPLIED, priority=91, fit=84, screening=95)
    q.set_status("old", INTERVIEW)
    assert q.get("old").status == INTERVIEW
    assert q.metrics()["interviews"] == 1


def test_old_applied_job_visible_to_observation(db):
    from src.apply.observe import snapshot
    _add(db, "old", "Data Engineer I", _posted(days=45))
    ApplicationQueue(db).set_status("old", APPLIED, priority=91, fit=84, screening=95)
    snap = snapshot(db)
    assert snap["funnel"]["applied"] == 1, "age filtering must not hide application history"


def test_age_policy_constant_is_unchanged():
    assert MAX_JOB_AGE_DAYS == 3, "the observation baseline freezes the policy at 3 days"
