"""Posted-date parsing — one parser, shared by discovery and the queue.

Two defects this pins:
  * Workday's "Posted 30+ Days Ago" did not match `_RELATIVE_RE` (the "+"
    between number and unit broke it), so `_parse_posted` returned None and
    `_is_too_old` kept the posting as brand new. 364 CI rows carried it; a
    ~77-day-old NXP role ranked #5 APPLY_FIRST in the queue.
  * `src/apply/shortlist.py` had its own four-format ISO parser, so every
    Workday relative date showed "date unknown" in the queue and was re-scored
    without the freshness points discovery had given it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.database import Database
from src.main import _is_too_old, _parse_posted, screen_job
from src.apply.shortlist import _age_str, build_queue, evaluate_job
from src.screening import reasons as R
from src.sources.base import Job

TOL = timedelta(seconds=30)
JD = ("Requirements:\n- 0-2 years of professional experience\n- Python\n- SQL\n"
      "Preferred qualifications:\n- Kubernetes\n")
ROW = {"title": "Data Engineer I", "location": "Remote - US", "country_focus": "",
       "description": JD, "resume_match": 0}


# ── The parser ────────────────────────────────────────────────────────────
@pytest.mark.parametrize("posted, age", [
    ("Posted Today",        timedelta(0)),
    ("Posted Yesterday",    timedelta(days=1)),
    ("Posted 2 Days Ago",   timedelta(days=2)),
    ("Posted 30+ Days Ago", timedelta(days=30)),
])
def test_workday_relative_dates(posted, age):
    dt = _parse_posted(posted)
    assert dt is not None, f"{posted!r} must parse"
    assert abs((datetime.now(timezone.utc) - dt) - age) < TOL


def test_thirty_plus_days_is_too_old():
    """"30+" resolves to its floor of 30 days, already far outside the window."""
    assert _is_too_old("Posted 30+ Days Ago")


def test_long_month_with_double_space():
    """Boards pad single-digit days: "September  1, 2026"."""
    assert _parse_posted("September  1, 2026") == datetime(2026, 9, 1, tzinfo=timezone.utc)


def test_iso_with_offset_keeps_the_instant():
    assert _parse_posted("2026-09-08T14:30:00-04:00") == datetime(2026, 9, 8, 18, 30, tzinfo=timezone.utc)


def test_naive_iso_is_read_as_utc():
    """Remotive emits ISO without an offset; it used to fall through to None."""
    assert _parse_posted("2026-06-11T13:24:37") == datetime(2026, 6, 11, 13, 24, 37, tzinfo=timezone.utc)


def test_empty_is_undated_and_kept():
    """Undated postings are kept — the documented policy of `_is_too_old`."""
    assert _parse_posted("") is None
    assert _is_too_old("") is False


# ── The queue uses the same parser ────────────────────────────────────────
def test_queue_age_label_reads_workday_dates():
    assert _age_str("Posted Today") == "just posted"
    assert _age_str("Posted 2 Days Ago") == "posted 2d ago"
    assert _age_str("Posted 30+ Days Ago") == "posted 30d ago"
    assert _age_str("") == "date unknown"


@pytest.mark.parametrize("posted", ["Posted Today", "Posted Yesterday", "Posted 2 Days Ago"])
def test_queue_rescore_matches_discovery_score(posted):
    """The queue re-scores stored rows; for the same inputs it must reach the
    score discovery stored, freshness points included."""
    job = Job(key="k", source="workday", company="Co", title=ROW["title"],
              location=ROW["location"], url="http://x", posted=posted,
              score=0, label="yes", description=JD)
    stored = screen_job(job)
    requeued = evaluate_job({**ROW, "posted": posted}, set()).screening
    assert requeued.score == stored.score
    assert requeued.priority == stored.priority
    assert R.NO_POSTED_DATE not in requeued.warnings


def test_queue_awards_workday_freshness():
    e = evaluate_job({**ROW, "posted": "Posted Today"}, set())
    assert R.FRESH_LT_6H in e.screening.positive_reasons


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "disc.db"), user_state=str(tmp_path / "us.db"))
    yield d
    d.close()


def test_thirty_plus_day_workday_job_leaves_the_queue(db):
    """The NXP case: identical rows except the posted string."""
    for key, posted in (("stale", "Posted 30+ Days Ago"), ("fresh", "Posted Today")):
        db.mark_job_seen(key=key, source="workday", company=f"Co-{key}", title=ROW["title"],
                         location=ROW["location"], url=f"http://x/{key}", posted=posted,
                         score=95, label="yes", description=JD,
                         priority="APPLY_NOW", opportunity_score=95,
                         role_family="data_engineering", location_class="US_REMOTE")
    keys = {e.job["key"] for e in build_queue(db, limit=100, max_per_company=0)}
    assert "stale" not in keys
    assert "fresh" in keys
