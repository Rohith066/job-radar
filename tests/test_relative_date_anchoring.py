"""Stored relative dates age from when they were observed, not from now.

"Posted Today" is true on the day a scraper reads it. Once stored, it is a
claim about `last_seen` — the moment `Database.mark_job_seen` wrote it, in the
same upsert as the string itself. Re-read against the current clock it stayed
"today" forever: on 2026-09-11, 37 queue rows said "Posted Today" although
their strings had been observed on 2026-09-02 or earlier, and three of them
(Citigroup, Boeing x2) sat in the top 15 as APPLY_FIRST 100.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.apply.eligibility import FRESH, STALE_JOB, live_eligibility
from src.apply.shortlist import _age_str, build_queue, evaluate_job
from src.database import Database
from src.main import _is_too_old, _parse_posted, screen_job
from src.screening import reasons as R
from src.sources.base import Job

NOW = datetime.now(timezone.utc)
TOL = timedelta(seconds=30)
JD = ("Requirements:\n- 0-2 years of professional experience\n- Python\n- SQL\n"
      "Preferred qualifications:\n- Kubernetes\n")
ROW = {"title": "Data Engineer I", "location": "Remote - US", "country_focus": "",
       "description": JD, "resume_match": 0}


def days_ago(n: float) -> datetime:
    return NOW - timedelta(days=n)


# ── The parser, anchored ───────────────────────────────────────────────────
def test_posted_today_stored_seven_days_ago_is_stale_today():
    assert _is_too_old("Posted Today", observed_at=days_ago(7))
    assert abs((NOW - _parse_posted("Posted Today", days_ago(7))) - timedelta(days=7)) < TOL


def test_posted_yesterday_stored_five_days_ago_is_stale_today():
    assert _is_too_old("Posted Yesterday", observed_at=days_ago(5))
    age = NOW - _parse_posted("Posted Yesterday", days_ago(5))
    assert abs(age - timedelta(days=6)) < TOL


@pytest.mark.parametrize("observed, expected_age, stale", [
    (0, 2, False),      # just observed: two days old, inside the window
    (2, 4, True),       # observed two days ago: four days old now
    (5, 7, True),
])
def test_posted_two_days_ago_ages_from_observation(observed, expected_age, stale):
    dt = _parse_posted("Posted 2 Days Ago", days_ago(observed))
    assert abs((NOW - dt) - timedelta(days=expected_age)) < TOL
    assert _is_too_old("Posted 2 Days Ago", observed_at=days_ago(observed)) is stale


@pytest.mark.parametrize("observed", [0, 1, 10])
def test_thirty_plus_days_ago_remains_stale(observed):
    assert _is_too_old("Posted 30+ Days Ago", observed_at=days_ago(observed))


@pytest.mark.parametrize("posted", [
    "2026-09-08T14:30:00-04:00", "2026-09-01", "September  1, 2026", "2026-06-11T13:24:37",
])
def test_absolute_dates_ignore_the_observation_time(posted):
    assert _parse_posted(posted, days_ago(9)) == _parse_posted(posted)
    assert _is_too_old(posted, observed_at=days_ago(9)) == _is_too_old(posted)


def test_undated_policy_is_unchanged():
    assert _parse_posted("", days_ago(9)) is None
    assert _is_too_old("", observed_at=days_ago(9)) is False
    assert _is_too_old("") is False


def test_the_stored_iso_timestamp_is_accepted_as_the_anchor():
    """last_seen comes out of SQLite as an ISO string with an offset."""
    assert _is_too_old("Posted Today", observed_at=days_ago(7).isoformat())
    assert not _is_too_old("Posted Today", observed_at=NOW.isoformat())


def test_discovery_semantics_are_unchanged_without_an_anchor():
    assert abs(NOW - _parse_posted("Posted Today")) < TOL
    assert not _is_too_old("Posted Today")


# ── Queue, eligibility and discovery agree ─────────────────────────────────
def _job(posted: str) -> Job:
    return Job(key="k", source="workday", company="Co", title=ROW["title"],
               location=ROW["location"], url="http://x", posted=posted,
               score=0, label="yes", description=JD)


@pytest.mark.parametrize("posted", ["Posted Today", "Posted Yesterday", "Posted 2 Days Ago"])
def test_queue_scores_a_just_observed_row_exactly_as_discovery_did(posted):
    discovered = screen_job(_job(posted))
    requeued = evaluate_job({**ROW, "posted": posted, "last_seen": NOW.isoformat()},
                            set()).screening
    assert (requeued.score, requeued.priority) == (discovered.score, discovered.priority)


def test_queue_stops_awarding_freshness_to_an_old_observation():
    # A row that does not saturate the 0-100 clamp — ROW scores ~124 before
    # clamping, so both variants would read 100 and hide the difference.
    unsaturated = {**ROW, "title": "Data Engineer", "location": "Remote"}
    fresh = evaluate_job({**unsaturated, "posted": "Posted Today",
                          "last_seen": NOW.isoformat()}, set()).screening
    old = evaluate_job({**unsaturated, "posted": "Posted Today",
                        "last_seen": days_ago(9).isoformat()}, set()).screening
    assert fresh.score < 100
    assert old.score < fresh.score
    assert R.STALE_POSTING in old.negative_reasons
    assert R.FRESH_LT_6H not in old.positive_reasons


def test_live_eligibility_uses_the_same_anchor():
    assert live_eligibility("Posted Today", days_ago(7)).status == STALE_JOB
    assert live_eligibility("Posted Today", days_ago(7)).age_days == 7
    assert live_eligibility("Posted Today", NOW).status == FRESH


def test_age_label_counts_from_observation():
    assert _age_str("Posted Today", days_ago(7)) == "posted 7d ago"
    assert _age_str("Posted Today", NOW) == "just posted"


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "disc.db"), user_state=str(tmp_path / "us.db"))
    yield d
    d.close()


def _seed(db, key: str, posted: str, observed: datetime) -> None:
    db.mark_job_seen(key=key, source="workday", company=f"Co-{key}", title=ROW["title"],
                     location=ROW["location"], url=f"http://x/{key}", posted=posted,
                     score=95, label="yes", description=JD, priority="APPLY_NOW",
                     opportunity_score=95, role_family="data_engineering",
                     location_class="US_REMOTE")
    # mark_job_seen stamps last_seen = now; set the observation time directly.
    with db._tx() as conn:
        conn.execute("UPDATE jobs SET last_seen=? WHERE key=?", (observed.isoformat(), key))


def test_the_queue_drops_relative_rows_observed_long_ago(db):
    _seed(db, "today-observed-9d-ago", "Posted Today", days_ago(9))   # the Citigroup / Boeing case
    _seed(db, "yesterday-observed-5d-ago", "Posted Yesterday", days_ago(5))
    _seed(db, "today-observed-now", "Posted Today", NOW)
    _seed(db, "two-days-observed-now", "Posted 2 Days Ago", NOW)
    keys = {e.job["key"] for e in build_queue(db, limit=100, max_per_company=0)}
    assert {"today-observed-now", "two-days-observed-now"} <= keys
    assert not keys & {"today-observed-9d-ago", "yesterday-observed-5d-ago"}
