"""Opportunity-scoring regression tests.

The load-bearing property is requirement 16: no accumulation of freshness,
location or role-family points may lift a clearly senior or managerial role
into an alert.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.screening import reasons as R
from src.screening.titles import analyze_title
from src.screening.locations import analyze_location
from src.screening.experience import analyze_experience
from src.screening.scoring import (
    score_job, APPLY_NOW, STRONG, REVIEW, LOW, REJECT,
    BAND_APPLY_NOW, BAND_STRONG, BAND_REVIEW,
)

NOW = datetime.now(timezone.utc)


def _score(title, location, description="", hours_old=1.0, country_focus=""):
    return score_job(
        title=analyze_title(title),
        location=analyze_location(location, country_focus),
        experience=analyze_experience(description),
        posted_at=NOW - timedelta(hours=hours_old) if hours_old is not None else None,
    )


# ── The four worked examples from the specification ───────────────────────
def test_excellent_job_is_apply_now():
    r = _score("New Grad Software Engineer", "Remote - US",
               "0-2 years of software engineering experience", hours_old=2)
    assert r.priority == APPLY_NOW
    assert r.score >= BAND_APPLY_NOW
    assert R.NEW_GRAD_EXPLICIT in r.positive_reasons
    assert R.US_REMOTE_CONFIRMED in r.positive_reasons


def test_good_but_imperfect_job_is_not_rejected():
    r = _score("Software Engineer", "New York, NY",
               "2-3 years of relevant experience", hours_old=5)
    assert r.priority in (STRONG, REVIEW)
    assert r.priority != REJECT


def test_obvious_senior_role_is_rejected_despite_freshness():
    r = _score("Senior Software Engineer", "United States",
               "5+ years of software engineering experience", hours_old=1)
    assert r.priority == REJECT
    assert r.score == 0


def test_manager_role_is_rejected():
    r = _score("Software Engineering Manager", "US Remote", hours_old=0.5)
    assert r.priority == REJECT


def test_ambiguous_remote_is_not_confirmed_us():
    r = score_job(
        title=analyze_title("Software Engineer I"),
        location=analyze_location("Remote", country_focus=""),
        experience=analyze_experience(""),
        posted_at=NOW - timedelta(hours=10),
    )
    assert r.priority != REJECT
    assert R.US_REMOTE_CONFIRMED not in r.positive_reasons
    assert R.REMOTE_UNSCOPED in r.warnings


# ── Requirement 16: freshness can never rescue a rejected role ────────────
@pytest.mark.parametrize("title", [
    "Senior Software Engineer",
    "Staff Data Engineer",
    "Principal Machine Learning Engineer",
    "Software Engineering Manager",
    "Director of Data Engineering",
    "VP of Engineering",
    "Software Engineer IV",
    "Data Engineer V",
])
def test_no_amount_of_freshness_rescues_a_senior_role(title):
    """Best possible everything else: US Remote, posted minutes ago."""
    r = _score(title, "Remote - US", "0-2 years of experience", hours_old=0.1)
    assert r.priority == REJECT, f"{title} escaped rejection with {r.score}"
    assert r.score == 0


def test_seven_plus_years_is_rejected():
    r = _score("Software Engineer", "Remote - US",
               "7-10 years of professional experience", hours_old=0.5)
    assert r.priority == REJECT


def test_confirmed_non_us_is_rejected():
    r = _score("New Grad Software Engineer", "Toronto, Canada",
               "0-2 years of experience", hours_old=0.1)
    assert r.priority == REJECT


# ── 2-3 year roles stay eligible ──────────────────────────────────────────
@pytest.mark.parametrize("description", [
    "2 years of software development experience",
    "2-3 years of relevant experience",
    "3+ years of engineering experience",
    "4+ years of professional experience",
])
def test_two_to_four_year_roles_remain_eligible(description):
    r = _score("Software Engineer", "Austin, Texas", description, hours_old=3)
    assert r.priority != REJECT
    assert r.priority != LOW


def test_five_plus_years_is_a_strong_negative_but_scored():
    r = _score("Software Engineer", "Austin, Texas",
               "5+ years of software engineering experience", hours_old=1)
    assert r.priority in (LOW, REVIEW)
    assert R.EXPERIENCE_5_PLUS in r.negative_reasons or R.EXPERIENCE_5_PLUS in r.warnings


# ── Freshness ─────────────────────────────────────────────────────────────
def test_freshness_ordering():
    a = _score("Software Engineer I", "Austin, TX", hours_old=2)
    b = _score("Software Engineer I", "Austin, TX", hours_old=20)
    c = _score("Software Engineer I", "Austin, TX", hours_old=60)
    assert a.score >= b.score >= c.score
    assert R.FRESH_LT_6H in a.positive_reasons
    assert R.FRESH_LT_24H in b.positive_reasons
    assert R.FRESH_LT_3D in c.positive_reasons


def test_missing_posted_date_earns_no_freshness_bonus():
    """Discovery time is not a posting time and must not be treated as one."""
    dated = _score("Software Engineer I", "Austin, TX", hours_old=1)
    undated = _score("Software Engineer I", "Austin, TX", hours_old=None)
    assert undated.score < dated.score
    assert R.NO_POSTED_DATE in undated.warnings


# ── Bands ─────────────────────────────────────────────────────────────────
def test_band_boundaries_are_contiguous():
    assert BAND_APPLY_NOW > BAND_STRONG > BAND_REVIEW


def test_scores_stay_in_range():
    for title in ("New Grad Software Engineer", "Software Engineer", "Data Analyst"):
        for loc in ("Remote - US", "Remote", "Austin, TX"):
            r = _score(title, loc, "0-2 years of experience", hours_old=0.1)
            assert 0 <= r.score <= 100


# ── Explainability ────────────────────────────────────────────────────────
def test_every_score_has_at_least_one_reason():
    """The system must never emit a score with no supporting reasons."""
    cases = [
        ("New Grad Software Engineer", "Remote - US", "0-2 years of experience"),
        ("Software Engineer", "Remote", ""),
        ("Senior Software Engineer", "Austin, TX", "5+ years of experience"),
        ("Software Engineering Manager", "US Remote", ""),
        ("Data Engineer II", "Milwaukee, WI", "2-3 years of relevant experience"),
    ]
    for title, loc, desc in cases:
        r = _score(title, loc, desc)
        all_reasons = r.positive_reasons + r.negative_reasons + r.warnings
        assert all_reasons, f"{title} produced a score with no reasons"
        assert r.reason_codes, f"{title} produced no reason codes"


def test_scoring_is_deterministic():
    a = _score("Software Engineer I", "Remote - US", "1-2 years of experience", hours_old=3)
    b = _score("Software Engineer I", "Remote - US", "1-2 years of experience", hours_old=3)
    assert a == b


def test_unrelated_engineering_roles_do_not_score():
    for title in ("Sales Engineer", "Mechanical Engineer", "Civil Engineer",
                  "Solutions Engineer"):
        assert _score(title, "Austin, TX", "0-2 years of experience").priority == REJECT
