"""Regression tests for the six Phase 1 corrections.

Each block pins one decision so the specific defect it fixes cannot return.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.screening import reasons as R
from src.screening.titles import analyze_title, ADJACENT_FAMILIES, PROFILE_MISMATCH
from src.screening.locations import analyze_location, US, US_REMOTE, NON_US, AMBIGUOUS
from src.screening.experience import analyze_experience
from src.screening.scoring import (
    score_job, APPLY_NOW, STRONG, REVIEW, LOW, REJECT, ADJACENT_CEILING, BAND_REVIEW,
)

NOW = datetime.now(timezone.utc)


def _score(title, location, description="", hours_old=2, country_focus=""):
    return score_job(
        title=analyze_title(title),
        location=analyze_location(location, country_focus),
        experience=analyze_experience(description),
        posted_at=NOW - timedelta(hours=hours_old),
    )


# ── Decision 1 — "management" is a domain word, not seniority ─────────────
@pytest.mark.parametrize("title", [
    "Data Engineer II, Data Management Team",
    "Full Stack Software Engineer - Alts & Data Management",
    "Data Engineer, Data Platform Management",
    "Software Development Engineer II - Data Management",
    "Data Management Technical Specialist (Entry Level)",
    "Data Scientist, Security Issue Management",
    "Product Engineer II - Data Management",
    "Associate Product Data Management Engineer",
    "Master Data Management Analyst",
])
def test_management_is_not_managerial(title):
    r = analyze_title(title)
    assert r.classification != "NO", f"{title} rejected as {r.seniority}"
    assert r.seniority != "manager"
    assert R.MANAGER_TITLE not in r.reasons


@pytest.mark.parametrize("title", [
    "Engineering Manager",
    "Data Engineering Manager",
    "Manager, Data Engineering",
    "Senior Manager, Machine Learning",
    "Software Engineering Manager",
    "Manager, Machine Learning Engineering (Fraud)",
    "Engineering Managers",
])
def test_actual_managers_still_reject(title):
    r = analyze_title(title)
    assert r.classification == "NO"
    assert r.seniority in ("manager", "senior", "executive", "director")


# ── Decision 2 — executive acronyms must name the candidate ───────────────
@pytest.mark.parametrize("title", [
    "Applied AI Engineer, Office of the CEO",
    "Staff AI Engineer, Agentic AI Application - Office of the CTO",
    "Data Engineer, Office of the CIO",
    "Data Analyst, Office of the Chief Data Officer",
    "Software Engineer reporting to the CTO",
])
def test_executive_office_is_not_an_executive_role(title):
    """An IC role inside an executive's org is not an executive posting."""
    r = analyze_title(title)
    assert r.seniority != "executive", f"{title} read as executive"
    assert R.EXECUTIVE_TITLE not in r.reasons


def test_office_of_the_ceo_ic_role_is_eligible():
    r = analyze_title("Applied AI Engineer, Office of the CEO")
    assert r.classification == "YES"
    assert r.role_family == "ml_ai"


@pytest.mark.parametrize("title", [
    "Chief Data Officer",
    "Chief Technology Officer",
    "CTO",
    "CEO",
    "Chief of Staff",
    "VP of Engineering",
    "SVP Marketing Data Science",
])
def test_actual_executives_still_reject(title):
    r = analyze_title(title)
    assert r.classification == "NO"
    assert r.seniority == "executive"


# ── Decision 3 — robotics / computer vision profile mismatch restored ─────
@pytest.mark.parametrize("title", [
    "Research Scientist, Robotics Research -  PhD New College Grad",
    "Robotics Engineer",
    "Computer Vision Engineer",
    "Computer Vision Scientist",
    "Machine Learning Engineer - Computer Vision",
    "New Grad Robotics Software Engineer",
    "Hardware Engineer",
    "Embedded Engineer",
    "Electrical Engineer",
    "Mechanical Engineer",
])
def test_profile_mismatch_never_alerts(title):
    r = analyze_title(title)
    assert r.classification == "NO", f"{title} was not excluded"
    assert R.PROFILE_MISMATCH_TITLE in r.reasons


def test_new_grad_wording_cannot_rescue_a_profile_mismatch():
    """The exact case that reached APPLY_NOW before this correction."""
    s = _score("Research Scientist, Robotics Research -  PhD New College Grad",
               "US, WA, Seattle", "", hours_old=1)
    assert s.priority == REJECT
    assert s.score == 0


# ── Decision 4 — Tier-3 adjacent occupations are review-only ──────────────
ADJACENT = [p for phrases in ADJACENT_FAMILIES.values() for p in phrases]

# "clinical data analyst" contains "data analyst", which sat in the higher tier
# pre-Phase-1 too (it classified yes/78, not 55). The tier ordering — target,
# then secondary, then adjacent — reproduces that shadowing exactly, so this
# phrase is verified against its real old behaviour rather than the Tier-3 list
# it also appears in.
SHADOWED_BY_HIGHER_TIER = {"clinical data analyst"}
ADJACENT_UNSHADOWED = [p for p in ADJACENT if p not in SHADOWED_BY_HIGHER_TIER]


@pytest.mark.parametrize("phrase", ADJACENT_UNSHADOWED)
def test_adjacent_occupations_are_review_only(phrase):
    r = analyze_title(phrase.title())
    assert r.classification == "MAYBE", f"{phrase} is not review-only"
    assert r.role_family in ADJACENT_FAMILIES


def test_clinical_data_analyst_keeps_its_pre_phase1_tier():
    """Verified against the pre-Phase-1 classifier: it scored yes/78 because
    'data analyst' matched the higher tier first, not 55 as a Tier-3 role."""
    r = analyze_title("Clinical Data Analyst")
    assert r.role_family == "data_analytics"


@pytest.mark.parametrize("title", [
    "Quantitative Analyst", "Quant Analyst", "Research Scientist",
    "Research Engineer", "Decision Scientist", "Forecasting Analyst",
    "AI Scientist", "Operations Research Analyst",
    "Statistical Analyst", "Business Analyst", "AI Analyst",
])
def test_restored_tier3_roles_cannot_exceed_review(title):
    """Pre-Phase-1 these scored 55/'maybe'. Best case is REVIEW, never STRONG."""
    s = _score(title, "Remote - US", "0-2 years of professional experience", hours_old=0.5)
    assert s.priority in (REVIEW, LOW), f"{title} reached {s.priority}"
    assert s.score <= ADJACENT_CEILING


def test_quant_associate_no_longer_reaches_apply_now():
    s = _score("Quantitative Analyst Associate (2027)", "Philadelphia, PA", "", hours_old=2)
    assert s.priority == REVIEW
    assert s.score <= ADJACENT_CEILING


def test_entry_marker_does_not_promote_an_adjacent_occupation():
    for t in ("New Grad Research Scientist", "Junior Quantitative Analyst"):
        assert analyze_title(t).classification == "MAYBE"
        assert _score(t, "Remote - US", "", hours_old=1).priority in (REVIEW, LOW)


def test_first_class_families_are_unaffected_by_the_adjacent_cap():
    for t in ("New Grad Software Engineer", "Data Engineer I", "Backend Engineer I"):
        s = _score(t, "Remote - US", "0-2 years of professional experience", hours_old=1)
        assert s.priority == APPLY_NOW


# ── Decision 5 — recall floor for bare-Remote target roles ────────────────
@pytest.mark.parametrize("title", [
    "Data Engineer", "Software Engineer", "Backend Engineer",
    "Full Stack Engineer", "Machine Learning Engineer", "Data Scientist",
])
@pytest.mark.parametrize("description", [
    "", "3+ years of engineering experience", "4+ years of professional experience",
    "0-2 years of professional experience",
])
def test_bare_remote_target_roles_reach_at_least_review(title, description):
    s = _score(title, "Remote", description, hours_old=200)
    assert s.priority in (REVIEW, STRONG, APPLY_NOW), f"{title}/{description} -> {s.priority}"
    assert s.score >= BAND_REVIEW


def test_recall_floor_keeps_location_ambiguous():
    """Protecting recall must not weaken geographic precision."""
    loc = analyze_location("Remote", "")
    assert loc.classification == AMBIGUOUS
    s = _score("Data Engineer", "Remote", "", hours_old=200)
    assert R.TARGET_ROLE_RECALL_FLOOR in s.warnings
    assert R.US_REMOTE_CONFIRMED not in s.positive_reasons
    assert R.US_CONFIRMED not in s.positive_reasons


def test_recall_floor_does_not_promote_beyond_review():
    s = _score("Data Engineer", "Remote", "", hours_old=200)
    assert s.priority == REVIEW
    assert s.score == BAND_REVIEW


def test_recall_floor_does_not_rescue_over_experienced_roles():
    for desc in ("7-10 years of professional experience", "8+ years of industry experience"):
        assert _score("Data Engineer", "Remote", desc).priority == REJECT


def test_recall_floor_does_not_rescue_senior_or_mismatch_titles():
    for t in ("Senior Data Engineer", "Data Engineering Manager", "Robotics Engineer"):
        assert _score(t, "Remote", "0-2 years of experience").priority == REJECT


def test_recall_floor_does_not_apply_to_non_target_families():
    assert _score("Data Analyst", "Remote", "", hours_old=200).priority == LOW


# ── Decision 6 — obvious foreign remote ───────────────────────────────────
@pytest.mark.parametrize("location", [
    "Remote (IND)", "Remote (DEU)", "Deutschland, remote", "Remote Estonia",
    "Remote - Canada", "Remote Europe", "Remote  (DEU)", "Remote (GBR)",
    "Florianópolis/ Remote", "Remote Estonia ", "Remote (BRA)",
])
def test_foreign_remote_is_non_us(location):
    assert analyze_location(location).classification == NON_US


@pytest.mark.parametrize("location", [
    "Remote - US", "US Remote", "Remote, USA", "Remote (United States)",
    "Remote (U.S.)", "Remote (US)", "United States - Remote",
])
def test_us_remote_survives_the_foreign_remote_rules(location):
    assert analyze_location(location).classification == US_REMOTE


@pytest.mark.parametrize("location", [
    "Seattle, WA, US", "US, WA, Seattle", "Austin, TX", "Dublin, OH",
    "Milwaukee, WI", "Indianapolis, IN",
])
def test_us_locations_are_unaffected(location):
    assert analyze_location(location).classification == US


def test_bare_remote_is_still_ambiguous_not_foreign():
    assert analyze_location("Remote").classification == AMBIGUOUS
