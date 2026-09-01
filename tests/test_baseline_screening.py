"""Screening leaks found by the corrected queue, before Day 1 observation.

Two title families reached actionable bands that Phase 1 never intended:
clerical data-entry work (via the "data" technical hint plus an "associate"
entry marker), and robotics roles spelled "Robot"/"Robotic" rather than
"Robotics". Both are hard exclusions, not score adjustments, so they cannot be
recovered by downstream boosts.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.screening import reasons as R
from src.screening.titles import analyze_title
from src.screening import analyze_location, analyze_experience, score_job

NOW = datetime.now(timezone.utc)


def _band(title, hours_old=1):
    a = analyze_title(title)
    return a, score_job(title=a, location=analyze_location("Remote - US"),
                        experience=analyze_experience(""),
                        posted_at=NOW - timedelta(hours=hours_old))


# ── Clerical data entry is not a technical role ───────────────────────────
@pytest.mark.parametrize("title", [
    "Data Entry Associate",
    "Data Entry Clerk",
    "Data Entry Specialist",
    "Temporary Data Entry Associate",
    "Data Entry Associate, Temporary (Onsite)",
    "Entry Level Data Entry Clerk",
    "Virtual Data Entry Clerk",
    "Data Entry Associate, QA",
])
def test_data_entry_is_not_actionable(title):
    a, s = _band(title)
    assert a.classification == "NO", f"{title} -> {a.classification}"
    assert a.role_family == "unrelated"
    assert s.priority == "REJECT", f"{title} reached {s.priority}"


def test_data_entry_cannot_be_rescued_by_freshness_or_location():
    """A hard mismatch must not be recoverable through downstream boosts."""
    for hours in (0.1, 1, 24):
        _, s = _band("Data Entry Associate", hours_old=hours)
        assert s.application_priority_score if False else s.priority == "REJECT"


# ── Robotics: same exclusion, the way postings actually spell it ──────────
@pytest.mark.parametrize("title", [
    "Human-Robot Interaction Applied Scientist",
    "Human-Robot Interaction,  Applied Scientist , Fauna",
    "Robotics Research Scientist",
    "Robotic Systems Scientist",
    "Research Engineer, Robot Intelligence",
    "Researcher, Robot Intelligence",
    "Robotics Data Operator",
    "Machine Learning Engineer, Robotics",
])
def test_robotics_variants_are_profile_mismatch(title):
    a, s = _band(title)
    assert a.classification == "NO", f"{title} -> {a.classification}"
    assert R.PROFILE_MISMATCH_TITLE in a.reasons
    assert s.priority == "REJECT"


# ── Boundary controls: unrelated software titles must be unaffected ───────
@pytest.mark.parametrize("title", [
    "Software Engineer I", "Backend Engineer I", "Full Stack Engineer I",
    "New Grad Software Engineer", "Junior Software Engineer",
    "Data Engineer I", "Entry Level Data Engineer", "Associate Data Engineer",
    "Junior Data Analyst", "Associate Data Scientist", "Data Scientist I",
    "Machine Learning Engineer I", "AI Engineer I", "Analytics Engineer",
])
def test_legitimate_roles_remain_eligible(title):
    a, s = _band(title)
    assert a.classification in ("YES", "MAYBE"), f"{title} -> {a.classification}"
    assert s.priority != "REJECT"


@pytest.mark.parametrize("title", [
    "Data Engineer, Robotics Delivery Analytics",   # robotics domain -> excluded
])
def test_robotics_domain_excluded_even_on_a_target_family(title):
    assert analyze_title(title).classification == "NO"


@pytest.mark.parametrize("title", [
    "Robust Systems Engineer",       # 'robust' must not match 'robot'
    "Data Entrypoint Engineer",      # 'entrypoint' must not match 'data entry'
    "Database Engineer",
])
def test_similar_words_do_not_trigger_the_exclusions(title):
    """Phrase matching is boundary-anchored; near-miss words must be safe."""
    assert analyze_title(title).role_family not in ("unrelated", "profile_mismatch"), title


# ── Negative controls: the narrowed rules must not overreach ──────────────
@pytest.mark.parametrize("title", [
    "Data Capture Engineer",
    "Sensor Data Capture Engineer",
    "Data Processing Engineer",
    "Data Input Pipeline Engineer",
    "Data Ingestion Engineer",
    "Data Platform Engineer",
    "Database Engineer",
    "Associate Data Engineer",
    "Junior Data Analyst",
    "Associate Data Scientist",
])
def test_generic_data_words_are_not_clerical_exclusions(title):
    """The defect is clerical data-entry work, not any title containing
    generic words about capturing, inputting or processing data."""
    assert analyze_title(title).role_family != "unrelated", title


@pytest.mark.parametrize("title", [
    "Robotic Process Automation Engineer",
    "RPA Developer",
    "RPA Engineer",
    "Automation Engineer - RPA",
    "Business Process Automation Engineer",
    "Robust Systems Engineer",
])
def test_rpa_is_not_the_robotics_domain(title):
    """Robotic Process Automation is workflow tooling, not robotics research
    or hardware — it must not be caught by the profile-mismatch rule."""
    a = analyze_title(title)
    assert a.role_family != "profile_mismatch", title
    assert R.PROFILE_MISMATCH_TITLE not in a.reasons, title


@pytest.mark.parametrize("title", [
    "Robot Learning Researcher",
    "Robot Perception Engineer",
    "Robot Manipulation Scientist",
    "Autonomous Robot Software Engineer",
    "Human-Robot Interaction Researcher",
])
def test_robotics_morphological_variants_are_covered(title):
    a = analyze_title(title)
    assert a.classification == "NO", title
    assert R.PROFILE_MISMATCH_TITLE in a.reasons, title
