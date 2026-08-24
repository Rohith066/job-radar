"""Tests for structured JD requirement extraction.

Cases are drawn from formulations actually observed in the 200-job frozen
corpus, including the three failure modes that motivated replacing the
character-window heuristic.
"""
from __future__ import annotations

import pytest

from src.matching.jd_parser import (
    SECTION_ABOUT,
    SECTION_BENEFITS,
    SECTION_PREFERRED,
    SECTION_REQUIRED,
    SECTION_RESPONSIBILITIES,
    classify_heading,
    parse_jd,
    split_sections,
)


def kinds(jd: str) -> dict[str, str]:
    return {r.canonical: r.kind for r in parse_jd(jd).requirements}


# ── Heading classification ───────────────────────────────────────────────────

@pytest.mark.parametrize("heading,expected", [
    ("Requirements", SECTION_REQUIRED),
    ("Minimum Qualifications", SECTION_REQUIRED),
    ("Basic Qualifications", SECTION_REQUIRED),
    ("Qualifications", SECTION_REQUIRED),
    ("What You'll Bring", SECTION_REQUIRED),
    ("Preferred Qualifications", SECTION_PREFERRED),
    ("Nice to Have", SECTION_PREFERRED),
    ("Bonus Points", SECTION_PREFERRED),
    ("Responsibilities", SECTION_RESPONSIBILITIES),
    ("Key Responsibilities", SECTION_RESPONSIBILITIES),
    ("What You'll Do", SECTION_RESPONSIBILITIES),
    ("Benefits", SECTION_BENEFITS),
    ("About Us", SECTION_ABOUT),
])
def test_heading_classification(heading, expected):
    assert classify_heading(heading) == expected


def test_preferred_beats_qualifications():
    """'Preferred Qualifications' must not be swallowed by 'Qualifications'."""
    assert classify_heading("Preferred Qualifications") == SECTION_PREFERRED


def test_responsibilities_prefix_matches():
    """Regression: a trailing \\b blocked the 'responsibilit' prefix, so
    'Responsibilities' silently fell through to OTHER."""
    assert classify_heading("Responsibilities") == SECTION_RESPONSIBILITIES
    assert classify_heading("Responsibility") == SECTION_RESPONSIBILITIES


# ── Section attribution ──────────────────────────────────────────────────────

JD_SECTIONED = """
About Us: We are a great company building great things with Tableau.

Requirements:
- 5+ years of experience with Python and SQL
- Strong experience with Airflow

Preferred Qualifications:
- Familiarity with Kubernetes
- Exposure to Snowflake

Responsibilities:
- You will build data pipelines using dbt
"""


def test_skills_attributed_to_their_section():
    k = kinds(JD_SECTIONED)
    assert k["python"] == "required"
    assert k["sql"] == "required"
    assert k["airflow"] == "required"
    assert k["kubernetes"] == "preferred"
    assert k["snowflake"] == "preferred"


def test_responsibilities_are_not_requirements():
    """'You will build pipelines using dbt' is a duty, not a qualification."""
    assert kinds(JD_SECTIONED)["dbt"] == "responsibility"


def test_about_and_benefits_sections_are_ignored():
    """A technology named in About Us is incidental, not a requirement."""
    assert "tableau" not in kinds(JD_SECTIONED)


def test_strongest_classification_wins_across_sections():
    """A skill in both Requirements and Nice-to-Have is required."""
    jd = ("Requirements: Strong experience with Python. "
          "Nice to Have: Python scripting is a plus.")
    assert kinds(jd)["python"] == "required"


# ── Colon-less headings (regression) ─────────────────────────────────────────

def test_bare_heading_without_colon():
    """Amazon-style listings run 'Basic Qualifications' straight into bullets."""
    jd = ("Basic Qualifications - 3+ years of experience programming in Python "
          "- Experience with SQL")
    k = kinds(jd)
    assert k.get("python") == "required"
    assert k.get("sql") == "required"


def test_req_skills_and_requirements_heading():
    jd = ("Req Skills and Requirements Foundational understanding of "
          "machine learning and deep learning.")
    k = kinds(jd)
    assert k.get("machine_learning") == "required"


# ── Sentence-cue fallback (no usable structure) ──────────────────────────────

def test_fallback_detects_you_have_phrasing():
    """Regression: 'You have a strong background in X' was being missed."""
    jd = "You have a strong background in data modeling and distributed systems."
    p = parse_jd(jd)
    assert not p.used_sections
    assert {r.canonical: r.kind for r in p.requirements}["data_modeling"] == "required"


def test_fallback_detects_years_phrasing():
    jd = "We are looking for someone with 5+ years of Python development."
    assert kinds(jd)["python"] == "required"


def test_fallback_marks_soft_language_preferred():
    jd = "Familiarity with Kubernetes is a plus for this role."
    assert kinds(jd)["kubernetes"] == "preferred"


# ── Years extraction ─────────────────────────────────────────────────────────

def test_min_years_extracted():
    assert parse_jd("Requirements: 5+ years of Python experience.").min_years == 5


def test_min_years_none_when_absent():
    assert parse_jd("Requirements: Experience with Python.").min_years is None


# ── Robustness ───────────────────────────────────────────────────────────────

def test_empty_and_trivial_input():
    assert parse_jd("").requirements == []
    assert parse_jd("   ").requirements == []
    assert parse_jd("No known skills mentioned here at all.").requirements == []


def test_unrecognised_heading_does_not_fragment_section():
    """A prose fragment ending in a colon must not reset the section kind."""
    jd = ("Requirements: We need the following. "
          "Cross-functional collaboration: you will work with Python and SQL daily.")
    k = kinds(jd)
    assert k.get("python") == "required"


def test_parse_is_deterministic():
    a = [(r.canonical, r.kind) for r in parse_jd(JD_SECTIONED).requirements]
    b = [(r.canonical, r.kind) for r in parse_jd(JD_SECTIONED).requirements]
    assert a == b


def test_requirement_carries_evidence():
    reqs = {r.canonical: r for r in parse_jd(JD_SECTIONED).requirements}
    assert "Python" in reqs["python"].evidence or "python" in reqs["python"].evidence.lower()


def test_sections_have_non_empty_bodies():
    for sec in split_sections(JD_SECTIONED):
        assert sec.text.strip()
