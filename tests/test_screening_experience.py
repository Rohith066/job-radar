"""Experience-extraction regression tests.

The previous extractor made the "experience" context word optional, so it read
a requirement out of "founded 20 years ago". Those traps are asserted here.
"""
from __future__ import annotations

import pytest

from src.screening import reasons as R
from src.screening.experience import analyze_experience


# ── The prose examples from the specification ─────────────────────────────
@pytest.mark.parametrize("text,min_years", [
    ("0-2 years of software engineering experience",   0),
    ("1+ years of professional experience",            1),
    ("2 years of software development experience",     2),
    ("2-3 years of relevant experience",               2),
    ("3+ years of engineering experience",             3),
    ("4+ years of professional experience",            4),
    ("5+ years of software engineering experience",    5),
    ("7-10 years of experience",                       7),
])
def test_stated_requirements(text, min_years):
    assert analyze_experience(text).min_years == min_years


@pytest.mark.parametrize("text,lo,hi", [
    ("0-2 years of software engineering experience", 0, 2),
    ("1-2 years of professional experience",         1, 2),
    ("2-3 years of relevant experience",             2, 3),
    ("3-5 years of industry experience",             3, 5),
    ("7-10 years of experience",                     7, 10),
])
def test_ranges(text, lo, hi):
    r = analyze_experience(text)
    assert (r.min_years, r.max_years) == (lo, hi)


# ── False-positive traps: numbers that are not experience requirements ────
@pytest.mark.parametrize("text", [
    "Our company was founded 20 years ago.",
    "The firm has been in business for 15 years.",
    "This is a 2 year contract with option to renew.",
    "You must complete your degree within 4 years.",
    "Benefits kick in after 1 year of service.",
    "Equity vests over 4 years with a 1 year cliff.",
    "Celebrating our 10 year anniversary!",
    "We have grown 5x over the past 3 years.",
])
def test_traps_do_not_yield_a_requirement(text):
    r = analyze_experience(text)
    assert r.min_years is None, f"{text!r} -> {r.min_years} ({r.raw_matches})"


def test_trap_next_to_a_real_requirement_still_extracts_the_requirement():
    text = ("Founded 20 years ago, we are hiring. "
            "Requirements: 2 years of professional software engineering experience.")
    r = analyze_experience(text)
    assert r.min_years == 2


# ── No-experience phrasings ───────────────────────────────────────────────
@pytest.mark.parametrize("text", [
    "No experience required.",
    "No prior experience necessary.",
    "No previous experience is required for this role.",
])
def test_no_experience_required(text):
    r = analyze_experience(text)
    assert r.min_years == 0
    assert r.entry_level_compatible
    assert R.EXPERIENCE_NONE_REQUIRED in r.reasons
    assert r.confidence == "high"


def test_equivalent_experience_is_flagged():
    r = analyze_experience("Bachelor's degree or equivalent practical experience, "
                           "plus 1 year of professional experience.")
    assert r.equivalent_experience


# ── Eligibility semantics ─────────────────────────────────────────────────
@pytest.mark.parametrize("text,compatible", [
    ("0-2 years of experience",                     True),
    ("1-2 years of professional experience",        True),
    ("2 years of software development experience",  True),
    ("2-3 years of relevant experience",            True),
    ("3+ years of engineering experience",          True),
    ("4+ years of professional experience",         True),
    ("5+ years of software engineering experience", False),
    ("7-10 years of experience",                    False),
    ("10+ years of industry experience",            False),
])
def test_entry_level_compatibility_ceiling(text, compatible):
    """Anything at or under the 4-year eligibility ceiling stays compatible."""
    assert analyze_experience(text).entry_level_compatible is compatible


def test_lowest_stated_floor_wins():
    """A JD listing a lower bar in 'minimum' and a higher one in 'preferred'
    should be judged on the minimum."""
    text = ("Minimum qualifications: 2 years of professional experience. "
            "Preferred qualifications: 5 years of industry experience.")
    assert analyze_experience(text).min_years == 2


def test_up_to_is_not_a_floor():
    assert analyze_experience("Up to 5 years of experience considered.").min_years is None


def test_written_numbers():
    assert analyze_experience("three years of professional experience").min_years == 3


def test_absent_requirement_is_not_a_rejection():
    r = analyze_experience("We build data pipelines and value curiosity.")
    assert r.min_years is None
    assert r.entry_level_compatible
    assert R.EXPERIENCE_UNKNOWN in r.reasons


def test_empty_input():
    r = analyze_experience("")
    assert r.min_years is None
    assert r.entry_level_compatible


def test_confidence_levels():
    strong = analyze_experience("3 years of professional experience required")
    assert strong.confidence == "high"
    weak = analyze_experience("2 years building distributed systems")
    assert weak.confidence in ("medium", "high")


def test_analysis_is_deterministic():
    t = "2-3 years of relevant software engineering experience"
    assert analyze_experience(t) == analyze_experience(t)


def test_raw_matches_are_recorded():
    r = analyze_experience("3+ years of engineering experience")
    assert r.raw_matches
