"""Title analysis regression tests.

The two named requirements from the Phase 1 spec are asserted explicitly:
`Software Engineering Manager` must never be a good entry-level Software
Engineer result, and `Software Engineer IV` must never read as entry level.
"""
from __future__ import annotations

import pytest

from src.screening import reasons as R
from src.screening.titles import analyze_title, normalize_title
from src.classifier import classify


# ── The full matrix from the specification ────────────────────────────────
@pytest.mark.parametrize("title,expected", [
    # Software engineering
    ("Software Engineer",                       "YES"),
    ("Software Engineer I",                     "YES"),
    ("Software Engineer II",                    "MAYBE"),
    ("Software Engineer III",                   "NO"),
    ("Software Engineer IV",                    "NO"),
    ("Software Engineer 1",                     "YES"),
    ("Software Engineer 2",                     "MAYBE"),
    ("Software Engineer 3",                     "NO"),
    ("Junior Software Engineer",                "YES"),
    ("Associate Software Engineer",             "YES"),
    ("New Grad Software Engineer",              "YES"),
    ("Software Engineer - University Graduate", "YES"),
    ("Senior Software Engineer",                "NO"),
    ("Sr. Software Engineer",                   "NO"),
    ("Staff Software Engineer",                 "NO"),
    ("Principal Software Engineer",             "NO"),
    ("Lead Software Engineer",                  "NO"),
    ("Software Engineering Manager",            "NO"),
    ("Engineering Manager, Software",           "NO"),
    ("Director of Software Engineering",        "NO"),
    ("VP of Engineering",                       "NO"),
    # Backend / full stack
    ("Backend Engineer",                        "YES"),
    ("Senior Backend Engineer",                 "NO"),
    ("Full Stack Engineer",                     "YES"),
    ("Full-Stack Developer",                    "YES"),
    # Data
    ("Data Engineer",                           "YES"),
    ("Data Engineer I",                         "YES"),
    ("Senior Data Engineer",                    "NO"),
    ("Data Engineer IV",                        "NO"),
    # ML / AI
    ("Machine Learning Engineer",               "YES"),
    ("Machine Learning Engineer II",            "MAYBE"),
    ("Staff Machine Learning Engineer",         "NO"),
    ("AI Engineer",                             "YES"),
    # Data science
    ("Data Scientist",                          "YES"),
    ("Senior Data Scientist",                   "NO"),
    # Must never be strong matches merely because they contain "engineer"
    ("Sales Engineer",                          "NO"),
    ("Solutions Engineer",                      "NO"),
    ("Mechanical Engineer",                     "NO"),
    ("Civil Engineer",                          "NO"),
])
def test_classification_matrix(title, expected):
    assert analyze_title(title).classification == expected


# ── The two explicit regression requirements ──────────────────────────────
def test_software_engineering_manager_is_not_an_entry_level_swe_job():
    r = analyze_title("Software Engineering Manager")
    assert r.classification == "NO"
    assert r.seniority == "manager"
    assert R.MANAGER_TITLE in r.reasons
    # And it must not reach the user through the legacy entry point either.
    assert classify("Software Engineering Manager").label == "no"


def test_software_engineer_iv_is_not_entry_level():
    r = analyze_title("Software Engineer IV")
    assert r.classification == "NO"
    assert r.level == 4
    assert not r.is_entry_level
    assert R.LEVEL_FOUR_PLUS_TITLE in r.reasons


def test_engineer_ii_is_reviewable_not_rejected():
    """Engineer II must stay ambiguous — never blindly rejected."""
    for t in ("Software Engineer II", "Data Engineer II", "Engineer 2"):
        r = analyze_title(t)
        assert r.classification == "MAYBE", t
        assert r.seniority == "ambiguous", t


# ── Phrase boundaries ─────────────────────────────────────────────────────
def test_substring_overlap_does_not_create_a_match():
    """'software engineer' must not match inside 'software engineering manager'
    just because the leading characters overlap."""
    r = analyze_title("Software Engineering Manager")
    assert r.classification == "NO"


@pytest.mark.parametrize("title", [
    "Manager, Data Engineering",
    "Senior Manager, Machine Learning Engineering",
    "Data Science Manager",
    "AI / ML Engineer Manager",
    "Associate Manager, Data Governance",
    "Head of Data Engineering",
    "VP Senior AI/AML Engineer",
    "Director, Data Platform",
])
def test_managerial_titles_are_rejected_regardless_of_word_order(title):
    """The previous classifier only matched the contiguous string
    'engineering manager', so 'Manager, Data Engineering' slipped through."""
    assert analyze_title(title).classification == "NO"


@pytest.mark.parametrize("title,seniority", [
    ("Senior Data Engineer",         "senior"),
    ("Sr Data Engineer",             "senior"),
    ("Sr. Data Engineer",            "senior"),
    ("Staff Data Engineer",          "staff"),
    ("Principal Data Engineer",      "principal"),
    ("Lead Data Engineer",           "lead"),
    ("Tech Lead, Backend",           "lead"),
    ("Data Architect",               "architect"),
    ("Distinguished Engineer",       "principal"),
    ("Chief Data Officer",           "executive"),
    ("Vice President, Engineering",  "executive"),
])
def test_seniority_buckets(title, seniority):
    r = analyze_title(title)
    assert r.seniority == seniority
    assert r.classification == "NO"


# ── Entry-level markers ───────────────────────────────────────────────────
@pytest.mark.parametrize("title,code", [
    ("New Grad Software Engineer",            R.NEW_GRAD_EXPLICIT),
    ("New Graduate Data Engineer",            R.NEW_GRAD_EXPLICIT),
    ("Software Engineer, University Graduate", R.NEW_GRAD_EXPLICIT),
    ("Campus Hire - Data Engineer",           R.NEW_GRAD_EXPLICIT),
    ("Entry Level Software Engineer",         R.ENTRY_LEVEL_EXPLICIT),
    ("Entry-Level Data Analyst",              R.ENTRY_LEVEL_EXPLICIT),
    ("Junior Backend Engineer",               R.JUNIOR_TITLE),
    ("Jr. Software Developer",                R.JUNIOR_TITLE),
    ("Early Career Software Engineer",        R.EARLY_CAREER_TITLE),
    ("Associate Data Engineer",               R.ASSOCIATE_TITLE),
    ("Software Engineer I",                   R.LEVEL_ONE_TITLE),
])
def test_entry_markers_are_detected(title, code):
    r = analyze_title(title)
    assert code in r.reasons
    assert r.seniority == "entry"
    assert r.classification == "YES"


def test_seniority_beats_entry_marker():
    """'Associate Director' is senior, not entry — the veto must win."""
    r = analyze_title("Associate Director, Data Engineering")
    assert r.classification == "NO"
    assert r.seniority == "director"


# ── Levels ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("title,level", [
    ("Software Engineer I",    1),
    ("Software Engineer II",   2),
    ("Software Engineer III",  3),
    ("Software Engineer IV",   4),
    ("Software Engineer V",    5),
    ("Data Engineer 1",        1),
    ("Data Engineer 4",        4),
    ("Data Scientist III",     3),
    ("Backend Engineer, Level 2", 2),
])
def test_level_extraction(title, level):
    assert analyze_title(title).level == level


@pytest.mark.parametrize("title", [
    "Data Engineer (2 openings)",
    "Software Engineer, Python 3 Platform",
    "AI Engineer",
])
def test_numbers_that_are_not_levels(title):
    """A number in a title is only a level when it follows a role noun or an
    explicit level word."""
    assert analyze_title(title).level is None


def test_ai_engineer_is_not_read_as_roman_numeral():
    r = analyze_title("AI Engineer")
    assert r.level is None
    assert r.classification == "YES"
    assert r.role_family == "ml_ai"


# ── Absolute vetoes ───────────────────────────────────────────────────────
@pytest.mark.parametrize("title", [
    "Software Engineer Intern",
    "Data Engineering Internship",
    "Software Engineer Co-op",
    "Part-Time Data Analyst",
])
def test_internships_and_part_time_are_rejected(title):
    r = analyze_title(title)
    assert r.classification == "NO"
    assert R.INTERNSHIP_TITLE in r.reasons


@pytest.mark.parametrize("title", [
    "Software Engineer - TS/SCI Required",
    "Data Engineer (Active Secret Clearance)",
    "Software Engineer - US Citizens Only",
])
def test_clearance_titles_are_rejected(title):
    assert analyze_title(title).classification == "NO"


# ── Role families ─────────────────────────────────────────────────────────
@pytest.mark.parametrize("title,family", [
    ("Software Engineer",          "software_engineering"),
    ("Backend Engineer",           "backend"),
    ("Full Stack Engineer",        "fullstack"),
    ("Data Engineer",              "data_engineering"),
    ("Machine Learning Engineer",  "ml_ai"),
    ("Data Scientist",             "data_science"),
    ("Data Analyst",               "data_analytics"),
    ("Sales Engineer",             "unrelated"),
    ("Mechanical Engineer",        "profile_mismatch"),
])
def test_role_family_detection(title, family):
    assert analyze_title(title).role_family == family


def test_empty_and_whitespace_titles():
    for t in ("", "   ", None):
        r = analyze_title(t)
        assert r.classification == "NO"


def test_normalization_is_stable():
    assert normalize_title("  Senior   Software  Engineer  ") == "senior software engineer"
    assert normalize_title("Data Engineer (Remote)") == "data engineer remote"


def test_analysis_is_deterministic():
    a = analyze_title("New Grad Software Engineer II")
    b = analyze_title("New Grad Software Engineer II")
    assert a == b
