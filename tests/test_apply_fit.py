"""Resume-fit analysis tests — required vs preferred, and gap tolerance."""
from __future__ import annotations

import pytest

from src.apply.fit import analyze_fit, FIT_PLAUSIBLE, FIT_STRONG

REQ_JD = """
About the team. We build data platforms.

Requirements:
- 0-2 years of professional software engineering experience
- Strong Python
- Strong SQL
- Experience with AWS

Preferred qualifications:
- Kubernetes
- Terraform
"""


def _fit(jd, matched, **kw):
    return analyze_fit(jd_text=jd, matched_canonicals=set(matched), **kw)


def test_required_and_preferred_are_separated():
    r = _fit(REQ_JD, {"python", "sql", "aws"})
    assert "Python" in r.matched_required_skills
    assert set(r.missing_required_skills) == set()
    assert r.missing_preferred_skills, "preferred gaps should be reported"


def test_missing_one_preferred_skill_stays_strong():
    """The brief's example: Python+Java+AWS+Kubernetes wanted, resume lacks K8s."""
    full = _fit(REQ_JD, {"python", "sql", "aws", "kubernetes", "terraform"})
    gap = _fit(REQ_JD, {"python", "sql", "aws"})
    assert gap.resume_fit_score >= FIT_STRONG
    assert full.resume_fit_score - gap.resume_fit_score <= 12, "preferred gap penalty too large"


def test_partial_required_overlap_remains_plausible():
    """Data Engineer wanting Python+SQL+Spark, resume has Python+SQL."""
    jd = ("Requirements:\n- 1-2 years of experience\n- Python\n- SQL\n- Apache Spark\n")
    r = _fit(jd, {"python", "sql"})
    assert r.resume_fit_score >= FIT_PLAUSIBLE, "a 2-of-3 match must stay application-worthy"
    assert "Spark" in " ".join(r.missing_required_skills)


def test_missing_required_costs_more_than_missing_preferred():
    req_gap = _fit(REQ_JD, {"python", "sql", "kubernetes", "terraform"})    # missing AWS (required)
    pref_gap = _fit(REQ_JD, {"python", "sql", "aws"})                        # missing both preferred
    assert req_gap.resume_fit_score < pref_gap.resume_fit_score


def test_long_wishlist_cannot_dominate():
    """A 20-item preferred list must not swamp the score."""
    jd = REQ_JD + "\nPreferred qualifications:\n" + "\n".join(
        f"- {s}" for s in ["go", "rust", "scala", "kafka", "flink", "hadoop",
                           "redis", "graphql", "ansible", "jenkins"])
    short = _fit(REQ_JD, {"python", "sql", "aws"})
    long = _fit(jd, {"python", "sql", "aws"})
    assert short.resume_fit_score - long.resume_fit_score <= 10


def test_no_jd_is_not_treated_as_a_gap():
    r = _fit("", {"python"})
    assert r.has_jd is False
    assert r.warnings


def test_experience_fit_buckets():
    assert _fit(REQ_JD, {"python"}, experience_min=1).experience_fit == "ideal"
    assert _fit(REQ_JD, {"python"}, experience_min=4).experience_fit == "stretch"
    assert _fit(REQ_JD, {"python"}, experience_min=6).experience_fit == "reach"
    assert _fit(REQ_JD, {"python"}, experience_min=None).experience_fit in ("ideal", "unknown")


def test_phd_gate_only_when_no_alternative():
    hard = _fit("Requirements:\n- PhD in Computer Science required\n", {"python"})
    soft = _fit("Requirements:\n- MS or PhD in a quantitative field\n", {"python"})
    assert hard.education_fit == "phd_required"
    assert soft.education_fit == "met"


def test_work_auth_signals():
    assert _fit("We are unable to sponsor visas for this role.", {"python"}).work_auth_fit == "risk"
    assert _fit("Visa sponsorship available for this role.", {"python"}).work_auth_fit == "sponsors"
    assert _fit(REQ_JD, {"python"}).work_auth_fit == "silent"


def test_fit_is_deterministic():
    a = _fit(REQ_JD, {"python", "sql", "aws"})
    b = _fit(REQ_JD, {"python", "sql", "aws"})
    assert a == b


def test_every_fit_result_is_explainable():
    r = _fit(REQ_JD, {"python", "sql"})
    assert r.positive_reasons or r.warnings
    assert 0 <= r.resume_fit_score <= 100
