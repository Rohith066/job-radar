"""End-to-end Phase 2 scenarios from the brief, plus notifier rendering."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.apply.fit import analyze_fit
from src.apply.priority import (
    application_priority, APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW, REJECT,
)
from src.screening import analyze_title, analyze_location, analyze_experience, score_job
from src.sources.base import Job
from src.notifier import _build_html, _build_plaintext

NOW = datetime.now(timezone.utc)


def pipeline(title, location, jd, resume_skills, *, hours_old=2, country_focus=""):
    """Full Phase 1 -> Phase 2 assessment, the same order production uses."""
    from src.matching import ontology
    t = analyze_title(title)
    loc = analyze_location(location, country_focus)
    exp = analyze_experience(jd)
    scr = score_job(title=t, location=loc, experience=exp,
                    posted_at=NOW - timedelta(hours=hours_old))
    jd_can = ontology.extract_canonical_skills(jd) if jd else {}
    fit = analyze_fit(jd_text=jd, matched_canonicals={c for c in jd_can if c in resume_skills},
                      role_family=t.role_family, experience_min=exp.min_years)
    ap = application_priority(screening_score=scr.score, screening_priority=scr.priority,
                              fit=fit, role_family=t.role_family,
                              location_class=loc.classification)
    return scr, fit, ap


RESUME = {"python", "sql", "aws"}

SWE_JD = ("Requirements:\n- 0-2 years of professional software engineering experience\n"
          "- Python\n- SQL\n- AWS\n")
BACKEND_JD = ("Requirements:\n- 1-2 years of experience\n- Python\n- AWS\n"
              "Preferred qualifications:\n- Kubernetes\n")
DE_JD = ("Requirements:\n- 1-2 years of experience\n- Python\n- SQL\n- Apache Spark\n")


def test_strong_match_is_top_priority():
    scr, fit, ap = pipeline("Software Engineer I", "Remote - US", SWE_JD, RESUME, hours_old=1)
    assert scr.priority == "APPLY_NOW"
    assert ap.priority in (APPLY_FIRST, HIGH)
    assert fit.band in ("strong", "exceptional")


def test_one_preferred_skill_missing_stays_strong():
    _, fit, ap = pipeline("Backend Engineer I", "Remote - US", BACKEND_JD, RESUME, hours_old=1)
    assert ap.priority in (APPLY_FIRST, HIGH)
    assert "Kubernetes" in " ".join(fit.missing_preferred_skills)
    assert ap.is_actionable


def test_partial_but_plausible_match_stays_application_worthy():
    _, fit, ap = pipeline("Data Engineer I", "Remote - US", DE_JD, RESUME, hours_old=2)
    assert ap.priority != REJECT
    assert ap.priority in (APPLY_FIRST, HIGH, MEDIUM, REVIEW)
    assert "Spark" in " ".join(fit.missing_required_skills)


def test_hard_seniority_veto_survives_perfect_overlap():
    scr, _, ap = pipeline("Senior Software Engineer", "Remote - US", SWE_JD, RESUME, hours_old=1)
    assert scr.priority == "REJECT"
    assert ap.priority == REJECT
    assert ap.application_priority_score == 0


def test_manager_veto_survives_perfect_overlap():
    _, _, ap = pipeline("Software Engineering Manager", "Remote - US", SWE_JD, RESUME)
    assert ap.priority == REJECT


def test_wrong_geography_survives_perfect_overlap():
    scr, _, ap = pipeline("Machine Learning Engineer I", "Remote (DEU)", SWE_JD, RESUME)
    assert scr.priority == "REJECT"
    assert ap.priority == REJECT


def test_ambiguous_remote_is_not_fabricated_into_us():
    scr, fit, ap = pipeline("Software Engineer I", "Remote", SWE_JD, RESUME, hours_old=2)
    loc = analyze_location("Remote", "")
    assert loc.classification == "AMBIGUOUS"
    assert ap.priority != REJECT
    assert any("not confirmed US" in w for w in ap.warnings)


def test_profile_mismatch_survives_perfect_overlap():
    _, _, ap = pipeline("Robotics Engineer", "Remote - US", SWE_JD, RESUME)
    assert ap.priority == REJECT


def test_all_first_class_families_can_reach_actionable():
    cases = [("Software Engineer I", SWE_JD), ("Backend Engineer I", BACKEND_JD),
             ("Full Stack Engineer I", SWE_JD), ("Data Engineer I", DE_JD),
             ("Machine Learning Engineer I", SWE_JD), ("Data Scientist I", SWE_JD)]
    for title, jd in cases:
        _, _, ap = pipeline(title, "Remote - US", jd, RESUME, hours_old=1)
        assert ap.is_actionable, f"{title} -> {ap.priority}"


def test_swe_role_with_no_matching_skills_stays_visible():
    """The resume has no Java/React/K8s; such roles must not vanish."""
    java_jd = ("Requirements:\n- 0-2 years of experience\n- Java\n- Spring Boot\n- Kubernetes\n")
    scr, fit, ap = pipeline("Software Engineer I", "Remote - US", java_jd, set(), hours_old=1)
    assert scr.priority == "APPLY_NOW"
    assert ap.priority in (APPLY_FIRST, HIGH, MEDIUM), f"buried at {ap.priority}"


def test_three_scores_are_distinct_and_diagnosable():
    scr, fit, ap = pipeline("Data Engineer I", "Remote - US", DE_JD, RESUME)
    assert ap.screening_score == scr.score
    assert ap.resume_fit_score == fit.resume_fit_score
    assert isinstance(ap.application_priority_score, int)


# ── Notifier rendering ────────────────────────────────────────────────────
def _job(**kw):
    j = Job(key="k", source="greenhouse", company="Acme", title="Software Engineer I",
            location="Remote - US", url="http://x",
            posted=(NOW - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S+0000"))
    for k, v in kw.items():
        setattr(j, k, v)
    return j


def test_actionable_job_renders_fit_detail():
    j = _job(priority="APPLY_NOW", opportunity_score=95, application_priority="APPLY_FIRST",
             application_priority_score=94, resume_fit_score=88,
             fit_matched_required=["Python", "SQL", "AWS"], fit_missing_preferred=["Kubernetes"])
    html = _build_html([j], [], "main")
    assert "APPLY FIRST" in html
    assert "Resume fit" in html
    assert "Kubernetes" in html
    text = _build_plaintext([j], [])
    assert "APPLY_FIRST" in text and "resume fit" in text


def test_ordinary_review_job_stays_compact():
    """REVIEW cards must not grow the fit block — the top of the email is the point."""
    j = _job(priority="REVIEW", opportunity_score=60, application_priority="REVIEW",
             application_priority_score=60, resume_fit_score=55)
    html = _build_html([], [j], "main")
    assert "APPLY FIRST" not in html
    assert "Application priority" not in html


def test_notifier_handles_jobs_without_phase2_fields():
    """Backward compatibility: a Job built by older code must still render."""
    html = _build_html([_job(priority="APPLY_NOW", opportunity_score=90)], [], "main")
    assert "Acme" in html
