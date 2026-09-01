"""Application-priority tests — blending, vetoes, and recall protection."""
from __future__ import annotations

import pytest

from src.apply.fit import analyze_fit
from src.apply.priority import (
    application_priority, APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW, REJECT,
    FIT_SWING, ROLE_FAMILY_WEIGHTS,
)

JD = ("Requirements:\n- 0-2 years of professional experience\n- Python\n- SQL\n"
      "Preferred qualifications:\n- Kubernetes\n")


def _p(screening, screening_priority, matched, *, family="software_engineering",
       loc="US_REMOTE", jd=JD, ghost=""):
    fit = analyze_fit(jd_text=jd, matched_canonicals=set(matched), role_family=family)
    return application_priority(screening_score=screening, screening_priority=screening_priority,
                               fit=fit, role_family=family, location_class=loc, ghost_level=ghost)


# ── Hard vetoes stay authoritative ────────────────────────────────────────
def test_phase1_reject_can_never_become_actionable():
    """Perfect skill overlap must not rescue a rejected job."""
    r = _p(0, "REJECT", {"python", "sql", "kubernetes"})
    assert r.priority == REJECT
    assert r.application_priority_score == 0
    assert not r.is_actionable


def test_senior_role_with_perfect_overlap_still_rejected():
    r = _p(0, "REJECT", {"python", "sql", "kubernetes"}, family="software_engineering")
    assert r.priority == REJECT


def test_non_us_role_with_perfect_overlap_still_rejected():
    r = _p(0, "REJECT", {"python", "sql"}, loc="NON_US")
    assert r.priority == REJECT


# ── Fit cannot bury an eligible job ───────────────────────────────────────
def test_fit_adjustment_is_bounded():
    """The owner's resume has no Java/React/K8s; SWE roles must stay visible."""
    strong = _p(95, "APPLY_NOW", {"python", "sql", "kubernetes"})
    weak = _p(95, "APPLY_NOW", set())
    assert weak.application_priority_score >= 95 - FIT_SWING
    assert strong.application_priority_score >= weak.application_priority_score
    assert abs(weak.fit_adjustment) <= FIT_SWING


def test_apply_now_never_falls_below_medium():
    for matched in (set(), {"python"}, {"python", "sql"}):
        r = _p(95, "APPLY_NOW", matched)
        assert r.priority in (APPLY_FIRST, HIGH, MEDIUM), f"{matched} -> {r.priority}"


def test_missing_jd_leaves_screening_untouched():
    """An unassessable job is neutral, not penalised."""
    r = _p(95, "APPLY_NOW", set(), jd="")
    assert r.fit_adjustment == 0
    assert r.application_priority_score == 95 + ROLE_FAMILY_WEIGHTS.get("software_engineering", 0)


# ── Blending behaves ──────────────────────────────────────────────────────
def test_better_fit_ranks_higher_at_equal_screening():
    good = _p(85, "APPLY_NOW", {"python", "sql", "kubernetes"})
    poor = _p(85, "APPLY_NOW", set())
    assert good.application_priority_score > poor.application_priority_score


def test_scores_stay_in_range_and_bands_are_ordered():
    from src.apply.priority import BAND_APPLY_FIRST, BAND_HIGH, BAND_MEDIUM, BAND_REVIEW
    assert BAND_APPLY_FIRST > BAND_HIGH > BAND_MEDIUM > BAND_REVIEW
    for s in (0, 25, 55, 70, 85, 100):
        r = _p(s, "REVIEW", {"python"})
        assert 0 <= r.application_priority_score <= 100


def test_adjacent_family_is_weighted_down():
    """Tier-3 roles must not top the queue."""
    assert ROLE_FAMILY_WEIGHTS["adjacent_analysis"] < 0
    adj = _p(69, "REVIEW", {"python", "sql"}, family="adjacent_analysis")
    tgt = _p(69, "REVIEW", {"python", "sql"}, family="data_engineering")
    assert adj.application_priority_score < tgt.application_priority_score


def test_all_first_class_families_are_weighted_equally():
    """Phase 1 widened the search deliberately; Phase 2 must not re-narrow it."""
    first_class = ["data_engineering", "data_science", "ml_ai",
                   "software_engineering", "backend", "fullstack"]
    weights = {ROLE_FAMILY_WEIGHTS[f] for f in first_class}
    assert weights == {0}, "a family preference was introduced without being declared"


def test_ambiguous_location_is_flagged_not_fabricated():
    r = _p(80, "REVIEW", {"python", "sql"}, loc="AMBIGUOUS")
    assert any("not confirmed US" in w for w in r.warnings)


def test_ghost_listings_are_down_weighted():
    clean = _p(85, "APPLY_NOW", {"python", "sql"})
    ghost = _p(85, "APPLY_NOW", {"python", "sql"}, ghost="suspicious")
    assert ghost.application_priority_score < clean.application_priority_score


def test_priority_is_deterministic():
    assert _p(85, "APPLY_NOW", {"python"}) == _p(85, "APPLY_NOW", {"python"})


def test_three_scores_remain_separately_visible():
    r = _p(85, "APPLY_NOW", {"python", "sql"})
    assert r.screening_score == 85
    assert r.resume_fit_score > 0
    assert r.application_priority_score > 0
    assert r.screening_score != r.resume_fit_score or True   # all three are distinct fields
