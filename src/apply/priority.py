"""Application priority — "which eligible job is worth twenty minutes?".

Deliberately a *bounded* adjustment of the Phase 1 screening score rather than
an independent score. Two reasons:

1. Phase 1's vetoes stay authoritative. A REJECT never becomes actionable
   here, and the blend runs only on jobs Phase 1 already allowed through.

2. Fit cannot bury an eligible job. The owner's profile and all three resumes
   contain no Java, Spring, React, Node or Kubernetes, so a pure fit ranking
   would systematically sink the software / backend / full-stack families that
   Phase 1 was explicitly widened to include. Capping the adjustment at
   ±FIT_SWING keeps those roles visible while still letting a genuinely strong
   data or ML match rise above them.

The three numbers stay separately inspectable — screening, fit, priority — so a
later phase can ask which of them actually predicted an interview.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .fit import FitResult
from ..screening.scoring import (
    APPLY_NOW as SCREEN_APPLY_NOW, STRONG as SCREEN_STRONG,
    REVIEW as SCREEN_REVIEW, LOW as SCREEN_LOW, REJECT as SCREEN_REJECT,
)

APPLY_FIRST = "APPLY_FIRST"
HIGH        = "HIGH"
MEDIUM      = "MEDIUM"
REVIEW      = "REVIEW"
LOW         = "LOW"
REJECT      = "REJECT"

# Calibrated against the production corpus — see the Phase 2 report. The bands
# are placed so APPLY_FIRST stays a short, actionable list rather than a
# relabelling of everything Phase 1 called APPLY_NOW.
BAND_APPLY_FIRST = 88
BAND_HIGH        = 78
BAND_MEDIUM      = 66
BAND_REVIEW      = 55

# Priority is a weighted blend of the two scores, then clamped so it can never
# deviate from the screening score by more than FIT_SWING.
#
# The blend supplies the spread: an additive "screening + fit bonus" pinned
# almost every strong job at 100 during calibration, which made the bands
# meaningless. The clamp supplies the safety: it guarantees a Phase 1
# APPLY_NOW job cannot fall below screening-18 on fit alone, which is what
# keeps the software / backend / full-stack families visible despite a resume
# that contains none of their technologies.
W_SCREENING = 0.65
W_FIT = 0.35
FIT_SWING = 18

# Role-family weighting. Left NEUTRAL on purpose. src/profile.py lists only
# data and AI target_roles and carries no software-engineering skills, so the
# profile *would* support a data/AI tilt — but Phase 1 deliberately widened the
# search to software, backend and full-stack, and silently re-narrowing it here
# would undo that decision. Set a family to a non-zero value to express a
# preference explicitly.
ROLE_FAMILY_WEIGHTS: dict[str, int] = {
    "data_engineering": 0,
    "data_science": 0,
    "ml_ai": 0,
    "software_engineering": 0,
    "backend": 0,
    "fullstack": 0,
    "data_analytics": -4,        # secondary in Phase 1; mirrors that standing
    "adjacent_analysis": -8,     # Tier 3 — review-only, must not top the queue
}

# Signals that make a job more or less worth the next twenty minutes, on top of
# what screening already accounted for.
W_WORK_AUTH_RISK = -12
W_PHD_REQUIRED   = -10
W_US_CONFIRMED   = 3
W_AMBIGUOUS_LOC  = -3
W_GHOST_CAUTION  = -4
W_GHOST_SUSPECT  = -10


@dataclass(frozen=True)
class ApplicationPriority:
    application_priority_score: int
    priority: str
    screening_score: int = 0
    resume_fit_score: int = 0
    fit_adjustment: int = 0
    positive_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def is_actionable(self) -> bool:
        return self.priority in (APPLY_FIRST, HIGH, MEDIUM, REVIEW)


def _band(score: int) -> str:
    if score >= BAND_APPLY_FIRST:
        return APPLY_FIRST
    if score >= BAND_HIGH:
        return HIGH
    if score >= BAND_MEDIUM:
        return MEDIUM
    if score >= BAND_REVIEW:
        return REVIEW
    return LOW


def application_priority(
    *,
    screening_score: int,
    screening_priority: str,
    fit: FitResult,
    role_family: str = "",
    location_class: str = "",
    ghost_level: str = "",
) -> ApplicationPriority:
    """Blend the Phase 1 screening verdict with resume fit.

    Phase 1's REJECT is absolute and short-circuits before any arithmetic, so
    no amount of skill overlap can make a senior, non-US or profile-mismatched
    job actionable.
    """
    if screening_priority == SCREEN_REJECT:
        return ApplicationPriority(
            application_priority_score=0, priority=REJECT,
            screening_score=screening_score, resume_fit_score=fit.resume_fit_score,
            warnings=("Rejected by Phase 1 screening — not an eligible job",),
        )

    positives: list[str] = []
    warnings: list[str] = []

    # No description means fit could not be assessed. Blending a placeholder
    # score here demoted Phase 1 APPLY_NOW new-grad roles purely because their
    # JD text was missing — penalising absence of information rather than a
    # genuine gap. Such jobs keep their screening score untouched.
    if not fit.has_jd:
        score = max(0, min(100, screening_score + ROLE_FAMILY_WEIGHTS.get(role_family, 0)))
        warnings.append("Resume fit not assessed — no job description available")
        warnings.extend(fit.warnings)
        return ApplicationPriority(
            application_priority_score=score, priority=_band(score),
            screening_score=screening_score, resume_fit_score=fit.resume_fit_score,
            fit_adjustment=0, warnings=tuple(dict.fromkeys(warnings)),
        )

    # Weighted blend, then clamped to the screening score ± FIT_SWING.
    blended = W_SCREENING * screening_score + W_FIT * fit.resume_fit_score
    floor, ceiling = screening_score - FIT_SWING, screening_score + FIT_SWING
    blended = max(floor, min(ceiling, blended))
    fit_adjustment = int(round(blended - screening_score))

    score = int(round(blended))
    score += ROLE_FAMILY_WEIGHTS.get(role_family, 0)

    if location_class == "US" or location_class == "US_REMOTE":
        score += W_US_CONFIRMED
    elif location_class == "AMBIGUOUS":
        score += W_AMBIGUOUS_LOC
        warnings.append("Location not confirmed US")

    if fit.work_auth_fit == "risk":
        score += W_WORK_AUTH_RISK
        warnings.append("JD suggests sponsorship is not offered")
    if fit.education_fit == "phd_required":
        score += W_PHD_REQUIRED
        warnings.append("PhD appears to be required")

    if ghost_level == "suspicious":
        score += W_GHOST_SUSPECT
        warnings.append("Posting looks like an evergreen / ghost listing")
    elif ghost_level == "caution":
        score += W_GHOST_CAUTION
        warnings.append("Posting shows some ghost-listing signals")

    positives.extend(fit.positive_reasons)
    warnings.extend(fit.warnings)
    if fit_adjustment > 0:
        positives.append(f"Resume fit {fit.resume_fit_score}/100 ({fit.band})")
    elif fit_adjustment < 0:
        warnings.append(f"Resume fit {fit.resume_fit_score}/100 ({fit.band})")

    score = max(0, min(100, score))
    return ApplicationPriority(
        application_priority_score=score,
        priority=_band(score),
        screening_score=screening_score,
        resume_fit_score=fit.resume_fit_score,
        fit_adjustment=fit_adjustment,
        positive_reasons=tuple(dict.fromkeys(positives)),
        warnings=tuple(dict.fromkeys(warnings)),
    )
