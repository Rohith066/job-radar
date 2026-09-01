"""Deterministic, explainable opportunity scoring.

This score answers a different question from `resume_match`, and the two are
kept separate on purpose. `resume_match` (src/matching/) measures skill overlap
against the resume, and its 92/85/65 bands are percentiles calibrated over the
frozen corpus — folding freshness or seniority into that number would silently
invalidate them (see CLAUDE.md). This score answers "is this an entry-level job
worth applying to right now", and drives alert routing only.

Every point is attributable to a reason code. Hard exclusions bypass the
arithmetic entirely, so a fresh posting can never rescue a manager role.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import reasons as R
from .titles import TitleAnalysis, TARGET_FAMILIES, ADJACENT_FAMILIES
from .locations import LocationAnalysis, US, US_REMOTE, NON_US, AMBIGUOUS
from .experience import ExperienceAnalysis, REJECT_FLOOR, STRONG_NEGATIVE_FLOOR

# ---------------------------------------------------------------------------
# Score table — the complete set of constants. Nothing else moves the score.
# ---------------------------------------------------------------------------
BASE_SCORE = 50

# Seniority / entry-level (title-derived)
W_NEW_GRAD            = 28
W_ENTRY_LEVEL         = 25
W_JUNIOR              = 20
W_EARLY_CAREER        = 20
W_ASSOCIATE           = 15
W_LEVEL_ONE           = 15
W_LEVEL_TWO           = 4     # ambiguous: kept reviewable, mildly positive
# Applied when nothing in the title OR the JD establishes the role as
# early-career. Without it a plain "Software Engineer" that is merely fresh
# and US-based reaches APPLY_NOW, which floods the top band with roles that
# were never shown to be entry level.
W_NO_ENTRY_EVIDENCE   = -12

# Role family
W_FAMILY_TARGET       = 12
W_FAMILY_SECONDARY    = 4
W_FAMILY_AMBIGUOUS    = -5

# Experience.  Per the owner's eligibility ceiling (4 years), nothing at or
# below 4 is penalised — a 2-3 year posting is a realistic application, and
# penalising it would suppress exactly the jobs this system exists to surface.
W_EXP_NONE_REQUIRED   = 22
W_EXP_0_2             = 20
W_EXP_1_2             = 15
W_EXP_2               = 10
W_EXP_2_3             = 8
W_EXP_3               = 4
W_EXP_4               = 0
W_EXP_5_PLUS          = -30
W_EXP_UNKNOWN         = 0
W_EXP_LOW_CONFIDENCE  = -2    # figure found but context was weak

# Location
W_US_CONFIRMED        = 10
W_US_REMOTE           = 12
W_US_VIA_BOARD        = 6
W_LOCATION_AMBIGUOUS  = -4

# Freshness — from posted_at only. A discovery timestamp is not a posting date,
# so first_seen_at never earns a freshness bonus.
W_FRESH_LT_6H         = 15
W_FRESH_LT_24H        = 10
W_FRESH_LT_3D         = 4
W_STALE               = -5
W_NO_POSTED_DATE      = 0

# Priority bands
BAND_APPLY_NOW = 85
BAND_STRONG    = 70
BAND_REVIEW    = 55

# Adjacent (pre-Phase-1 Tier 3) occupations are visible but review-only. The
# ceiling keeps them out of STRONG and APPLY_NOW however strong their other
# signals are, restoring the old score-55 "maybe" semantics.
ADJACENT_CEILING = BAND_STRONG - 1

# Recall floor. A first-class target role inside the experience ceiling should
# not disappear from the digest just because its location is an unscoped
# "Remote" and its title carries no explicit entry wording — those two
# penalties compound to below 55 on their own. The floor lifts such jobs to
# REVIEW and no further: it protects recall without asserting the job is US.
RECALL_FLOOR_EXPERIENCE_MAX = 4

APPLY_NOW = "APPLY_NOW"
STRONG    = "STRONG"
REVIEW    = "REVIEW"
LOW       = "LOW"
REJECT    = "REJECT"

STALE_AFTER_DAYS = 7


@dataclass(frozen=True)
class OpportunityScore:
    score: int
    priority: str
    positive_reasons: tuple[str, ...] = field(default_factory=tuple)
    negative_reasons: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    reason_codes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def alerts(self) -> bool:
        return self.priority in (APPLY_NOW, STRONG, REVIEW)


def _hours_since(posted_at: datetime | None) -> float | None:
    if posted_at is None:
        return None
    if posted_at.tzinfo is None:
        posted_at = posted_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - posted_at).total_seconds() / 3600.0


def _experience_points(exp: ExperienceAnalysis) -> tuple[int, list[str], list[str]]:
    pos: list[str] = []
    neg: list[str] = []

    if exp.min_years is None:
        return W_EXP_UNKNOWN, pos, neg

    lo, hi = exp.min_years, exp.max_years
    if R.EXPERIENCE_NONE_REQUIRED in exp.reasons:
        pos.append(R.EXPERIENCE_NONE_REQUIRED)
        return W_EXP_NONE_REQUIRED, pos, neg

    if lo == 0:
        pts, code = W_EXP_0_2, R.EXPERIENCE_0_2
    elif lo == 1:
        pts, code = W_EXP_1_2, R.EXPERIENCE_1_2
    elif lo == 2:
        if hi is not None and hi >= 3:
            pts, code = W_EXP_2_3, R.EXPERIENCE_2_3
        else:
            pts, code = W_EXP_2, R.EXPERIENCE_2
    elif lo == 3:
        pts, code = W_EXP_3, R.EXPERIENCE_3
    elif lo == 4:
        pts, code = W_EXP_4, R.EXPERIENCE_4
    elif lo >= REJECT_FLOOR:
        pts, code = W_EXP_5_PLUS, R.EXPERIENCE_7_PLUS
    else:
        pts, code = W_EXP_5_PLUS, R.EXPERIENCE_5_PLUS

    (pos if pts > 0 else neg if pts < 0 else pos).append(code)

    if exp.confidence != "high":
        pts += W_EXP_LOW_CONFIDENCE
        neg.append(R.EXPERIENCE_LOW_CONF)

    return pts, pos, neg


_ENTRY_WEIGHTS = {
    R.NEW_GRAD_EXPLICIT:    W_NEW_GRAD,
    R.ENTRY_LEVEL_EXPLICIT: W_ENTRY_LEVEL,
    R.JUNIOR_TITLE:         W_JUNIOR,
    R.EARLY_CAREER_TITLE:   W_EARLY_CAREER,
    R.ASSOCIATE_TITLE:      W_ASSOCIATE,
    R.LEVEL_ONE_TITLE:      W_LEVEL_ONE,
    R.LEVEL_TWO_AMBIGUOUS:  W_LEVEL_TWO,
}


def score_job(
    *,
    title: TitleAnalysis,
    location: LocationAnalysis,
    experience: ExperienceAnalysis | None = None,
    posted_at: datetime | None = None,
) -> OpportunityScore:
    """Combine the three analyses plus freshness into a 0-100 score.

    Hard exclusions short-circuit to REJECT before any arithmetic runs, which
    is what guarantees requirement 16: no accumulation of freshness, location
    or family points can lift a senior or managerial title into an alert.
    """
    codes: list[str] = []
    pos: list[str] = []
    neg: list[str] = []
    warn: list[str] = []

    # ── Hard exclusions ────────────────────────────────────────────────────
    if title.classification == "NO":
        neg.extend(title.reasons)
        return OpportunityScore(0, REJECT, (), tuple(title.reasons), (), tuple(title.reasons))

    if location.classification == NON_US:
        neg.extend(location.reasons)
        return OpportunityScore(0, REJECT, (), tuple(location.reasons), (), tuple(location.reasons))

    if experience is not None and experience.min_years is not None:
        if experience.min_years >= REJECT_FLOOR:
            codes = list(title.reasons) + list(location.reasons) + list(experience.reasons)
            return OpportunityScore(0, REJECT, (), tuple(experience.reasons), (), tuple(codes))

    score = BASE_SCORE

    # ── Seniority / entry-level ────────────────────────────────────────────
    entry_pts = 0
    for code in title.reasons:
        if code in _ENTRY_WEIGHTS:
            entry_pts = max(entry_pts, _ENTRY_WEIGHTS[code])
            (pos if _ENTRY_WEIGHTS[code] > 0 else warn).append(code)
    score += entry_pts

    # ── Role family ────────────────────────────────────────────────────────
    if R.ROLE_FAMILY_TARGET in title.reasons:
        score += W_FAMILY_TARGET
        pos.append(R.ROLE_FAMILY_TARGET)
    elif R.ROLE_FAMILY_SECONDARY in title.reasons:
        score += W_FAMILY_SECONDARY
        pos.append(R.ROLE_FAMILY_SECONDARY)
    elif R.ROLE_FAMILY_AMBIGUOUS in title.reasons:
        score += W_FAMILY_AMBIGUOUS
        warn.append(R.ROLE_FAMILY_AMBIGUOUS)

    # ── Experience ─────────────────────────────────────────────────────────
    if experience is not None:
        pts, e_pos, e_neg = _experience_points(experience)
        score += pts
        pos.extend(e_pos)
        neg.extend(e_neg)
        if experience.min_years is not None and experience.min_years >= STRONG_NEGATIVE_FLOOR:
            warn.append(R.EXPERIENCE_5_PLUS)

    # ── No entry-level evidence from either title or JD ────────────────────
    # A 0-2 / 1-2 / "2 years" requirement establishes an entry-level role.
    # A 2-3 range does not: its ceiling is above entry, so such a posting still
    # needs an entry signal of its own to reach the top band.
    exp_supports_entry = (
        experience is not None
        and experience.min_years is not None
        and experience.min_years <= 2
        and (experience.max_years is None or experience.max_years <= 2)
    )
    if entry_pts == 0 and not exp_supports_entry:
        score += W_NO_ENTRY_EVIDENCE
        warn.append(R.EXPERIENCE_UNKNOWN if experience is None
                    or experience.min_years is None else R.EXPERIENCE_3)

    # ── Location ───────────────────────────────────────────────────────────
    if location.classification == US_REMOTE:
        score += W_US_REMOTE if location.confidence == "high" else W_US_VIA_BOARD
        pos.append(location.reasons[0] if location.reasons else R.US_REMOTE_CONFIRMED)
    elif location.classification == US:
        score += W_US_CONFIRMED
        pos.append(R.US_CONFIRMED)
    elif location.classification == AMBIGUOUS:
        score += W_LOCATION_AMBIGUOUS
        warn.extend(location.reasons or (R.LOCATION_AMBIGUOUS,))

    # ── Freshness (posted_at only) ─────────────────────────────────────────
    hours = _hours_since(posted_at)
    if hours is None:
        score += W_NO_POSTED_DATE
        warn.append(R.NO_POSTED_DATE)
    elif hours <= 6:
        score += W_FRESH_LT_6H
        pos.append(R.FRESH_LT_6H)
    elif hours <= 24:
        score += W_FRESH_LT_24H
        pos.append(R.FRESH_LT_24H)
    elif hours <= 72:
        score += W_FRESH_LT_3D
        pos.append(R.FRESH_LT_3D)
    elif hours > STALE_AFTER_DAYS * 24:
        score += W_STALE
        neg.append(R.STALE_POSTING)

    score = max(0, min(100, score))

    # ── Adjacent occupations are capped at review ──────────────────────────
    if title.role_family in ADJACENT_FAMILIES and score > ADJACENT_CEILING:
        score = ADJACENT_CEILING
        warn.append(R.ROLE_FAMILY_ADJACENT)

    # ── Recall floor for ambiguous-location target roles ───────────────────
    exp_within_ceiling = (
        experience is None
        or experience.min_years is None
        or experience.min_years <= RECALL_FLOOR_EXPERIENCE_MAX
    )
    if (
        score < BAND_REVIEW
        and title.role_family in TARGET_FAMILIES
        and location.classification == AMBIGUOUS
        and exp_within_ceiling
    ):
        score = BAND_REVIEW
        warn.append(R.TARGET_ROLE_RECALL_FLOOR)

    if score >= BAND_APPLY_NOW:
        priority = APPLY_NOW
    elif score >= BAND_STRONG:
        priority = STRONG
    elif score >= BAND_REVIEW:
        priority = REVIEW
    else:
        priority = LOW

    codes = list(dict.fromkeys(list(title.reasons) + list(location.reasons)
                               + list(experience.reasons if experience else ())))

    return OpportunityScore(
        score=score,
        priority=priority,
        positive_reasons=tuple(dict.fromkeys(pos)),
        negative_reasons=tuple(dict.fromkeys(neg)),
        warnings=tuple(dict.fromkeys(warn)),
        reason_codes=tuple(codes),
    )
