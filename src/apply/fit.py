"""Resume-vs-JD fit analysis.

Consumes the existing machinery rather than duplicating it:

  * `src/matching/jd_parser.parse_jd` supplies the required / preferred /
    responsibility split, which it derives from JD section headings.
  * `src/matching/hybrid.match` (via `resume_matcher`) supplies whether the
    resume satisfies each skill, including the family veto that stops a sibling
    technology counting as a match.

The judgement this module adds is how much each gap should *cost*. That is
deliberately asymmetric: entry-level postings routinely list wish-list
technologies, so a missing preferred skill is nearly free while a missing core
required skill is not. Nothing here can reject a job — rejection is Phase 1's
job, and fit only re-orders what Phase 1 already allowed through.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from ..matching import ontology
from ..matching.jd_parser import parse_jd
from ..profile import PROFILE

# ---------------------------------------------------------------------------
# Weights — the complete set. Every point is attributable to one of these.
# ---------------------------------------------------------------------------
BASE_FIT = 55                  # neutral starting point for an eligible job

W_REQUIRED_MATCH = 9           # per satisfied required skill
W_PREFERRED_MATCH = 3          # per satisfied preferred skill
W_OTHER_MATCH = 1              # per satisfied unclassified skill

W_REQUIRED_MISS = -7           # per unsatisfied required skill
W_PREFERRED_MISS = -1          # per unsatisfied preferred skill — nearly free
W_OTHER_MISS = 0               # unclassified gaps cost nothing

# Caps stop a JD with a 30-item wish list from dominating the score in either
# direction. Without them, long postings score differently from short ones for
# reasons that have nothing to do with fit.
CAP_REQUIRED_MATCH = 36
CAP_PREFERRED_MATCH = 12
CAP_OTHER_MATCH = 6
CAP_REQUIRED_MISS = -28
CAP_PREFERRED_MISS = -6

W_NO_JD = -5                   # no description to judge; mild, not punitive

# Experience compatibility, relative to the owner's profile years.
W_EXP_IDEAL = 12               # requirement at or below profile years
W_EXP_STRETCH = 4              # 1 year above profile — very reachable
W_EXP_REACH = -6               # 2 years above profile
W_EXP_UNKNOWN = 0

# Education, when the JD states a hard degree requirement.
W_EDU_MET = 4
W_EDU_PHD_REQUIRED = -12       # owner holds an M.S.; a hard PhD gate is real

W_WORK_AUTH_OK = 3             # JD explicitly offers sponsorship
W_WORK_AUTH_RISK = -10         # JD signals no sponsorship (Phase 1 drops the
                               # explicit blockers; this catches softer wording)

# Bands. Descriptive only — priority.py owns the actionable thresholds.
FIT_EXCEPTIONAL = 85
FIT_STRONG = 70
FIT_PLAUSIBLE = 50

_PHD_RE = re.compile(r"\bph\.?\s?d\b|\bdoctorate\b", re.IGNORECASE)
_PHD_SOFT_RE = re.compile(r"ph\.?\s?d\s+(?:is\s+)?(?:a\s+)?(?:plus|preferred|nice)", re.IGNORECASE)
_MS_RE = re.compile(r"\bmaster'?s?\b|\bm\.?s\.?\b|\bmsc\b", re.IGNORECASE)
_BS_RE = re.compile(r"\bbachelor'?s?\b|\bb\.?s\.?\b|\bbsc\b|\bundergraduate\s+degree\b", re.IGNORECASE)
_NO_SPONSOR_RE = re.compile(
    r"(?:not|unable\s+to|cannot|will\s+not|do\s+not)\s+(?:be\s+)?(?:able\s+to\s+)?sponsor"
    r"|no\s+(?:visa\s+)?sponsorship"
    r"|without\s+(?:current\s+or\s+future\s+)?sponsorship",
    re.IGNORECASE,
)
_SPONSOR_RE = re.compile(r"\b(?:will\s+)?sponsor(?:ship)?\s+(?:is\s+)?(?:available|offered|provided)"
                         r"|we\s+sponsor\b|visa\s+sponsorship\s+available", re.IGNORECASE)


@dataclass(frozen=True)
class FitResult:
    resume_fit_score: int
    band: str                                   # exceptional | strong | plausible | weak
    matched_required_skills: tuple[str, ...] = ()
    missing_required_skills: tuple[str, ...] = ()
    matched_preferred_skills: tuple[str, ...] = ()
    missing_preferred_skills: tuple[str, ...] = ()
    matched_other_skills: tuple[str, ...] = ()
    experience_fit: str = "unknown"             # ideal | stretch | reach | unknown
    role_fit: str = "unknown"                   # target | secondary | adjacent | other
    education_fit: str = "unknown"              # met | exceeded | phd_required | unknown
    work_auth_fit: str = "unknown"              # sponsors | silent | risk
    positive_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    has_jd: bool = False
    matcher_score: int = 0                      # the hybrid matcher's own number

    @property
    def is_plausible(self) -> bool:
        return self.resume_fit_score >= FIT_PLAUSIBLE


def _clamp(value: int, cap: int) -> int:
    return min(value, cap) if cap >= 0 else max(value, cap)


def _display(canonical: str) -> str:
    try:
        return ontology.display_name(canonical)
    except Exception:
        return canonical


def _education_fit(jd_text: str) -> tuple[str, int, Optional[str]]:
    """The owner holds an M.S., so only a hard PhD gate is a genuine barrier."""
    if not jd_text:
        return "unknown", 0, None
    if _PHD_RE.search(jd_text) and not _PHD_SOFT_RE.search(jd_text):
        # Most postings that name a PhD also accept a Master's ("MS or PhD",
        # "advanced degree"). Only treat it as a gate when no alternative is
        # offered, otherwise every research-flavoured JD looks closed.
        if _MS_RE.search(jd_text) or _BS_RE.search(jd_text):
            return "met", W_EDU_MET, None
        return "phd_required", W_EDU_PHD_REQUIRED, "PhD appears to be required"
    if _MS_RE.search(jd_text) or _BS_RE.search(jd_text):
        return "met", W_EDU_MET, None
    return "unknown", 0, None


def _work_auth_fit(jd_text: str) -> tuple[str, int, Optional[str]]:
    if not jd_text:
        return "unknown", 0, None
    if _NO_SPONSOR_RE.search(jd_text):
        return "risk", W_WORK_AUTH_RISK, "JD suggests sponsorship is not offered"
    if _SPONSOR_RE.search(jd_text):
        return "sponsors", W_WORK_AUTH_OK, None
    return "silent", 0, None


def _experience_fit(min_years: Optional[int], profile_years: int) -> tuple[str, int]:
    if min_years is None:
        return "unknown", W_EXP_UNKNOWN
    delta = min_years - profile_years
    if delta <= 0:
        return "ideal", W_EXP_IDEAL
    if delta == 1:
        return "stretch", W_EXP_STRETCH
    return "reach", W_EXP_REACH


def analyze_fit(
    *,
    jd_text: str,
    matched_canonicals: set[str],
    role_family: str = "",
    experience_min: Optional[int] = None,
    matcher_score: int = 0,
    profile_years: Optional[int] = None,
) -> FitResult:
    """Score how well the resume fits one JD.

    `matched_canonicals` is the set of canonical skills the hybrid matcher
    confirmed the resume satisfies — passing it in keeps this function pure and
    lets the caller decide whether to run the expensive matcher at all.
    """
    profile_years = PROFILE.get("experience_years", 3) if profile_years is None else profile_years
    positives: list[str] = []
    warnings: list[str] = []

    if not jd_text:
        exp_fit, exp_pts = _experience_fit(experience_min, profile_years)
        score = BASE_FIT + W_NO_JD + exp_pts
        warnings.append("No job description available to assess fit")
        return FitResult(
            resume_fit_score=max(0, min(100, score)), band="unknown",
            experience_fit=exp_fit, role_fit=role_family or "unknown",
            warnings=tuple(warnings), has_jd=False, matcher_score=matcher_score,
        )

    parsed = parse_jd(jd_text)
    kinds: dict[str, str] = {}
    for r in parsed.requirements:
        # A skill named in both a required and a preferred section is treated as
        # required — the stricter reading is the one that gates the application.
        if kinds.get(r.canonical) != "required":
            kinds[r.canonical] = r.kind

    req_hit, req_miss, pref_hit, pref_miss, other_hit = [], [], [], [], []
    for canonical, kind in kinds.items():
        satisfied = canonical in matched_canonicals
        if kind == "required":
            (req_hit if satisfied else req_miss).append(_display(canonical))
        elif kind == "preferred":
            (pref_hit if satisfied else pref_miss).append(_display(canonical))
        elif kind != "responsibility" and satisfied:
            other_hit.append(_display(canonical))

    score = BASE_FIT
    score += _clamp(len(req_hit) * W_REQUIRED_MATCH, CAP_REQUIRED_MATCH)
    score += _clamp(len(pref_hit) * W_PREFERRED_MATCH, CAP_PREFERRED_MATCH)
    score += _clamp(len(other_hit) * W_OTHER_MATCH, CAP_OTHER_MATCH)
    score += _clamp(len(req_miss) * W_REQUIRED_MISS, CAP_REQUIRED_MISS)
    score += _clamp(len(pref_miss) * W_PREFERRED_MISS, CAP_PREFERRED_MISS)

    if req_hit:
        positives.append(f"{len(req_hit)} required skill(s) matched: {', '.join(req_hit[:5])}")
    if pref_hit:
        positives.append(f"{len(pref_hit)} preferred skill(s) matched")
    if req_miss:
        warnings.append(f"Missing required: {', '.join(req_miss[:4])}")
    if pref_miss:
        warnings.append(f"Missing preferred: {', '.join(pref_miss[:4])}")

    stated_years = experience_min if experience_min is not None else parsed.min_years
    exp_fit, exp_pts = _experience_fit(stated_years, profile_years)
    score += exp_pts
    if exp_fit == "ideal":
        positives.append(f"Experience requirement within {profile_years} years")
    elif exp_fit == "reach":
        warnings.append(f"Asks {stated_years} years vs {profile_years} on the resume")

    edu_fit, edu_pts, edu_warn = _education_fit(jd_text)
    score += edu_pts
    if edu_warn:
        warnings.append(edu_warn)

    auth_fit, auth_pts, auth_warn = _work_auth_fit(jd_text)
    score += auth_pts
    if auth_warn:
        warnings.append(auth_warn)
    elif auth_fit == "sponsors":
        positives.append("JD mentions visa sponsorship")

    score = max(0, min(100, score))
    band = ("exceptional" if score >= FIT_EXCEPTIONAL else
            "strong" if score >= FIT_STRONG else
            "plausible" if score >= FIT_PLAUSIBLE else "weak")

    return FitResult(
        resume_fit_score=score,
        band=band,
        matched_required_skills=tuple(sorted(req_hit)),
        missing_required_skills=tuple(sorted(req_miss)),
        matched_preferred_skills=tuple(sorted(pref_hit)),
        missing_preferred_skills=tuple(sorted(pref_miss)),
        matched_other_skills=tuple(sorted(other_hit)),
        experience_fit=exp_fit,
        role_fit=role_family or "unknown",
        education_fit=edu_fit,
        work_auth_fit=auth_fit,
        positive_reasons=tuple(positives),
        warnings=tuple(warnings),
        has_jd=True,
        matcher_score=matcher_score,
    )
