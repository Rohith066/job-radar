"""Job title classification — thin adapter over `src.screening.titles`.

The tier lists that used to live here matched with plain substring `in`, which
is why `Software Engineering Manager` read as a Software Engineer role and
`Data Engineer IV` scored the same 92 as `Data Engineer I`. The real logic now
lives in `src/screening/titles.py`, which matches on token boundaries and
treats seniority as a veto rather than a score cap.

This module keeps `classify()` and `is_match()` with unchanged signatures so
all nineteen source adapters in `src/sources/` continue to work untouched.

Label mapping:
  YES   -> "yes"    alert
  MAYBE -> "maybe"  review
  NO    -> "no"     suppressed

Scores are a coarse proxy retained for backward compatibility with the DB
column, the ML feature vector, and `fit_rank_key`. The authoritative ranking
signal is now the opportunity score in `src/screening/scoring.py`, which is
computed once per run in `_dispatch_results` and cannot be undone by the
downstream bonuses that used to lift senior roles back over the threshold.
"""
from __future__ import annotations

from dataclasses import dataclass

from .screening.titles import (
    TitleAnalysis,
    analyze_title,
    normalize_title,
    TARGET_FAMILIES,
    SECONDARY_FAMILIES,
    UNRELATED_FAMILIES,
)

__all__ = [
    "ClassifyResult", "classify", "is_match", "analyze_title",
    "TARGET_FAMILIES", "SECONDARY_FAMILIES", "UNRELATED_FAMILIES",
]

# Proxy scores per (classification, seniority). Chosen so the historical
# thresholds in main.py — yes >= 70, maybe >= 40 — keep producing the same
# label the analyzer already decided.
_SCORE_ENTRY_TARGET     = 95
_SCORE_ENTRY_SECONDARY  = 85
_SCORE_TARGET           = 78
_SCORE_SECONDARY        = 62
_SCORE_AMBIGUOUS        = 50
_SCORE_REJECT           = 0


@dataclass
class ClassifyResult:
    score: int   # 0-100
    label: str   # "yes" | "maybe" | "no"
    track: str   # "de" | "ai" | "analyst" | "other"


def _score_for(t: TitleAnalysis) -> int:
    if t.classification == "NO":
        return _SCORE_REJECT
    target = t.role_family in TARGET_FAMILIES
    if t.seniority == "entry":
        return _SCORE_ENTRY_TARGET if target else _SCORE_ENTRY_SECONDARY
    if t.classification == "YES":
        return _SCORE_TARGET if target else _SCORE_SECONDARY
    return _SCORE_SECONDARY if t.role_family in SECONDARY_FAMILIES else _SCORE_AMBIGUOUS


def classify(title: str) -> ClassifyResult:
    """Score and label a job title.

    Returns the resume track so the email can show which resume to send.
    """
    t = analyze_title(title)
    label = {"YES": "yes", "MAYBE": "maybe", "NO": "no"}[t.classification]
    track = t.track if t.classification != "NO" else "other"
    return ClassifyResult(score=_score_for(t), label=label, track=track)


def is_match(title: str) -> bool:
    return classify(title).label in ("yes", "maybe")
