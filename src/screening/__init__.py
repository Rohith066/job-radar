"""Deterministic, explainable job screening — Phase 1.

Five pure-function layers, each independently unit-testable:

    titles.analyze_title(title)                  -> TitleAnalysis
    locations.analyze_location(loc, focus)       -> LocationAnalysis
    experience.analyze_experience(text)          -> ExperienceAnalysis
    relevance.analyze_relevance(title, jd)       -> RelevanceResult
    scoring.score_job(...)                       -> OpportunityScore

`relevance` runs before fit anywhere it is consulted: whether a job is the
candidate's kind of work is a different question from how well the resume
matches it, and letting similarity answer the first question is what put
`Plumbing Engineer I` on the board at STRONG.

Nothing here performs I/O, reads config, or depends on the database, so the
whole screening decision for any job is reproducible from its inputs alone.
Every verdict carries structured reason codes (see `reasons.py`), which is what
lets the email explain *why* a job ranks where it does.
"""
from .reasons import describe, describe_all
from .titles import TitleAnalysis, analyze_title
from .locations import LocationAnalysis, analyze_location
from .experience import ExperienceAnalysis, analyze_experience
from .relevance import (
    RelevanceResult, analyze_relevance, RELEVANCE_STATES,
    TARGET, ADJACENT, AMBIGUOUS, OUT_OF_SCOPE,
)
from .scoring import OpportunityScore, score_job

__all__ = [
    "TitleAnalysis", "analyze_title",
    "LocationAnalysis", "analyze_location",
    "ExperienceAnalysis", "analyze_experience",
    "RelevanceResult", "analyze_relevance", "RELEVANCE_STATES",
    "TARGET", "ADJACENT", "AMBIGUOUS", "OUT_OF_SCOPE",
    "OpportunityScore", "score_job",
    "describe", "describe_all",
]
