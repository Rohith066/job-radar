"""Deterministic, explainable job screening — Phase 1.

Four pure-function layers, each independently unit-testable:

    titles.analyze_title(title)                  -> TitleAnalysis
    locations.analyze_location(loc, focus)       -> LocationAnalysis
    experience.analyze_experience(text)          -> ExperienceAnalysis
    scoring.score_job(...)                       -> OpportunityScore

Nothing here performs I/O, reads config, or depends on the database, so the
whole screening decision for any job is reproducible from its inputs alone.
Every verdict carries structured reason codes (see `reasons.py`), which is what
lets the email explain *why* a job ranks where it does.
"""
from .reasons import describe, describe_all
from .titles import TitleAnalysis, analyze_title
from .locations import LocationAnalysis, analyze_location
from .experience import ExperienceAnalysis, analyze_experience
from .scoring import OpportunityScore, score_job

__all__ = [
    "TitleAnalysis", "analyze_title",
    "LocationAnalysis", "analyze_location",
    "ExperienceAnalysis", "analyze_experience",
    "OpportunityScore", "score_job",
    "describe", "describe_all",
]
