"""Phase 2 — resume-aware prioritization and application workflow.

Phase 1 answers "is this job eligible?". Phase 2 answers "which eligible job
should I spend the next twenty minutes applying to?". The two verdicts are kept
as separate numbers end to end:

    screening_score        Phase 1, src/screening/scoring.py
    resume_fit_score       Phase 2, fit.py
    application_priority   Phase 2, priority.py — a bounded blend of the two

Nothing here re-implements skill matching. The hybrid matcher and the JD
requirement parser in src/matching/ already do that, and their thresholds are
corpus-calibrated; this package consumes their output.
"""
from .fit import FitResult, analyze_fit
from .priority import (
    ApplicationPriority, application_priority,
    APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW,
)
from .queue import ApplicationQueue, STATUSES, TERMINAL_STATUSES

__all__ = [
    "FitResult", "analyze_fit",
    "ApplicationPriority", "application_priority",
    "APPLY_FIRST", "HIGH", "MEDIUM", "REVIEW", "LOW",
    "ApplicationQueue", "STATUSES", "TERMINAL_STATUSES",
]
