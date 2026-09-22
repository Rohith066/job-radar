"""Live application eligibility — kept deliberately separate from approval.

Two different questions used to be answered by one mechanism:

  APPROVAL VALIDITY             did the user approve this exact document?
  LIVE APPLICATION ELIGIBILITY  should new application work start on this job now?

An approval is historical truth. "The user approved this resume on 2026-09-02"
stays true forever, and nothing here edits it. Eligibility is a statement about
today: a posting outside the live freshness window should not be re-rendered,
re-approved or packaged merely because it once was. Without this separation a
two-week-old SteerBridge posting kept resurfacing as "the resume to approve"
while genuinely new jobs were being discovered.

The freshness rule is not re-implemented here. `_is_too_old` and
`MAX_JOB_AGE_DAYS` belong to the orchestrator, so eligibility tracks the live
alerting and queue policy automatically, including however that policy treats
undated postings.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

FRESH = "FRESH"
STALE_JOB = "STALE_JOB"


@dataclass(frozen=True)
class LiveEligibility:
    status: str                  # FRESH | STALE_JOB
    age_days: Optional[int]      # None when the posting date cannot be parsed
    window_days: int
    dated: bool
    posted: str = ""

    @property
    def eligible(self) -> bool:
        return self.status == FRESH

    def describe(self) -> str:
        if self.age_days is None:
            age = "posting date unknown"
        else:
            age = f"age {self.age_days} day{'' if self.age_days == 1 else 's'}"
        if self.eligible:
            return f"FRESH — {age}"
        return f"STALE — {age} (live application window: {self.window_days} days)"


def live_eligibility(posted: str, observed_at=None) -> LiveEligibility:
    """Whether new application work may start on a job posted at `posted`.

    `observed_at` is the stored row's `last_seen`: a relative posted string
    ("Posted Today") is true as of when it was scraped, not as of now.
    """
    # Imported here: src.main imports this package, so a module-level import
    # would be circular.
    from ..main import MAX_JOB_AGE_DAYS, _is_too_old, _parse_posted

    posted = posted or ""
    dt = _parse_posted(posted, observed_at)
    age = None
    if dt is not None:
        age = max(0, int((datetime.now(timezone.utc) - dt).total_seconds() // 86400))
    status = STALE_JOB if _is_too_old(posted, observed_at=observed_at) else FRESH
    return LiveEligibility(status, age, MAX_JOB_AGE_DAYS, dt is not None, posted)
