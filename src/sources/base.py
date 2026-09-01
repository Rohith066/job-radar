"""Base class and shared data types for all job sources."""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field  # noqa: F401 — field used by Job
from typing import Optional

# Location vocabulary now lives in src/screening/locations.py — the sets that
# used to sit here matched as substrings, which rejected "Milwaukee, WI" (for
# "uk") and "Indianapolis, IN" (for "india").

@dataclass
class Job:
    key: str
    source: str
    company: str
    title: str
    location: str
    url: str
    posted: str = ""
    score: int = 0
    label: str = "no"
    salary: str = ""        # e.g. "$80,000 – $120,000/yr" — empty when not available
    work_type: str = ""     # "Remote" | "Hybrid" | "Onsite" | "" when unknown
    description: str = ""  # raw JD text (HTML stripped) — populated when available
    resume_match: int = 0  # 0-100 resume-vs-JD match score; 0 = not yet scored
    experience_ok: bool = True  # False when JD requires more years than MAX_EXPERIENCE_YEARS
    # ── Ephemeral fields — computed each run, NOT persisted to DB ──────────────
    top_bullets: list = field(default_factory=list)  # top 3 resume bullets matching this JD
    linkedin_dm: str = ""    # pre-written LinkedIn outreach message
    ghost_level: str = ""    # "" | "caution" | "suspicious"
    ghost_reasons: list = field(default_factory=list)  # reasons for ghost flag
    resume_track: str = ""   # "de" | "ai" | "" — which resume matched best
    visa_status: str = ""    # "available" | "none" | "" — sponsorship signal from JD
    visa_phrase: str = ""    # matched JD phrase for the visa status
    # Surfaced in the email so the reader can see WHY a job ranks where it does.
    # Both are computed by the matcher already; these fields only carry them.
    matched_skills: list = field(default_factory=list)   # JD skills the resume satisfies
    missing_required: list = field(default_factory=list)  # unsatisfied REQUIRED skills
    # ── Phase 1 screening — computed in _dispatch_results, persisted to DB ────
    country_focus: str = ""      # board metadata from the CSV: "US" | "Global" | ""
    opportunity_score: int = 0   # 0-100 deterministic entry-level score
    priority: str = ""           # APPLY_NOW | STRONG | REVIEW | LOW | REJECT
    location_class: str = ""     # US | US_REMOTE | NON_US | AMBIGUOUS
    seniority: str = ""          # entry | ambiguous | unspecified | senior | ...
    role_family: str = ""        # e.g. "software_engineering"
    experience_min: int | None = None
    experience_max: int | None = None
    classification_reasons: list = field(default_factory=list)  # reason codes


def make_location(parts: list[Optional[str]]) -> str:
    clean = [str(p).strip() for p in parts if p and str(p).strip()]
    return ", ".join(clean) if clean else "Unknown Location"


def _norm_for_hash(s: str) -> str:
    """Normalize a string for the dedup hash: lowercase, strip punctuation,
    collapse whitespace. So 'Google LLC' and 'google, llc.' hash the same."""
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)   # drop punctuation
    s = re.sub(r"\s+", " ", s).strip()
    # Drop common company suffixes so 'Stripe' == 'Stripe Inc'
    s = re.sub(r"\b(inc|llc|ltd|corp|corporation|co|gmbh|limited)\b", "", s).strip()
    return re.sub(r"\s+", " ", s).strip()


def _first_city(location: str) -> str:
    """Extract just the city portion of a location for hashing —
    'Seattle, WA, USA' → 'seattle'. Remote variants collapse to 'remote'."""
    loc = (location or "").lower()
    if "remote" in loc:
        return "remote"
    return _norm_for_hash(loc.split(",")[0]) if loc else ""


def job_fingerprint(company: str, title: str, location: str) -> str:
    """Deterministic dedup id: sha256(company|title|city)[:16].

    Inspired by JobScout's job_id scheme — gives a stable hash that catches
    the same role posted across multiple sources (e.g. company site + Remotive).
    """
    import hashlib
    raw = f"{_norm_for_hash(company)}|{_norm_for_hash(title)}|{_first_city(location)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def is_us_location(location: str, country_focus: str = "") -> bool:
    """Return True if the location could plausibly be US-based.

    Delegates to `screening.locations.analyze_location`. The predicate is
    deliberately "not confirmed non-US" rather than "confirmed US": a bare
    "Remote" or an unrecognised place name is ambiguous, and dropping it costs
    an application the owner might have wanted. Callers that need the finer
    verdict should use `analyze_location` directly and read `.classification`.

    Retained with this name and return type because nineteen source adapters
    and `_dispatch_results` call it.
    """
    from ..screening.locations import analyze_location
    return analyze_location(location, country_focus).is_plausibly_us


class BaseSource(ABC):
    """Abstract base class every job source must implement."""

    name: str  # unique source identifier (e.g. "microsoft")

    @abstractmethod
    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        """Fetch jobs from the source and return a list of Job objects.

        Args:
            seen_keys: Set of job keys already in the database (for early-exit).
            timeout: HTTP timeout in seconds.

        Returns:
            All retrieved jobs (scored + labelled). Deduplication is done by
            the orchestrator — sources do not need to filter against seen_keys.
        """
        ...
