"""Base class and shared data types for all job sources."""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

US_STATE_ABBRS = frozenset({
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la",
    "me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
    "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc",
})


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


def make_location(parts: list[Optional[str]]) -> str:
    clean = [str(p).strip() for p in parts if p and str(p).strip()]
    return ", ".join(clean) if clean else "Unknown Location"


def is_us_location(location: str) -> bool:
    """Return True if the location string is plausibly US-based."""
    loc = (location or "").strip().lower()
    if not loc or loc == "unknown location":
        return False
    if "united states" in loc or "u.s." in loc:
        return True
    if re.search(r"\busa\b", loc):
        return True
    if re.search(r"\bus\b", loc):
        return True
    # Accept any remote job — sources already filter to US at the API level
    if "remote" in loc:
        return True
    if "washington, dc" in loc or "district of columbia" in loc:
        return True
    # City, State abbreviation — e.g. "Seattle, WA"
    m = re.search(r",\s*([a-z]{2})(\b|[^a-z])", loc)
    if m and m.group(1) in US_STATE_ABBRS:
        return True
    return False


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
