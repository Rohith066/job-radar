"""Base class and shared data types for all job sources."""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field  # noqa: F401 — field used by Job
from typing import Optional

US_STATE_ABBRS = frozenset({
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la",
    "me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
    "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy","dc",
})

# Non-US country names / regions — any location containing these is rejected
# even if it also contains the word "remote"
_NON_US_COUNTRIES = frozenset({
    "argentina", "colombia", "brazil", "brasil", "mexico", "méxico",
    "canada", "united kingdom", "uk", "england", "scotland", "ireland",
    "australia", "india", "germany", "france", "spain", "netherlands",
    "poland", "portugal", "italy", "sweden", "norway", "denmark",
    "singapore", "japan", "china", "hong kong", "new zealand",
    "south africa", "nigeria", "kenya", "philippines", "indonesia",
    "pakistan", "bangladesh", "sri lanka", "ukraine", "russia",
    "israel", "turkey", "egypt", "uae", "dubai", "saudi arabia",
    "latin america", "latam", "south america", "europe", "emea", "apac",
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


def is_us_location(location: str) -> bool:
    """Return True if the location string is plausibly US-based.

    Hard-blocks non-US countries even when "remote" appears in the string
    (e.g. "Remote - Argentina" must NOT pass).
    """
    loc = (location or "").strip().lower()
    if not loc or loc == "unknown location":
        return False

    # Hard-block any location containing a known non-US country name
    for country in _NON_US_COUNTRIES:
        if country in loc:
            return False

    if "united states" in loc or "u.s." in loc:
        return True
    if re.search(r"\busa\b", loc):
        return True
    if re.search(r"\bus\b", loc):
        return True
    # Accept remote jobs that aren't tied to a non-US country (checked above)
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
