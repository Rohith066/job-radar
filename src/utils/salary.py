"""Salary and work-type extraction utilities.

Salary patterns cover the most common formats found in job postings:
  $80,000 - $120,000/year
  $80K - $120K/yr
  $40 - $60/hour
  USD 90,000 – 130,000 annually
  80,000 to 120,000 USD

Work-type is inferred from job title and/or location string:
  "Remote"   — fully remote
  "Hybrid"   — hybrid (mix of in-office and remote)
  "Onsite"   — in-office only
"""
from __future__ import annotations

import re


# ---------------------------------------------------------------------------
# Salary extraction
# ---------------------------------------------------------------------------

# Match dollar amounts with optional K suffix and optional decimals
_AMOUNT = r"\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)(?:\s*[Kk](?:b?ps)?)?"

# Period suffixes
_PERIOD = r"(?:/?\s*(?:yr|year|years|annually|annual|hr|hour|hours|per\s+hour|per\s+year))?"

_SALARY_PATTERNS = [
    # $80,000 – $120,000 /year  |  $80K – $120K   (K suffix included in capture group)
    re.compile(
        r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?[Kk]?|\d+(?:\.\d+)?[Kk]?)"
        r"\s*(?:–|-|to|through)\s*"
        r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?[Kk]?|\d+(?:\.\d+)?[Kk]?)"
        r"(?:\s*/?\s*(?:yr|year|years|annually|annual|hr|hour|hours|per\s+hour|per\s+year))?",
        re.IGNORECASE,
    ),
    # USD 80,000 – 130,000   |  80000 to 120000 USD
    re.compile(
        r"(?:USD|usd)\s*(\d{1,3}(?:,\d{3})*[Kk]?)\s*(?:–|-|to)\s*(\d{1,3}(?:,\d{3})*[Kk]?)"
        r"(?:\s*/?\s*(?:yr|year|annually|annual))?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(\d{1,3}(?:,\d{3})*[Kk]?)\s*(?:–|-|to)\s*(\d{1,3}(?:,\d{3})*[Kk]?)\s*(?:USD|usd)"
        r"(?:\s*/?\s*(?:yr|year|annually|annual))?",
        re.IGNORECASE,
    ),
    # Single value: $95,000/year  |  $95K/yr
    re.compile(
        r"\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?[Kk]?|\d+(?:\.\d+)?[Kk]?)"
        r"\s*/?\s*(?:yr|year|years|annually|annual|hr|hour)",
        re.IGNORECASE,
    ),
]


def _normalise_amount(raw: str) -> str:
    """Convert '80K' → '$80,000', '80,000' → '$80,000', '80.5' → '$80.50'."""
    raw = raw.replace(",", "").strip()
    is_k = raw.lower().endswith("k")
    if is_k:
        raw = raw[:-1]
    try:
        val = float(raw)
    except ValueError:
        return raw
    if is_k:
        val *= 1_000
    if val == int(val):
        return f"${int(val):,}"
    return f"${val:,.2f}"


def _raw_to_num(raw: str) -> float:
    """Parse a raw amount string (may include K suffix) to a float."""
    clean = raw.replace(",", "").strip()
    is_k = clean.lower().endswith("k")
    if is_k:
        clean = clean[:-1]
    try:
        val = float(clean)
    except ValueError:
        return 0.0
    return val * 1_000 if is_k else val


def extract_salary(text: str) -> str:
    """Return a human-readable salary string, or empty string if not found."""
    if not text:
        return ""

    for pat in _SALARY_PATTERNS:
        m = pat.search(text)
        if m:
            groups = m.groups()
            if len(groups) >= 2 and groups[1]:
                lo = _normalise_amount(groups[0])
                hi = _normalise_amount(groups[1])
                lo_val = _raw_to_num(groups[0])
                period = "/hr" if lo_val < 500 else "/yr"
                return f"{lo} – {hi}{period}"
            elif groups:
                amt = _normalise_amount(groups[0])
                lo_val = _raw_to_num(groups[0])
                period = "/hr" if lo_val < 500 else "/yr"
                return f"{amt}{period}"

    return ""


# ---------------------------------------------------------------------------
# Work-type detection
# ---------------------------------------------------------------------------

_REMOTE_PATTERNS = [
    re.compile(r"\bremote\b", re.IGNORECASE),
    re.compile(r"\bwork from home\b", re.IGNORECASE),
    re.compile(r"\bwfh\b", re.IGNORECASE),
    re.compile(r"\bfully remote\b", re.IGNORECASE),
    re.compile(r"\btelework\b", re.IGNORECASE),
    re.compile(r"\bvirtual\b", re.IGNORECASE),
]

_HYBRID_PATTERNS = [
    re.compile(r"\bhybrid\b", re.IGNORECASE),
    re.compile(r"\bhybrid[- ]?remote\b", re.IGNORECASE),
    re.compile(r"\bpartially remote\b", re.IGNORECASE),
    re.compile(r"\bflexible work\b", re.IGNORECASE),
]


def detect_work_type(title: str = "", location: str = "", description: str = "") -> str:
    """Return 'Remote', 'Hybrid', 'Onsite', or '' (unknown).

    Checks title first (most reliable), then location, then description.
    """
    combined = f"{title} {location} {description}".strip()
    if not combined:
        return ""

    # Hybrid is checked before remote because "Hybrid Remote" should → Hybrid
    for pat in _HYBRID_PATTERNS:
        if pat.search(combined):
            return "Hybrid"

    for pat in _REMOTE_PATTERNS:
        if pat.search(combined):
            return "Remote"

    # Explicit onsite signals
    if re.search(r"\bon[- ]?site\b|\bin[- ]?office\b|\bon[- ]?location\b", combined, re.IGNORECASE):
        return "Onsite"

    return ""
