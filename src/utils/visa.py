"""Visa sponsorship detection — critical for H1B candidates.

Scans a job description for explicit statements about visa sponsorship and
returns one of:

    "available"  — JD explicitly offers / mentions sponsorship  → boost
    "none"       — JD explicitly refuses sponsorship             → warn / down-rank
    ""           — no statement found                            → unknown, neutral

Pure regex, zero dependencies, zero cost.
"""
from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Negative signals — JD refuses sponsorship (strongest signal, checked first)
# ---------------------------------------------------------------------------
_NO_SPONSOR_RE = re.compile(
    r"\b(?:"
    r"no\s+(?:visa\s+)?sponsorship"
    r"|not?\s+(?:able\s+to\s+|in\s+a\s+position\s+to\s+)?sponsor"
    r"|unable\s+to\s+sponsor"
    r"|will\s+not\s+sponsor"
    r"|does\s+not\s+(?:offer|provide)\s+(?:visa\s+)?sponsor"
    r"|cannot\s+(?:offer|provide)\s+(?:visa\s+)?sponsor"
    r"|without\s+(?:the\s+need\s+for\s+)?(?:visa\s+|current\s+or\s+future\s+)?sponsorship"
    r"|sponsorship\s+is\s+not\s+(?:available|provided|offered)"
    r"|not\s+(?:provide|offer)\s+(?:visa\s+)?sponsorship"
    r"|are\s+(?:not\s+able|unable)\s+to\s+provide\s+(?:visa\s+)?sponsorship"
    r")",
    re.IGNORECASE,
)

# "must be authorized to work ... without sponsorship" pattern
_AUTH_NO_SPONSOR_RE = re.compile(
    r"authoriz(?:ed|ation)\s+to\s+work[^.]{0,80}?"
    r"(?:without\s+(?:visa\s+)?sponsorship|do(?:es)?\s+not\s+require\s+sponsorship|"
    r"now\s+or\s+in\s+the\s+future)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Positive signals — JD offers sponsorship
# ---------------------------------------------------------------------------
_YES_SPONSOR_RE = re.compile(
    r"\b(?:"
    r"(?:visa\s+)?sponsorship\s+(?:is\s+)?(?:available|provided|offered|possible)"
    r"|we\s+(?:will\s+|can\s+|do\s+)?sponsor"
    r"|will\s+(?:provide|offer)\s+(?:visa\s+)?sponsorship"
    r"|open\s+to\s+(?:visa\s+)?sponsorship"
    r"|h-?1-?b\s+sponsor"
    r"|able\s+to\s+sponsor"
    r"|offer\s+(?:visa\s+)?sponsorship"
    r"|sponsor\s+(?:work\s+)?visas?"
    r"|green\s+card\s+sponsor"
    r")",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Citizenship / work-authorization gates.
#
# SEPARATE from security clearance and from sponsorship — and the trap in
# federal-contract hunting: a role can require zero clearance and still be
# closed to non-citizens. Measured against this repo's own corpus, 43% of
# federal-contractor roles require citizenship while 73% require clearance;
# 13% require citizenship with no clearance mentioned at all.
# ---------------------------------------------------------------------------
_CITIZENSHIP_RE = re.compile(
    r"\b(?:"
    r"u\.?\s?s\.?\s+citizen(?:ship)?"
    r"|united\s+states\s+citizen"
    r"|must\s+be\s+a\s+(?:u\.?s\.?\s+)?citizen"
    r"|citizenship\s+(?:is\s+)?required"
    r"|only\s+u\.?s\.?\s+citizens"
    r"|sole(?:ly)?\s+u\.?s\.?\s+citizen"
    r"|u\.?s\.?\s+persons?\s+(?:only|required)"
    r"|green\s+card\s+holder"
    r"|permanent\s+resident\s+(?:status\s+)?(?:is\s+)?required"
    r"|itar"
    r"|export\s+control"
    r")",
    re.IGNORECASE,
)

_CLEARANCE_RE = re.compile(
    r"\b(?:"
    r"security\s+clearance|clearance\s+(?:is\s+)?required|active\s+clearance"
    r"|secret\s+clearance|top\s+secret|ts/sci|\bsci\b|polygraph"
    r"|public\s+trust|dod\s+clearance|must\s+be\s+cleared|cleared\s+position"
    r")",
    re.IGNORECASE,
)


def detect_citizenship_requirement(text: str) -> tuple[bool, str]:
    """Return (requires_citizenship, matched_phrase).

    Detects US-citizenship / permanent-residency / ITAR gates that block a
    visa-holding candidate regardless of whether a clearance is mentioned.
    """
    if not text:
        return False, ""
    m = _CITIZENSHIP_RE.search(text)
    return (True, m.group(0).strip()) if m else (False, "")


def detect_clearance_requirement(text: str) -> tuple[bool, str]:
    """Return (requires_clearance, matched_phrase)."""
    if not text:
        return False, ""
    m = _CLEARANCE_RE.search(text)
    return (True, m.group(0).strip()) if m else (False, "")


def is_work_authorization_blocked(text: str) -> tuple[bool, str]:
    """True when the JD is closed to a candidate who needs sponsorship.

    Blocks on ANY of: explicit no-sponsorship, citizenship/PR/ITAR gate, or a
    security-clearance requirement. This is the single check the pipeline uses
    to decide whether a role is realistically applicable.
    """
    status, phrase = detect_visa_sponsorship(text)
    if status == "none":
        return True, f"no sponsorship: {phrase}"
    blocked, phrase = detect_citizenship_requirement(text)
    if blocked:
        return True, f"citizenship/ITAR: {phrase}"
    blocked, phrase = detect_clearance_requirement(text)
    if blocked:
        return True, f"clearance: {phrase}"
    return False, ""


def detect_visa_sponsorship(text: str) -> tuple[str, str]:
    """Return (status, matched_phrase).

    status: "available" | "none" | "" (unknown)
    Negative statements take priority — a JD that says "we sponsor for some
    roles but not this one" should be treated conservatively as "none".
    """
    if not text:
        return "", ""

    # Negative first — strongest, most decision-relevant signal
    m = _NO_SPONSOR_RE.search(text)
    if m:
        return "none", m.group(0).strip()
    m = _AUTH_NO_SPONSOR_RE.search(text)
    if m:
        return "none", m.group(0).strip()[:80]

    # Positive
    m = _YES_SPONSOR_RE.search(text)
    if m:
        return "available", m.group(0).strip()

    return "", ""
