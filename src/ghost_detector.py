"""Ghost / fake job posting detector.

Many listings on Greenhouse/Lever are 'ghost jobs' — posted to build a
pipeline, never to be filled.  This module scores each job against a set
of cheap heuristic signals and returns a level:

  ""           — clean, no red flags
  "caution"    — 1-2 signals, worth a quick sanity check
  "suspicious" — 3+ signals, strongly consider skipping

Zero external dependencies — pure Python + re.
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .sources.base import Job

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Signal patterns
# ---------------------------------------------------------------------------

# Language that strongly implies a third-party recruiter / staffing post
_AGENCY_RE = re.compile(
    r"\bour\s+client\b"
    r"|\bthird[- ]party\b"
    r"|\bc2h\b|contract[\s-]to[\s-]hire"
    r"|\b(w-?2|1099)\s+(only|position|contractor)"
    r"|\bstaffing\s+(firm|agency|company)\b"
    r"|\btalent\s+acquisition\s+partner\b"
    r"|\bwe\s+are\s+(recruiting|sourcing)\s+on\s+behalf\b",
    re.IGNORECASE,
)

# Phrases that suggest an evergreen/pipeline post
_EVERGREEN_RE = re.compile(
    r"\bbuilding\s+(a\s+)?pipeline\b"
    r"|\bno\s+current\s+openings\b"
    r"|\bfuture\s+(consideration|opportunities)\b"
    r"|\bgeneral\s+application\b"
    r"|\btalent\s+community\b"
    r"|\bwe['']re\s+always\s+(looking|hiring)\b",
    re.IGNORECASE,
)

# Titles that are often used as placeholder / mass-post names
_GENERIC_TITLE_RE = re.compile(
    r"^(data analyst|business analyst|software engineer|"
    r"data engineer|product manager|analyst)\s*$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

@dataclass
class GhostResult:
    level: str = ""                              # "" | "caution" | "suspicious"
    reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ghost_check(job: "Job", db=None) -> GhostResult:
    """Evaluate a single Job for ghost-posting signals.

    Parameters
    ----------
    job : Job
        The job to evaluate (uses .description, .title, .company, .salary).
    db  : Database | None
        Optional — if provided, checks how many times this company+title
        has been re-posted (strong signal of an evergreen listing).

    Returns
    -------
    GhostResult with .level and .reasons populated.
    """
    reasons: list[str] = []
    desc = (job.description or "").strip()
    desc_words = len(desc.split()) if desc else 0

    # ── Signal 1: Missing or very short description ───────────────────────────
    if desc_words == 0:
        reasons.append("No job description — cannot verify role details")
    elif desc_words < 80:
        reasons.append(f"Suspiciously short description ({desc_words} words)")

    # ── Signal 2: Third-party recruiter / agency language ─────────────────────
    if desc and _AGENCY_RE.search(desc):
        reasons.append("Third-party recruiter language detected in JD ('our client', 'c2h', etc.)")

    # ── Signal 3: Evergreen / pipeline language ───────────────────────────────
    if desc and _EVERGREEN_RE.search(desc):
        reasons.append("Evergreen/pipeline posting language detected ('building a pipeline', etc.)")

    # ── Signal 4: Ultra-generic title with no modifier ────────────────────────
    if _GENERIC_TITLE_RE.match(job.title.strip()):
        reasons.append(f"Very generic title '{job.title}' — may be a catch-all posting")

    # ── Signal 5: Re-posted — same company + similar title seen before ─────────
    if db is not None:
        try:
            count = db.count_company_posts(job.company, job.title)
            if count >= 3:
                reasons.append(
                    f"This company has re-posted a similar '{job.title}' role {count}× — likely evergreen"
                )
            elif count >= 2:
                reasons.append(f"Similar role seen {count}× from {job.company} before")
        except Exception:
            pass

    # ── Determine level ───────────────────────────────────────────────────────
    # Agency language on its own is a hard caution — it means you're applying to
    # a middleman who may not even have access to the actual company.
    has_agency = any("recruiter" in r or "third-party" in r or "'our client'" in r
                     for r in reasons)
    has_evergreen = any("evergreen" in r or "pipeline" in r for r in reasons)

    if len(reasons) >= 3 or (has_agency and len(reasons) >= 2):
        level = "suspicious"
    elif len(reasons) >= 2 or has_agency or has_evergreen:
        level = "caution"
    else:
        level = ""

    if level:
        log.info(
            "Ghost check [%s]: %s — %s | %s",
            level.upper(), job.company, job.title,
            "; ".join(reasons),
        )

    return GhostResult(level=level, reasons=reasons)
