"""Provenance for a fit score — which candidate configuration produced it.

Resumes and the profile change over time. Without a marker, a fit score of 84
recorded today is indistinguishable from one recomputed months later against a
different resume, which would quietly corrupt any future outcome analysis.

Stores a short deterministic hash, never resume contents.
"""
from __future__ import annotations

import hashlib
import json
from typing import Optional

from ..profile import PROFILE, SKILLS_STRONG, SKILLS_MODERATE

_cached: Optional[str] = None


def profile_version(resume_text: str = "", *, use_cache: bool = True) -> str:
    """Stable 12-char identifier for the profile + resume used for scoring.

    Deterministic: the same profile and resume always yield the same value, and
    any edit to either yields a different one.
    """
    global _cached
    if use_cache and _cached is not None and not resume_text:
        return _cached

    payload = json.dumps(
        {
            "experience_years": PROFILE.get("experience_years"),
            "education": PROFILE.get("education"),
            "target_roles": sorted(PROFILE.get("target_roles", [])),
            "skills_strong": sorted(SKILLS_STRONG),
            "skills_moderate": sorted(SKILLS_MODERATE),
            # Length and digest only — the resume text itself is never stored.
            "resume_len": len(resume_text or ""),
            "resume_sha": hashlib.sha256((resume_text or "").encode("utf-8")).hexdigest()[:16],
        },
        sort_keys=True,
    )
    version = "p1-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:9]
    if use_cache and not resume_text:
        _cached = version
    return version
