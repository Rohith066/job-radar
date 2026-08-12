"""Tunable knobs for the hybrid resume/JD matcher.

Everything the matcher weights or thresholds lives here so behaviour can be
adjusted without touching matching logic. Values are overridable via the
environment, which keeps CI and local runs configurable without code edits.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _f(env: str, default: float) -> float:
    try:
        return float(os.environ.get(env, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class MatchConfig:
    """Scoring weights and thresholds for hybrid matching."""

    # ── Credit awarded per match classification (0.0 - 1.0) ──────────────────
    # EXACT/EQUIVALENT are full credit: the resume demonstrably has the skill.
    # SEMANTIC is partial: the concept is evidenced but not stated in the JD's
    # own vocabulary, so it is weaker proof to a human screener and to an ATS.
    # RELATED_ONLY is deliberately zero — see ontology.same_family().
    credit_exact: float = field(default_factory=lambda: _f("MATCH_CREDIT_EXACT", 1.0))
    credit_equivalent: float = field(default_factory=lambda: _f("MATCH_CREDIT_EQUIVALENT", 1.0))
    credit_semantic: float = field(default_factory=lambda: _f("MATCH_CREDIT_SEMANTIC", 0.6))
    credit_related: float = field(default_factory=lambda: _f("MATCH_CREDIT_RELATED", 0.0))

    # ── Requirement weighting ────────────────────────────────────────────────
    weight_required: float = field(default_factory=lambda: _f("MATCH_WEIGHT_REQUIRED", 2.0))
    weight_preferred: float = field(default_factory=lambda: _f("MATCH_WEIGHT_PREFERRED", 1.0))

    # ── Final blend ──────────────────────────────────────────────────────────
    weight_skill_fit: float = field(default_factory=lambda: _f("MATCH_WEIGHT_SKILL", 0.65))
    weight_doc_sim: float = field(default_factory=lambda: _f("MATCH_WEIGHT_DOC", 0.35))

    # ── Guardrail: semantic similarity must not mask a required-skill miss ───
    # Each unsatisfied REQUIRED skill lowers the achievable ceiling.
    required_miss_penalty: float = field(default_factory=lambda: _f("MATCH_REQUIRED_MISS_PENALTY", 12.0))

    # ── Semantic thresholds (cosine over L2-normalised MiniLM embeddings) ────
    # Accept a semantic match at/above this similarity.
    semantic_threshold: float = field(default_factory=lambda: _f("MATCH_SEMANTIC_THRESHOLD", 0.45))
    # Below this, a same-family pair is not even worth reporting as related.
    related_threshold: float = field(default_factory=lambda: _f("MATCH_RELATED_THRESHOLD", 0.40))

    # ── Document-level similarity scaling ────────────────────────────────────
    # Raw resume-vs-JD cosine tops out well below 1.0; scale into 0-100.
    doc_sim_scale: float = field(default_factory=lambda: _f("MATCH_DOC_SIM_SCALE", 200.0))

    def credit_for(self, kind: str) -> float:
        return {
            "EXACT": self.credit_exact,
            "EQUIVALENT": self.credit_equivalent,
            "SEMANTIC": self.credit_semantic,
            "RELATED_ONLY": self.credit_related,
            "MISSING": 0.0,
        }.get(kind, 0.0)


DEFAULT_CONFIG = MatchConfig()

# Embedding model — configurable, must be a sentence-transformers model name.
EMBED_MODEL = os.environ.get("MATCH_EMBED_MODEL", "all-MiniLM-L6-v2")


# ---------------------------------------------------------------------------
# Resume-match score bands.
#
# CALIBRATION NOTE — these changed when the hybrid matcher landed.
#
# The legacy TF-IDF matcher scored real JDs at mean ~44 (max ~56), because the
# document-similarity half of its formula rarely exceeded 25. The hybrid scores
# the same JDs at mean ~86: alias/acronym/subsumption resolution genuinely finds
# skills the substring matcher missed, so skill_fit is legitimately far higher.
#
# The classification is better; the scale is simply not comparable. Reusing the
# old cut-offs would have fired the auto-feedback rule on 40% of all jobs
# (48/120 measured) and flooded the ML training set with junk positives.
#
# Bands below are percentiles of the hybrid score over 200 real scraped JDs
# matched against config/resume_de.txt:
#     p50 = 64    p70 = 84    p75 = 87    p85 = 92    p90 = 96
# ---------------------------------------------------------------------------

# Auto-record "interested" (ML bootstrap). ~top 15% of jobs. Was 75 on the old
# scale, which fired on ~40% of jobs under hybrid scoring.
AUTO_INTERESTED_THRESHOLD = int(os.environ.get("MATCH_AUTO_INTERESTED", 92))

# Email / dashboard badge bands. Were 70 / 45 on the legacy scale.
BAND_STRONG = int(os.environ.get("MATCH_BAND_STRONG", 85))   # ~top 25%
BAND_MODERATE = int(os.environ.get("MATCH_BAND_MODERATE", 65))  # ~median

# `--tailor` verdict bands. Were 70 / 50 / 35 on the legacy scale.
TAILOR_STRONG = int(os.environ.get("MATCH_TAILOR_STRONG", 85))
TAILOR_DECENT = int(os.environ.get("MATCH_TAILOR_DECENT", 70))
TAILOR_STRETCH = int(os.environ.get("MATCH_TAILOR_STRETCH", 55))
