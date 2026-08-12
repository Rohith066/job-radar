"""Hybrid resume ↔ job-description matching.

Layers: deterministic (aliases/acronyms/canonical skills) → family veto →
semantic (local sentence-transformers, optional) → transparent scoring.

See ``hybrid.match()`` for the entry point.
"""
from .config import DEFAULT_CONFIG, MatchConfig
from .hybrid import HybridResult, SkillMatch, match

__all__ = ["match", "HybridResult", "SkillMatch", "MatchConfig", "DEFAULT_CONFIG"]
