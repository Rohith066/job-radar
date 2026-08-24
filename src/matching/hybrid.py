"""Hybrid resume ↔ JD matcher: deterministic + semantic + verification.

Classification precedence (order matters):

1. **EXACT**        canonical skill literally present in the resume
2. **EQUIVALENT**   alias/acronym or subsumption resolves to the same skill
                    (``Amazon Web Services`` → ``aws``; PySpark covers Spark)
3. **RELATED_ONLY** a *different* canonical skill in the same family is present
                    (Docker vs Kubernetes). **Vetoes semantic acceptance.**
4. **SEMANTIC**     no family conflict, and embedding similarity to some resume
                    sentence clears the threshold
5. **MISSING**      none of the above

Step 3 running before step 4 is the point of the whole design. Embeddings rate
``docker``↔``kubernetes`` around 0.6 — enough to look like a match. The family
rule overrides that, so a sibling technology can never silently satisfy an
explicitly named requirement.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from . import ontology, semantic
from .jd_parser import parse_jd
from .config import DEFAULT_CONFIG, MatchConfig

log = logging.getLogger(__name__)

MatchKind = str  # "EXACT" | "EQUIVALENT" | "SEMANTIC" | "RELATED_ONLY" | "MISSING"


@dataclass
class SkillMatch:
    """One JD requirement and how (or whether) the resume satisfies it."""

    canonical: str
    jd_surface: str                       # the JD's own wording
    kind: MatchKind
    required: bool = False
    credit: float = 0.0
    similarity: Optional[float] = None    # cosine, when semantic was consulted
    evidence: str = ""                    # resume sentence that justified it
    related_via: Optional[str] = None     # canonical skill that is merely related

    @property
    def display(self) -> str:
        return ontology.display_name(self.canonical)

    @property
    def satisfied(self) -> bool:
        return self.kind in ("EXACT", "EQUIVALENT", "SEMANTIC")


@dataclass
class HybridResult:
    """Transparent, component-wise match result."""

    overall_score: int = 0
    skill_fit: int = 0
    doc_similarity: int = 0

    exact_coverage: int = 0
    equivalent_coverage: int = 0
    semantic_coverage: int = 0

    matches: list[SkillMatch] = field(default_factory=list)
    semantic_used: bool = False
    has_jd: bool = False

    # ── Convenience views ────────────────────────────────────────────────────
    def _of(self, *kinds: str) -> list[SkillMatch]:
        return [m for m in self.matches if m.kind in kinds]

    @property
    def exact(self) -> list[SkillMatch]:
        return self._of("EXACT")

    @property
    def equivalent(self) -> list[SkillMatch]:
        return self._of("EQUIVALENT")

    @property
    def semantic(self) -> list[SkillMatch]:
        return self._of("SEMANTIC")

    @property
    def related_only(self) -> list[SkillMatch]:
        return self._of("RELATED_ONLY")

    @property
    def missing(self) -> list[SkillMatch]:
        return self._of("MISSING", "RELATED_ONLY")

    @property
    def missing_required(self) -> list[SkillMatch]:
        return [m for m in self.missing if m.required]

    @property
    def matched_skills(self) -> list[str]:
        return sorted(m.display for m in self.matches if m.satisfied)

    @property
    def missing_skills(self) -> list[str]:
        return sorted(m.display for m in self.missing)

    @property
    def jd_terminology(self) -> list[tuple[str, str]]:
        """JD wording the resume substantiates but does not literally use.

        These are safe, truthful vocabulary swaps for a tailored resume: the
        evidence already exists, only the JD's phrasing is absent. Skills with
        no supporting evidence are deliberately excluded — this never suggests
        adding something the resume cannot back up.
        """
        return [(m.jd_surface, m.evidence) for m in self.semantic if m.evidence]


_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")
_REQUIRED_CTX = re.compile(
    r"\b(required|must have|must-have|minimum qualifications|basic qualifications|"
    r"you have|requirements|required skills|required experience)\b",
    re.IGNORECASE,
)
_PREFERRED_CTX = re.compile(
    r"\b(preferred|nice to have|nice-to-have|bonus|a plus|desired|ideally)\b",
    re.IGNORECASE,
)


def split_sentences(text: str, min_len: int = 20, cap: int = 400) -> list[str]:
    """Split resume text into evidence-bearing units (sentences/bullets)."""
    out: list[str] = []
    for raw in _SENT_SPLIT.split(text or ""):
        s = raw.strip().lstrip("•-*→▪◦►– ").strip()
        if len(s) >= min_len:
            out.append(s)
    return out[:cap]


def is_required(jd_text: str, surface: str) -> bool:
    """Whether a skill appears under required (vs preferred) framing.

    Looks at the 400 characters preceding the mention; the nearest cue wins.
    """
    idx = jd_text.lower().find(surface.lower())
    if idx < 0:
        return False
    window = jd_text[max(0, idx - 400):idx]
    req, pref = _REQUIRED_CTX.search(window), _PREFERRED_CTX.search(window)
    if req and pref:
        return req.start() > pref.start()   # nearest cue to the mention
    return bool(req)


def _find_evidence(resume_sentences: list[str], surface: str) -> str:
    """First resume sentence literally containing the surface form."""
    low = surface.lower()
    for s in resume_sentences:
        if low in s.lower():
            return s
    return ""


def match(
    resume_text: str,
    jd_text: str,
    job_title: str = "",
    config: MatchConfig = DEFAULT_CONFIG,
    use_semantic: bool = True,
) -> HybridResult:
    """Run the full three-layer match."""
    if not resume_text or not jd_text:
        return HybridResult(has_jd=bool(jd_text))

    resume_skills = ontology.extract_canonical_skills(resume_text)
    jd_skills = ontology.extract_canonical_skills(jd_text)
    resume_sentences = split_sentences(resume_text)

    # Structured requirement extraction (src/matching/jd_parser.py). Parsed
    # once per JD rather than re-scanning a character window per skill. See
    # that module for why the window approach was replaced.
    parsed = parse_jd(jd_text)
    requirement_kind = {r.canonical: r.kind for r in parsed.requirements}

    want_semantic = use_semantic and semantic.is_available()
    sem_matrix = None
    unresolved: list[tuple[str, str]] = []   # (canonical, surface) needing L2

    matches: list[SkillMatch] = []

    # ── Layers 1 & 3: deterministic classification + family veto ─────────────
    for cid, surface in jd_skills.items():
        required = requirement_kind.get(cid) == "required"

        # L1a — EXACT: the canonical skill is present under some surface form
        if cid in resume_skills:
            r_surface = resume_skills[cid]
            kind = "EXACT" if r_surface == surface else "EQUIVALENT"
            matches.append(SkillMatch(
                canonical=cid, jd_surface=surface, kind=kind, required=required,
                credit=config.credit_for(kind),
                evidence=_find_evidence(resume_sentences, r_surface),
            ))
            continue

        # L1b — EQUIVALENT via subsumption (PySpark covers Spark)
        covering = next(
            (r for r in resume_skills if ontology.subsumes(r, cid)), None
        )
        if covering:
            matches.append(SkillMatch(
                canonical=cid, jd_surface=surface, kind="EQUIVALENT",
                required=required, credit=config.credit_for("EQUIVALENT"),
                evidence=_find_evidence(resume_sentences, resume_skills[covering]),
                related_via=covering,
            ))
            continue

        # L3 — FAMILY VETO, applied before any semantic consideration
        sibling = next(
            (r for r in resume_skills if ontology.same_family(r, cid)), None
        )
        if sibling:
            matches.append(SkillMatch(
                canonical=cid, jd_surface=surface, kind="RELATED_ONLY",
                required=required, credit=config.credit_for("RELATED_ONLY"),
                evidence=_find_evidence(resume_sentences, resume_skills[sibling]),
                related_via=sibling,
            ))
            continue

        unresolved.append((cid, surface))

    # ── Layer 2: semantic, only for skills that survived the veto ────────────
    if want_semantic and unresolved and resume_sentences:
        phrases = [ontology.display_name(c) for c, _ in unresolved]
        sem_matrix = semantic.cosine_matrix(phrases, resume_sentences)

    for i, (cid, surface) in enumerate(unresolved):
        required = requirement_kind.get(cid) == "required"
        best_sim, best_sent = None, ""
        if sem_matrix is not None:
            row = sem_matrix[i]
            j = int(row.argmax())
            best_sim, best_sent = float(row[j]), resume_sentences[j]

        if best_sim is not None and best_sim >= config.semantic_threshold:
            matches.append(SkillMatch(
                canonical=cid, jd_surface=surface, kind="SEMANTIC",
                required=required, credit=config.credit_for("SEMANTIC"),
                similarity=best_sim, evidence=best_sent,
            ))
        else:
            matches.append(SkillMatch(
                canonical=cid, jd_surface=surface, kind="MISSING",
                required=required, credit=0.0, similarity=best_sim,
            ))

    # ── Scoring ──────────────────────────────────────────────────────────────
    total_w = credited_w = 0.0
    cov = {"EXACT": 0.0, "EQUIVALENT": 0.0, "SEMANTIC": 0.0}
    for m in matches:
        w = config.weight_required if m.required else config.weight_preferred
        total_w += w
        credited_w += m.credit * w
        if m.kind in cov:
            cov[m.kind] += w

    skill_fit = int(round(credited_w / total_w * 100)) if total_w else 0
    pct = lambda x: int(round(x / total_w * 100)) if total_w else 0

    # Document-level similarity: semantic when available, else TF-IDF
    doc_sim_raw = None
    if want_semantic:
        doc_sim_raw = semantic.similarity(resume_text, f"{job_title}\n{jd_text}")
    if doc_sim_raw is None:
        from ..resume_matcher import _tfidf_similarity
        doc_sim_raw = _tfidf_similarity(resume_text, f"{job_title}\n{jd_text}")
    doc_similarity = max(0, min(100, int(doc_sim_raw * config.doc_sim_scale)))

    overall = (config.weight_skill_fit * skill_fit
               + config.weight_doc_sim * doc_similarity)

    # Guardrail: an unsatisfied REQUIRED skill lowers the achievable ceiling,
    # so document-level similarity cannot paper over an explicit mismatch.
    n_missing_req = sum(1 for m in matches if m.required and not m.satisfied)
    ceiling = 100.0 - config.required_miss_penalty * n_missing_req
    overall = max(0, min(int(round(min(overall, ceiling))), 100))

    return HybridResult(
        overall_score=overall,
        skill_fit=skill_fit,
        doc_similarity=doc_similarity,
        exact_coverage=pct(cov["EXACT"]),
        equivalent_coverage=pct(cov["EQUIVALENT"]),
        semantic_coverage=pct(cov["SEMANTIC"]),
        matches=matches,
        semantic_used=sem_matrix is not None,
        has_jd=True,
    )
