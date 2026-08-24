"""Structured JD parsing: sections -> requirements.

Replaces a character-window heuristic with document structure.

Why
---
The previous approach asked "is there a 'required' cue within 400 characters
before this skill's first mention?". Measured on the 200-job frozen corpus:

* 63% of skill mentions had **no cue at all** in the window, silently
  defaulting to not-required
* 34% of skills appear more than once, and for 29% of those the verdict flips
  depending on which occurrence you look at — i.e. arbitrary
* only 18% of skills classified as required, and **half of all jobs detected
  zero** required skills

The downstream consequence was that the required-miss ceiling and the 2.0
requirement weight — the guardrails meant to stop semantic similarity from
masking an explicit mismatch — were inert for most jobs.

Approach
--------
Job descriptions have structure. Segment on headings, classify each section,
then attribute skills to the section containing them:

    REQUIRED         "Requirements", "Minimum Qualifications", "You Have"
    PREFERRED        "Preferred Qualifications", "Nice to Have", "Bonus"
    RESPONSIBILITIES "What You'll Do", "Key Responsibilities"
    ABOUT / BENEFITS  ignored for requirement purposes
    OTHER            unclassified

Responsibilities are deliberately distinguished from requirements: "You will
build data pipelines" describes the job, not a qualification the candidate
must already hold. Treating duties as requirements inflates the requirement
set and penalises candidates for skills the JD never actually demanded.

Where a JD has no usable headings (many are a single blob), the parser falls
back to sentence-level cue detection, which is still narrower and more
reliable than a fixed character window.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from . import ontology

# ---------------------------------------------------------------------------
# Section classification
#
# Patterns derived from headings actually observed in the frozen corpus, not
# invented: "responsibilities" (14), "qualifications" (11), "what you'll do"
# (8), "preferred qualifications" (7), "required qualifications" (5), etc.
# ---------------------------------------------------------------------------
SECTION_REQUIRED = "required"
SECTION_PREFERRED = "preferred"
SECTION_RESPONSIBILITIES = "responsibilities"
SECTION_ABOUT = "about"
SECTION_BENEFITS = "benefits"
SECTION_OTHER = "other"

# Order matters: "preferred qualifications" must beat "qualifications".
_SECTION_PATTERNS: list[tuple[str, re.Pattern]] = [
    (SECTION_PREFERRED, re.compile(
        r"\b(preferred|nice[\s-]to[\s-]have|bonus|good to have|desired|"
        r"pluses|it'?s a plus|additionally helpful|extra credit)\b", re.I)),
    (SECTION_REQUIRED, re.compile(
        r"\b(requirements?|required qualifications?|minimum qualifications?|"
        r"basic qualifications?|qualifications?|must[\s-]haves?|"
        r"what you'?ll bring|what we'?re looking for|who you are|you have|"
        r"skills? (?:and|&) experience|experience (?:required|needed|we look for)|"
        r"the stats|your background|"
        r"required skills?|essential)\b", re.I)),
    (SECTION_RESPONSIBILITIES, re.compile(
        r"\b(responsibilit(?:y|ies)|what you'?ll do|what you will do|the role|"
        r"day[\s-]to[\s-]day|in this role|duties|the game plan|you will|"
        r"job duties|about the role|role overview|what you'?ll be doing)\b", re.I)),
    (SECTION_BENEFITS, re.compile(
        r"\b(benefits?|perks?|what we offer|compensation|salary|pay range|"
        r"time off|insurance|401k|equity|total rewards)\b", re.I)),
    (SECTION_ABOUT, re.compile(
        r"\b(about (?:us|the (?:team|company))|who we are|our (?:mission|values|team)|"
        r"company overview|equal opportunity|disclaimer|statement of|"
        r"privacy|applicants?|please note|location|job title)\b", re.I)),
]

# A heading: a short title-ish run ending in a colon, or a standalone short
# capitalised line. Kept deliberately tight to avoid matching prose.
_HEADING_RE = re.compile(
    r"(?:^|\n|\.\s+|•\s*)([A-Z][A-Za-z'’/&\-, ]{2,48}?)\s*:",
)

# Headings that appear without a colon, run straight into their list. Only
# unambiguous phrases belong here — anything looser would match prose.
_BARE_HEADING_RE = re.compile(
    r"\b((?:basic|minimum|required|preferred|additional)\s+qualifications?"
    r"|req(?:uired)?\s+skills?(?:\s+and\s+requirements?)?"
    r"|what you'?ll bring|what we'?re looking for|nice[\s-]to[\s-]have)\b", re.I)

# Sentence-level cues, used when no section structure is available
_SENT_REQUIRED = re.compile(
    r"\b(must have|must be|required|requires|you (?:should|must) have|"
    r"we require|is required|minimum of|at least|proven (?:experience|track)|"
    r"you have|you'?ll have|you bring|strong background|solid background|"
    r"\d+\+?\s*(?:-|to)?\s*\d*\+?\s*years?|"
    r"experience (?:in|with|programming|building|developing)|"
    r"(?:strong|deep|solid|hands[\s-]on|demonstrated|proven) (?:experience|knowledge|"
    r"understanding|proficiency|command)|proficien(?:t|cy) (?:in|with)|"
    r"working knowledge|background in)\b", re.I)
_SENT_PREFERRED = re.compile(
    r"\b(preferred|nice to have|a plus|bonus|ideally|desirable|"
    r"familiarity (?:with|in)|exposure to|would be great)\b", re.I)

_YEARS_RE = re.compile(
    r"(\d+)\s*\+?\s*(?:-|to|–)?\s*(\d+)?\s*(?:\+)?\s*years?", re.I)

_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+|(?=•)")


@dataclass
class Section:
    heading: str
    kind: str
    text: str
    start: int
    end: int


@dataclass
class Requirement:
    """One skill the JD asks for, with the evidence that classified it."""

    canonical: str
    surface: str
    kind: str                       # required | preferred | responsibility | unspecified
    section_heading: str = ""
    years: Optional[int] = None
    evidence: str = ""              # the sentence the skill appeared in

    @property
    def is_required(self) -> bool:
        return self.kind == "required"

    @property
    def display(self) -> str:
        return ontology.display_name(self.canonical)


@dataclass
class ParsedJD:
    sections: list[Section] = field(default_factory=list)
    requirements: list[Requirement] = field(default_factory=list)
    min_years: Optional[int] = None
    used_sections: bool = False     # False => fell back to sentence cues

    def by_kind(self, kind: str) -> list[Requirement]:
        return [r for r in self.requirements if r.kind == kind]

    @property
    def required(self) -> list[Requirement]:
        return self.by_kind("required")

    @property
    def preferred(self) -> list[Requirement]:
        return self.by_kind("preferred")


def classify_heading(heading: str) -> str:
    """Map a heading to a section kind."""
    h = heading.strip().lower()
    for kind, pat in _SECTION_PATTERNS:
        if pat.search(h):
            return kind
    return SECTION_OTHER


def split_sections(text: str) -> list[Section]:
    """Segment a JD into headed sections.

    Returns [] when no usable headings are found, signalling the caller to fall
    back to sentence-level cues.
    """
    if not text:
        return []
    # Two-pass: every plausible heading is a *boundary*, but only recognised
    # ones carry a classification. An unrecognised heading inherits the kind of
    # the section it sits inside — "Cross-functional collaboration:" appearing
    # under "Responsibilities:" is still responsibilities, and must neither
    # start a new topic nor reset the section to OTHER.
    marks: list[tuple[int, int, str, str]] = []
    for m in _HEADING_RE.finditer(text):
        heading = m.group(1).strip()
        if not (1 <= len(heading.split()) <= 7):
            continue
        marks.append((m.start(1), m.end(), heading, classify_heading(heading)))
    for m in _BARE_HEADING_RE.finditer(text):
        heading = m.group(1).strip()
        marks.append((m.start(1), m.end(), heading, classify_heading(heading)))
    marks.sort(key=lambda t: t[0])
    # Drop duplicates where both regexes caught the same heading
    deduped: list[tuple[int, int, str, str]] = []
    for mk in marks:
        if deduped and mk[0] - deduped[-1][0] < 4:
            continue
        deduped.append(mk)
    marks = deduped

    # A JD needs at least one heading we actually understand; otherwise the
    # boundaries carry no information and sentence cues are the better signal.
    if not any(kind != SECTION_OTHER for *_, kind in marks):
        return []

    # Propagate classification forward across unrecognised headings
    resolved: list[tuple[int, int, str, str]] = []
    current = SECTION_OTHER
    for h_start, body_start, heading, kind in marks:
        if kind != SECTION_OTHER:
            current = kind
        resolved.append((h_start, body_start, heading, current))
    marks = resolved

    sections: list[Section] = []
    for i, (h_start, body_start, heading, kind) in enumerate(marks):
        body_end = marks[i + 1][0] if i + 1 < len(marks) else len(text)
        body = text[body_start:body_end].strip()
        if not body:
            continue
        sections.append(Section(
            heading=heading, kind=kind,
            text=body, start=body_start, end=body_end,
        ))
    return sections


def _years_in(text: str) -> Optional[int]:
    m = _YEARS_RE.search(text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _sentences(text: str) -> list[str]:
    return [s.strip().lstrip("•-*→▪◦►– ").strip()
            for s in _SENT_SPLIT.split(text or "") if s and s.strip()]


# Strength ordering — a skill appearing in several places takes the strongest
# classification. Appearing under "Requirements" outranks a later mention in
# "Nice to have"; both outrank a passing mention in a duties list.
_STRENGTH = {"required": 3, "preferred": 2, "responsibility": 1, "unspecified": 0}


def _kind_from_section(section_kind: str) -> str:
    return {
        SECTION_REQUIRED: "required",
        SECTION_PREFERRED: "preferred",
        SECTION_RESPONSIBILITIES: "responsibility",
    }.get(section_kind, "unspecified")


def _kind_from_sentence(sentence: str) -> str:
    """Classify from cues inside the sentence itself."""
    req, pref = _SENT_REQUIRED.search(sentence), _SENT_PREFERRED.search(sentence)
    if req and pref:
        # Whichever qualifies the skill more locally wins; ties favour preferred,
        # which is the conservative choice (a wrong 'required' inflates the
        # penalty, a wrong 'preferred' merely under-weights).
        return "required" if req.start() > pref.start() else "preferred"
    if req:
        return "required"
    if pref:
        return "preferred"
    return "unspecified"


def parse_jd(text: str) -> ParsedJD:
    """Parse a job description into structured requirements."""
    if not text:
        return ParsedJD()

    sections = split_sections(text)
    best: dict[str, Requirement] = {}

    def consider(cid: str, surface: str, kind: str, heading: str,
                 sentence: str) -> None:
        years = _years_in(sentence)
        existing = best.get(cid)
        if existing is None or _STRENGTH[kind] > _STRENGTH[existing.kind]:
            best[cid] = Requirement(
                canonical=cid, surface=surface, kind=kind,
                section_heading=heading, years=years, evidence=sentence[:300],
            )
        elif existing.years is None and years is not None:
            existing.years = years

    if sections:
        for sec in sections:
            # Benefits/about sections mention technologies incidentally
            # ("we use Python here") — not requirements.
            if sec.kind in (SECTION_BENEFITS, SECTION_ABOUT):
                continue
            base_kind = _kind_from_section(sec.kind)
            for sentence in _sentences(sec.text):
                found = ontology.extract_canonical_skills(sentence)
                if not found:
                    continue
                # A sentence-level cue can strengthen an OTHER section, but
                # never overrides an explicit Preferred heading.
                kind = base_kind
                if sec.kind == SECTION_OTHER:
                    kind = _kind_from_sentence(sentence)
                elif sec.kind == SECTION_REQUIRED:
                    if _SENT_PREFERRED.search(sentence):
                        kind = "preferred"   # "X preferred" inside Requirements
                for cid, surface in found.items():
                    consider(cid, surface, kind, sec.heading, sentence)
    else:
        # No usable structure — sentence-level cues only.
        for sentence in _sentences(text):
            found = ontology.extract_canonical_skills(sentence)
            if not found:
                continue
            kind = _kind_from_sentence(sentence)
            for cid, surface in found.items():
                consider(cid, surface, kind, "", sentence)

    # Overall minimum years: prefer a required section, else anywhere
    min_years = None
    req_years = [r.years for r in best.values() if r.kind == "required" and r.years]
    any_years = [r.years for r in best.values() if r.years]
    if req_years:
        min_years = min(req_years)
    elif any_years:
        min_years = min(any_years)
    else:
        min_years = _years_in(text)

    return ParsedJD(
        sections=sections,
        requirements=sorted(best.values(), key=lambda r: (-_STRENGTH[r.kind], r.canonical)),
        min_years=min_years,
        used_sections=bool(sections),
    )
