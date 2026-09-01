"""Years-of-experience extraction from job description text.

The existing extractor in `resume_matcher.py` made the "experience" context
word optional, so it read a requirement out of "our company was founded 20
years ago" and "complete your degree within 4 years". This module requires
experience context near the number and explicitly rejects known trap phrasings.

Bias is deliberately permissive. The objective is applications, so an
unparseable or borderline requirement resolves toward keeping the job.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import reasons as R

# Normal eligibility ceiling — mirrors resume_matcher.MAX_EXPERIENCE_YEARS.
ELIGIBILITY_CEILING = 4
# At or above this, the requirement is a strong negative signal.
STRONG_NEGATIVE_FLOOR = 5
# At or above this, the job is rejected outright absent contrary evidence.
REJECT_FLOOR = 7

_WORD_NUMS = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}
_NUM_ALT = r"\d{1,2}|" + "|".join(_WORD_NUMS)

# A number (or range) immediately attached to a years unit.
_YEARS_RE = re.compile(
    rf"""
    (?P<prefix>minimum\s+(?:of\s+)?|at\s+least\s+|at\s+minimum\s+|over\s+|more\s+than\s+|up\s+to\s+)?
    (?P<lo>{_NUM_ALT})
    \s*(?P<plus>\+)?
    (?:\s*(?:-|–|to|through)\s*(?P<hi>{_NUM_ALT})\s*(?P<hiplus>\+)?)?
    \s*(?P<unit>years?|yrs?)
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Experience context that must appear near the number for it to count.
_EXPERIENCE_CTX = re.compile(
    r"experience|background|working|work(?:ing)?\s+in|professional|industry|"
    r"hands[\s-]?on|practical|relevant|related|software\s+develop\w*|"
    r"engineering|programming|development|building|designing|"
    r"data\s+engineering|machine\s+learning|track\s+record|expertise|"
    r"proficiency|practitioner",
    re.IGNORECASE,
)

# Phrasings where a number of years is NOT an experience requirement.
_TRAP_CTX = re.compile(
    r"founded|established|incorporated|in\s+business|anniversary|"
    r"\bago\b|over\s+the\s+(?:past|last)\s+\w+\s+years|next\s+\w+\s+years|"
    r"contract|assignment|engagement|term\s+of|duration|lease|tenure\s+of|"
    r"within\s+\w+\s+years?|"
    r"\d+[-\s]year\s+(?:degree|program|curriculum|course|study|studies|plan|visa)|"
    r"vest(?:s|ing|ed)?|cliff|"
    r"after\s+\w+\s+years?\s+of\s+service|"
    r"benefits?\b|401k|pto|vacation|warranty|severance|"
    r"history\s+of\s+the",
    re.IGNORECASE,
)

_NO_EXPERIENCE_RE = re.compile(
    r"no\s+(?:prior\s+|previous\s+|professional\s+)?experience\s+(?:is\s+)?"
    r"(?:required|necessary|needed)"
    r"|no\s+experience\s+necessary"
    r"|entry[\s-]?level\s+position"
    r"|suitable\s+for\s+(?:new\s+)?grad\w*",
    re.IGNORECASE,
)

_EQUIVALENT_RE = re.compile(r"equivalent\s+(?:practical\s+)?experience|or\s+equivalent",
                            re.IGNORECASE)

# How far either side of the number we look for context.
_CTX_WINDOW = 70
# Traps bind tightly to their number; a wider window produced false vetoes.
_TRAP_WINDOW_BEFORE = 32
_TRAP_WINDOW_AFTER = 24


@dataclass(frozen=True)
class ExperienceAnalysis:
    min_years: int | None
    max_years: int | None
    raw_matches: tuple[str, ...] = field(default_factory=tuple)
    entry_level_compatible: bool = True
    confidence: str = "low"          # high | medium | low
    reasons: tuple[str, ...] = field(default_factory=tuple)
    equivalent_experience: bool = False

    @property
    def stated(self) -> bool:
        return self.min_years is not None


def _to_int(tok: str) -> int | None:
    tok = (tok or "").strip().lower()
    if tok.isdigit():
        return int(tok)
    return _WORD_NUMS.get(tok)


def _reason_for(lo: int, hi: int | None) -> str:
    if lo == 0:
        return R.EXPERIENCE_0_2 if (hi or 0) <= 2 else R.EXPERIENCE_0_2
    if lo == 1:
        return R.EXPERIENCE_1_2
    if lo == 2:
        return R.EXPERIENCE_2_3 if hi and hi >= 3 else R.EXPERIENCE_2
    if lo == 3:
        return R.EXPERIENCE_3
    if lo == 4:
        return R.EXPERIENCE_4
    if lo >= REJECT_FLOOR:
        return R.EXPERIENCE_7_PLUS
    return R.EXPERIENCE_5_PLUS


def analyze_experience(text: str) -> ExperienceAnalysis:
    """Extract the minimum years of experience a JD requires.

    Returns min_years=None when nothing reliable was found — which is treated
    as eligible, not as a rejection.
    """
    if not text:
        return ExperienceAnalysis(None, None, (), True, "low", (R.EXPERIENCE_UNKNOWN,))

    body = re.sub(r"\s+", " ", text)

    if _NO_EXPERIENCE_RE.search(body):
        return ExperienceAnalysis(
            0, None, ("no experience required",), True, "high",
            (R.EXPERIENCE_NONE_REQUIRED,),
        )

    equivalent = bool(_EQUIVALENT_RE.search(body))

    hits: list[tuple[int, int | None, str, bool]] = []   # (lo, hi, snippet, strong_ctx)
    for m in _YEARS_RE.finditer(body):
        lo = _to_int(m.group("lo"))
        if lo is None or lo > 20:
            continue
        hi = _to_int(m.group("hi") or "")
        window = body[max(0, m.start() - _CTX_WINDOW):min(len(body), m.end() + _CTX_WINDOW)]
        trap_window = (
            body[max(0, m.start() - _TRAP_WINDOW_BEFORE):m.start()]
            + " "
            + body[m.end():m.end() + _TRAP_WINDOW_AFTER]
        )

        if _TRAP_CTX.search(trap_window):
            continue
        if not _EXPERIENCE_CTX.search(window):
            continue

        # "up to N years" / "more than N years" describe a ceiling or a floor;
        # only the floor form sets a minimum.
        prefix = (m.group("prefix") or "").strip().lower()
        if prefix == "up to":
            continue

        strong = bool(re.search(r"experience|professional|industry", window, re.I))
        hits.append((lo, hi, m.group(0).strip(), strong))

    if not hits:
        return ExperienceAnalysis(None, None, (), True, "low", (R.EXPERIENCE_UNKNOWN,),
                                  equivalent_experience=equivalent)

    # Lowest stated floor across the JD — a posting often lists a lower bar in
    # "minimum qualifications" and a higher one in "preferred".
    lo = min(h[0] for h in hits)
    his = [h[1] for h in hits if h[1] is not None]
    hi = max(his) if his else None
    if hi is not None and hi < lo:
        hi = None

    strong_ctx = any(h[3] for h in hits if h[0] == lo)
    raw = tuple(h[2] for h in hits)[:8]

    codes = [_reason_for(lo, hi)]
    confidence = "high" if strong_ctx else "medium"
    if not strong_ctx:
        codes.append(R.EXPERIENCE_LOW_CONF)

    return ExperienceAnalysis(
        min_years=lo,
        max_years=hi,
        raw_matches=raw,
        entry_level_compatible=lo <= ELIGIBILITY_CEILING,
        confidence=confidence,
        reasons=tuple(codes),
        equivalent_experience=equivalent,
    )
