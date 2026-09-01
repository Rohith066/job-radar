"""Token- and phrase-aware job title analysis.

Replaces the substring `in` matching that let `Software Engineering Manager`
read as a Software Engineer role and `Data Engineer IV` read as entry level.

Two independent axes are extracted and then combined:

  role family — which technical discipline the title belongs to
  seniority   — how senior the role is, from explicit words AND numeric /
                roman level suffixes

Seniority is a **veto**, not a score cap. That is the single most important
change: the previous classifier clamped senior titles to 65 and eight
downstream bonuses then pushed them back over the alert threshold, which is
why 552 manager/director/VP roles were alerting in production.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from . import reasons as R

# ---------------------------------------------------------------------------
# Role families
# ---------------------------------------------------------------------------
# Phrases are matched on token boundaries, so "software engineer" cannot match
# inside "software engineering manager" — the trailing "ing" defeats \b.

TARGET_FAMILIES: dict[str, tuple[str, ...]] = {
    "software_engineering": (
        "software engineer", "software engineering", "software developer",
        "software development engineer",
        "software engineering intern",  # caught earlier by the internship veto
        "sde", "application developer", "applications developer",
        "software design engineer", "programmer analyst", "software programmer",
    ),
    "backend": (
        "backend engineer", "backend engineering", "back-end engineer", "back end engineer",
        "backend developer", "back-end developer", "back end developer",
        "backend software engineer", "server side engineer", "server-side engineer",
        "api engineer", "platform engineer", "distributed systems engineer",
    ),
    "fullstack": (
        "full stack engineer", "full stack engineering", "fullstack engineer",
        "full-stack engineer",
        "full stack developer", "fullstack developer", "full-stack developer",
        "full stack software engineer", "web developer", "web engineer",
    ),
    "data_engineering": (
        "analytics engineer", "analytics engineering", "data engineer",
        "data engineering", "platform data engineer", "dbt engineer",
        "dbt developer", "dbt analyst", "data platform engineer", "etl engineer",
        "etl developer", "elt engineer", "data pipeline engineer",
        "data infrastructure engineer", "data reliability engineer",
        "data warehouse engineer", "data warehousing engineer", "dwh engineer",
        "data modeler", "data modeling engineer", "analytics developer",
        "big data engineer", "data developer",
    ),
    "ml_ai": (
        "llm engineer", "llm developer", "ai engineer", "applied ai engineer",
        "generative ai engineer", "gen ai engineer", "retrieval engineer",
        "rag engineer", "nlp engineer", "conversational ai engineer",
        "foundation model engineer", "ai data engineer", "prompt engineer",
        "ai/ml engineer", "ml systems engineer", "ai systems engineer",
        "llm systems engineer", "ai platform engineer",
        "machine learning platform engineer", "machine learning engineer",
        "machine learning engineering", "ai engineering", "ml engineering",
        # Broader discipline phrases so real AI roles with unusual qualifiers
        # ("Generative AI Integration Engineer") attribute to this family
        # rather than falling through to the unclassified technical bucket.
        "machine learning", "generative ai", "gen ai", "genai",
        "artificial intelligence", "deep learning",
        "ml engineer", "mlops engineer", "ml platform engineer",
        "deep learning engineer", "ai software engineer", "ml software engineer",
    ),
    "data_science": (
        "data scientist", "data science", "applied scientist",
    ),
}

# Adjacent families — real matches, but a weaker fit than the targets above.
SECONDARY_FAMILIES: dict[str, tuple[str, ...]] = {
    "data_analytics": (
        "data analyst", "data analytics", "business intelligence analyst",
        "bi analyst", "business intelligence engineer", "bi engineer",
        "bi developer", "analytics analyst", "insights analyst",
        "reporting analyst", "product analyst", "growth analyst",
        "marketing analyst", "operations analyst",
        "insights engineer", "data quality analyst", "data quality engineer",
        "data governance", "data operations analyst", "data steward",
        "decision science analyst",
    ),
}

# Tier 3 in the pre-Phase-1 classifier: visible but review-only, never a
# first-class target. Phase 1 accidentally promoted three of these into
# data_science and dropped the rest entirely; both are corrected here.
# Scoring caps this tier below the STRONG band (see scoring.ADJACENT_CEILING).
ADJACENT_FAMILIES: dict[str, tuple[str, ...]] = {
    "adjacent_analysis": (
        "business analyst", "research scientist", "research engineer",
        "decision scientist", "quantitative analyst", "quant analyst",
        "statistical analyst", "forecasting analyst", "ai analyst",
        "ai scientist", "operations research", "clinical data analyst",
    ),
}

# Families that must never become strong matches even though they may contain
# the word "engineer". Checked BEFORE target families so "Sales Engineer" and
# "Solutions Engineer" can never be rescued by a later phrase match.
UNRELATED_FAMILIES: tuple[str, ...] = (
    "sales engineer", "solutions engineer", "solution engineer",
    "pre-sales engineer", "presales engineer", "customer engineer",
    "field engineer", "sales", "account executive", "account manager",
    "civil engineer", "mechanical engineer", "electrical engineer",
    "manufacturing engineer", "industrial engineer", "chemical engineer",
    "biomedical engineer", "aerospace engineer", "structural engineer",
    "process engineer", "hardware engineer", "embedded engineer",
    "firmware engineer", "robotics engineer", "computer vision engineer",
    "computer vision scientist", "optical engineer", "rf engineer",
    "quality assurance", "qa engineer", "qa analyst", "test engineer",
    "quality engineer", "validation engineer", "sdet",
    "product manager", "program manager", "project manager", "scrum master",
    "agile coach", "recruiter", "talent acquisition", "human resources",
    "customer support", "technical support", "support engineer", "help desk",
    "it support", "it administrator", "systems administrator", "sysadmin",
    "database administrator", "network engineer", "security engineer",
    "cybersecurity", "penetration tester", "site reliability", "sre",
    "devops engineer", "cloud engineer", "frontend engineer", "front-end engineer",
    "front end engineer", "mobile engineer", "ios engineer", "android engineer",
    "supply chain", "technical writer", "designer", "ux researcher",
    # Non-engineering roles that reached review because their titles contain no
    # technical token at all, so nothing else excluded them.
    "technician", "warehouse", "shipping", "receiving", "team member",
    "manufacturing", "machinist", "welder", "electrician", "operator",
    "driver", "custodian", "nurse", "accountant", "attorney", "paralegal",
    "teacher", "barista", "cashier", "stocker", "picker", "packer",
)

# Restored verbatim from the pre-Phase-1 classifier. These are domains the
# owner is deliberately not searching; Phase 1 did not intend to broaden into
# them. Checked before role families so a Tier-3 or target phrase elsewhere in
# the title cannot rescue them — which is how "Research Scientist, Robotics
# Research - PhD New College Grad" reached APPLY_NOW.
PROFILE_MISMATCH: tuple[str, ...] = (
    "computer vision engineer", "computer vision scientist", "robotics engineer",
    "hardware engineer", "embedded engineer", "electrical engineer",
    "mechanical engineer",
    # Domain forms of the same exclusions — the pre-Phase-1 list matched as a
    # substring, so "Robotics Research" and "Computer Vision" were caught by
    # the bare terms too.
    "robotics", "computer vision",
)

# Words that mark a title as technical even when no family phrase matched.
_TECHNICAL_HINTS: tuple[str, ...] = (
    "engineer", "engineering", "developer", "software", "data",
    "machine learning", "ml", "ai", "analytics", "programmer", "scientist",
)

# ---------------------------------------------------------------------------
# Seniority
# ---------------------------------------------------------------------------
# Ordered most-severe first — the first hit wins, so "Senior Engineering
# Manager" reports MANAGER rather than SENIOR.

_SENIORITY_PATTERNS: tuple[tuple[str, str, str], ...] = (
    # (regex, seniority bucket, reason code)
    (r"\bchief\b|\b(?:ceo|cto|cio|cfo|coo|cmo)\b",  "executive", R.EXECUTIVE_TITLE),
    (r"\bvice\s+president\b|\bvp\b|\bsvp\b|\bevp\b", "executive", R.EXECUTIVE_TITLE),
    (r"\bhead\s+of\b|\bglobal\s+head\b|\bhead\b,",  "executive", R.EXECUTIVE_TITLE),
    (r"\bdirector\b",                                "director",  R.DIRECTOR_TITLE),
    # "management" is a DOMAIN word, not a seniority signal: "Data Engineer II,
    # Data Management Team" is an individual contributor. Only the agent noun
    # ("manager"/"managers") denotes the role itself.
    (r"\bmanagers?\b",                              "manager",   R.MANAGER_TITLE),
    (r"\bdistinguished\b|\bfellow\b",                "principal", R.PRINCIPAL_TITLE),
    (r"\bprincipal\b",                               "principal", R.PRINCIPAL_TITLE),
    (r"\bstaff\b",                                   "staff",     R.STAFF_TITLE),
    (r"\barchitect\b",                               "architect", R.ARCHITECT_TITLE),
    (r"\bsenior\b|\bsr\.?\b|\bsnr\b",                "senior",    R.SENIOR_TITLE),
    (r"\blead\b|\bleader\b|\bteam\s+lead\b|\btech(?:nical)?\s+lead\b",
                                                     "lead",      R.LEAD_TITLE),
)

# Entry-level markers. "associate" is deliberately last: it is the weakest of
# these signals and is frequently paired with a senior word ("Associate
# Director"), where the seniority veto must win.
_ENTRY_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bnew\s+grads?\b|\bnew\s+graduates?\b|\bcollege\s+grads?\b", R.NEW_GRAD_EXPLICIT),
    (r"\bgraduate\s+(?:software\s+)?(?:engineer|developer|programme|program)\b"
     r"|\buniversity\s+graduate\b|\bcampus\s+hire\b|\bcampus\s+recruit\w*\b"
     r"|\bgraduate\s+rotational\b",                                  R.NEW_GRAD_EXPLICIT),
    (r"\bentry[\s-]?level\b|\bno\s+experience\s+required\b",         R.ENTRY_LEVEL_EXPLICIT),
    (r"\bjunior\b|\bjr\.?\b",                                        R.JUNIOR_TITLE),
    (r"\bearly\s+career\b|\bearly[\s-]?in[\s-]?career\b",            R.EARLY_CAREER_TITLE),
    (r"\bapprentice\w*\b",                                           R.EARLY_CAREER_TITLE),
    (r"\bassociate\b",                                               R.ASSOCIATE_TITLE),
)

# Level suffixes. The level token must follow a role noun or an explicit
# "level"/"grade" word — this is what keeps "Data Engineer (2 openings)" and
# "Tier 1 Support" from being read as levels.
_LEVEL_RE = re.compile(
    r"\b(?:engineer|engineering|developer|scientist|analyst|programmer|"
    r"architect|level|lvl|grade|tier)\s*[-–—,]?\s*"
    r"(i{1,3}|iv|vi{0,3}|v|[1-9])\b"
    # A count is not a level: normalisation drops the brackets from
    # "Data Engineer (2 openings)", so without this the "2" reads as level II.
    r"(?!\s*(?:openings?|positions?|roles?|vacanc\w*|spots?|hires?|headcount"
    r"|days?|weeks?|months?|years?|yrs?|hours?|shifts?))",
    re.IGNORECASE,
)

_ROMAN_TO_INT = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7, "viii": 8}

# Absolute vetoes — no score, no override.
# References to an executive's org unit. "Applied AI Engineer, Office of the
# CEO" is an IC role reporting into that office, not a CEO posting, so these
# phrases are removed before seniority detection runs.
_EXEC_OFFICE_RE = re.compile(
    r"\boffice\s+of\s+the\s+(?:ceo|cto|cio|cfo|coo|cmo|chief[\w\s]{0,24}?officer|chief)\b"
    r"|\b(?:reporting\s+)?to\s+the\s+(?:ceo|cto|cio|cfo|coo|cmo)\b"
    r"|\b(?:ceo|cto|cio)'?s?\s+office\b",
    re.IGNORECASE,
)

_INTERNSHIP_RE = re.compile(
    r"\bintern(?:ship)?\b|\bco[-\s]?op\b|\bcoop\b|\bpart[-\s]?time\b|\bseasonal\b|\bcontract(?:or)?\b",
    re.IGNORECASE,
)
_CLEARANCE_RE = re.compile(
    r"\bts[/\s-]?sci\b|\btop\s+secret\b|\bpolygraph\b|\bpublic\s+trust\b"
    r"|\bclearance\b|\bus\s+citizen\w*\b|\bcitizenship\b",
    re.IGNORECASE,
)

# Family → resume track hint, consumed by src/tailor.py. Unknown values fall
# back to "de" there, so a new family never breaks tailoring.
_FAMILY_TRACK = {
    "data_engineering": "de", "data_analytics": "analyst", "data_science": "analyst",
    "adjacent_analysis": "analyst",
    "ml_ai": "ai", "software_engineering": "de", "backend": "de", "fullstack": "de",
}

_SENIORITY_REJECT = frozenset({
    "senior", "staff", "principal", "lead", "architect",
    "manager", "director", "executive",
})


@dataclass(frozen=True)
class TitleAnalysis:
    normalized_title: str
    role_family: str              # e.g. "software_engineering" | "unrelated" | "unknown"
    seniority: str                # "entry" | "unspecified" | "ambiguous" | "senior" | ...
    level: int | None             # numeric level when the title states one
    classification: str           # "YES" | "MAYBE" | "NO"
    reasons: tuple[str, ...] = field(default_factory=tuple)
    track: str = "de"

    @property
    def is_target_family(self) -> bool:
        return self.role_family in TARGET_FAMILIES

    @property
    def is_entry_level(self) -> bool:
        return self.seniority == "entry"


def normalize_title(title: str) -> str:
    """Lowercase, collapse separators, strip decorative punctuation.

    Keeps '/' (ai/ml), '+' and '#' (c++, c#) and '-' because they carry
    meaning inside role names; everything else becomes a space so that token
    boundaries behave predictably.
    """
    t = (title or "").strip().lower()
    t = t.replace("’", "'").replace("–", "-").replace("—", "-")
    t = re.sub(r"[()\[\]{}|:;•·]+", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip(" ,-")


def _phrase_re(phrase: str) -> re.Pattern:
    """Word-boundary regex for a multi-word phrase, tolerant of - and / joins."""
    parts = [re.escape(p) for p in phrase.split()]
    return re.compile(r"(?<![a-z0-9])" + r"[\s\-/]+".join(parts) + r"(?![a-z0-9])", re.IGNORECASE)


_TARGET_RES = {
    fam: tuple(_phrase_re(p) for p in phrases) for fam, phrases in TARGET_FAMILIES.items()
}
_SECONDARY_RES = {
    fam: tuple(_phrase_re(p) for p in phrases) for fam, phrases in SECONDARY_FAMILIES.items()
}
_UNRELATED_RES = tuple(_phrase_re(p) for p in UNRELATED_FAMILIES)
_ADJACENT_RES = {
    fam: tuple(_phrase_re(p) for p in phrases) for fam, phrases in ADJACENT_FAMILIES.items()
}
_PROFILE_MISMATCH_RES = tuple(_phrase_re(p) for p in PROFILE_MISMATCH)


def _detect_family(t: str) -> tuple[str, str]:
    """Return (family, reason_code). Unrelated families are checked first."""
    for rx in _UNRELATED_RES:
        if rx.search(t):
            return "unrelated", R.ROLE_FAMILY_UNRELATED
    for fam, patterns in _TARGET_RES.items():
        if any(rx.search(t) for rx in patterns):
            return fam, R.ROLE_FAMILY_TARGET
    for fam, patterns in _SECONDARY_RES.items():
        if any(rx.search(t) for rx in patterns):
            return fam, R.ROLE_FAMILY_SECONDARY
    for fam, patterns in _ADJACENT_RES.items():
        if any(rx.search(t) for rx in patterns):
            return fam, R.ROLE_FAMILY_ADJACENT
    if any(_phrase_re(h).search(t) for h in _TECHNICAL_HINTS):
        return "technical_other", R.ROLE_FAMILY_AMBIGUOUS
    return "unknown", R.ROLE_FAMILY_UNKNOWN


def _detect_level(t: str) -> int | None:
    """Extract a numeric seniority level from the title, if it states one."""
    best: int | None = None
    for m in _LEVEL_RE.finditer(t):
        tok = m.group(1).lower()
        val = _ROMAN_TO_INT.get(tok) if not tok.isdigit() else int(tok)
        if val is None or not (1 <= val <= 8):
            continue
        # A title carrying two levels ("Engineer II / III") takes the higher —
        # the senior bar is the one that actually gates the application.
        best = val if best is None else max(best, val)
    return best


def analyze_title(title: str) -> TitleAnalysis:
    """Classify a job title into family + seniority + YES/MAYBE/NO.

    Decision order, most-absolute first:
      1. internship / co-op / part-time  -> NO
      2. clearance or citizenship gate   -> NO
      3. unrelated role family           -> NO
      4. senior/staff/principal/lead/architect/manager/director/exec -> NO
      5. level >= 3                      -> NO
      6. level == 2                      -> MAYBE (ambiguous, kept for review)
      7. explicit entry marker           -> YES
      8. level == 1                      -> YES
      9. target family, no signal        -> YES  (unspecified seniority)
     10. secondary family, no signal     -> MAYBE
     11. technical but family unclear    -> MAYBE
    """
    t = normalize_title(title)
    if not t:
        return TitleAnalysis("", "unknown", "unknown", None, "NO",
                             (R.ROLE_FAMILY_UNKNOWN,), "de")

    codes: list[str] = []

    if _INTERNSHIP_RE.search(t):
        return TitleAnalysis(t, "unknown", "intern", None, "NO", (R.INTERNSHIP_TITLE,), "de")
    if _CLEARANCE_RE.search(t):
        return TitleAnalysis(t, "unknown", "unknown", None, "NO", (R.CLEARANCE_TITLE,), "de")
    if any(rx.search(t) for rx in _PROFILE_MISMATCH_RES):
        return TitleAnalysis(t, "profile_mismatch", "unknown", None, "NO",
                             (R.PROFILE_MISMATCH_TITLE,), "de")

    family, family_code = _detect_family(t)
    codes.append(family_code)
    track = _FAMILY_TRACK.get(family, "de")

    if family == "unrelated":
        return TitleAnalysis(t, family, "unknown", None, "NO", tuple(codes), track)

    level = _detect_level(t)

    # ── Seniority veto ─────────────────────────────────────────────────────
    # Strip references to an executive's office first, so "Office of the CEO"
    # cannot make an IC role read as an executive posting.
    t_sen = _EXEC_OFFICE_RE.sub(" ", t)
    for rx, bucket, code in _SENIORITY_PATTERNS:
        if re.search(rx, t_sen, re.IGNORECASE):
            codes.append(code)
            if bucket in _SENIORITY_REJECT:
                return TitleAnalysis(t, family, bucket, level, "NO", tuple(codes), track)
            break

    # ── Level-based seniority ──────────────────────────────────────────────
    if level is not None:
        if level >= 4:
            codes.append(R.LEVEL_FOUR_PLUS_TITLE)
            return TitleAnalysis(t, family, "senior", level, "NO", tuple(codes), track)
        if level == 3:
            codes.append(R.LEVEL_THREE_TITLE)
            return TitleAnalysis(t, family, "mid_senior", level, "NO", tuple(codes), track)

    # ── Entry-level markers ────────────────────────────────────────────────
    entry_code = None
    for rx, code in _ENTRY_PATTERNS:
        if re.search(rx, t, re.IGNORECASE):
            entry_code = code
            break

    if entry_code:
        codes.append(entry_code)
        # An entry marker on an unrelated-but-technical title stays reviewable
        # rather than becoming a strong match.
        if family in TARGET_FAMILIES or family in SECONDARY_FAMILIES:
            cls = "YES"
        elif family in ADJACENT_FAMILIES:
            # An entry marker does not promote an adjacent occupation out of
            # review — that is what made "Quantitative Analyst Associate" and
            # "Research Scientist ... New College Grad" reach the top band.
            cls = "MAYBE"
        elif family == "technical_other":
            cls = "MAYBE"
        else:
            cls = "NO"
        return TitleAnalysis(t, family, "entry", level, cls, tuple(codes), track)

    if level == 1:
        codes.append(R.LEVEL_ONE_TITLE)
        return TitleAnalysis(t, family, "entry", 1, "YES", tuple(codes), track)

    if level == 2:
        # Explicitly ambiguous per spec — never blindly rejected.
        codes.append(R.LEVEL_TWO_AMBIGUOUS)
        return TitleAnalysis(t, family, "ambiguous", 2, "MAYBE", tuple(codes), track)

    if family in TARGET_FAMILIES:
        return TitleAnalysis(t, family, "unspecified", None, "YES", tuple(codes), track)
    if family in SECONDARY_FAMILIES or family in ADJACENT_FAMILIES:
        return TitleAnalysis(t, family, "unspecified", None, "MAYBE", tuple(codes), track)
    if family == "technical_other":
        # Technical, but the family could not be pinned down. Preserved for
        # review rather than dropped — false negatives cost applications.
        return TitleAnalysis(t, family, "unspecified", None, "MAYBE", tuple(codes), track)

    # No technical token anywhere in the title: not an ambiguous match, just a
    # different job. Warehouse, retail and logistics postings land here.
    return TitleAnalysis(t, family, "unspecified", None, "NO", tuple(codes), track)
