"""Role relevance — "is this job in the candidate's career universe at all?".

This answers a question that is prior to, and independent of, "how well does
the resume match". Before this module existed the two were conflated, and the
consequence was measurable: `Plumbing Engineer I` reached STRONG and
`AV Events Engineer` reached STRONG, because the pipeline had no way to say
"this is a different occupation" — only "this is technical, and the resume
does not overlap much".

Why resume similarity cannot be the gate
----------------------------------------
It already wasn't the problem. Traced in production, `Plumbing Engineer I`
scored `resume_match = 2`, the hybrid matcher returned zero EXACT, zero
EQUIVALENT, zero SEMANTIC and zero RELATED_ONLY, and the ontology extracted
**zero** canonical skills from its 7,310-character JD. The matcher was right.
The job still reached the board, because nothing downstream required skill
evidence to exist before awarding a fit score. Raising a similarity threshold
would not have caught it — the similarity was already ~0.

The generic-word problem
------------------------
`_TECHNICAL_HINTS` in titles.py contains the bare token "engineer", so every
occupation on earth with Engineer in its title becomes `technical_other`:
Plumbing Engineer, Broadcast Engineer, Operating Engineer, Solar Operations
Engineer, Transportation Engineer, Power System Protection Engineer. A word
that admits all of those is not evidence of anything.

So relevance is decided by *occupational domain* tokens, split three ways:

  POSITIVE   data / ML / software domain nouns — "data", "analytics",
             "machine learning", "backend". These establish the universe.
  GENERIC    "engineer", "systems", "operations", "analysis", "technical",
             "design", "project", "infrastructure". Technical-sounding and
             occupationally empty. These may NEVER establish relevance alone.
  NEGATIVE   other occupational domains — "plumbing", "hvac", "avionics",
             "asic", "clinical". Domain nouns, not job titles, so they
             generalise: "plumbing" catches Plumbing Engineer, Plumbing
             Designer and Plumbing Project Engineer without a title list.

The decisive rule, and the reason "AV" is safe
----------------------------------------------
**A negative anchor fires only when the title carries no positive anchor.**

Production contains both `AV Events Engineer` (audio-visual, out of scope) and
`Data Scientist - AV Metrics & Evaluation Analytics` (autonomous vehicles, a
genuine Data Scientist role). A literal "av" deny-list would destroy the
second. Under the ordering rule the second is protected by "data", "scientist"
and "analytics" before "av" is ever consulted. Every negative anchor is
similarly subordinate to positive evidence, which is what keeps the gate from
becoming a deny-list.

Decision order
--------------
  1. positive title anchor           -> TARGET / ADJACENT
  2. negative title anchor           -> OUT_OF_SCOPE
  3. titles.py analyst family        -> TARGET / ADJACENT   (corroboration)
  4. generic title + usable JD:
        >= 2 technical skills        -> ADJACENT
        other-occupation evidence    -> OUT_OF_SCOPE
        otherwise                    -> AMBIGUOUS
  5. nothing to go on                -> AMBIGUOUS

Absence of evidence is not evidence of absence
----------------------------------------------
The first version of step 4 blocked any generic-titled job whose JD named zero
technologies. Measured, that was wrong: 240 of 496 generic-titled JDs in
production name none, including `Product Analyst`, `Operations Research
Analyst`, `Business Analyst II`, and a Scientific Program Analyst role the
owner had asked for a resume for. The ontology is a tech-stack vocabulary;
business-language JDs sail past it. Zero technologies is *missing* evidence,
which the brief maps to AMBIGUOUS, not to a hard block.

A hard OUT_OF_SCOPE from the JD therefore needs *positive evidence of another
occupation*: at least two distinct other-occupation anchors, drawn only from
anchors measured to be rare in genuine target JDs (see `_BOILERPLATE_PRONE`).
"""
from __future__ import annotations

import re
from dataclasses import dataclass

TARGET = "TARGET"
ADJACENT = "ADJACENT"
AMBIGUOUS = "AMBIGUOUS"
OUT_OF_SCOPE = "OUT_OF_SCOPE"

RELEVANCE_STATES = (TARGET, ADJACENT, AMBIGUOUS, OUT_OF_SCOPE)

# A JD shorter than this is a stub (a location line, a benefits blurb), not a
# description, and must not be read as evidence of anything either way.
MIN_USABLE_JD = 400

# Below this many technical skills a generic-titled JD cannot establish that
# the role is technical *in the candidate's sense*. One stray token is not a
# technical universe.
MIN_JD_SKILLS_FOR_ADJACENT = 2

# Distinct other-occupation anchors a JD must show before its evidence alone
# can block a job. Two, not one: a single mention ("our facilities team")
# happens in genuine postings; two independent domain terms rarely do.
MIN_JD_OCCUPATION_ANCHORS = 2


# ---------------------------------------------------------------------------
# Occupational vocabularies
# ---------------------------------------------------------------------------
# Words that sound technical and mean nothing occupationally. Not consulted by
# the decision — the decision only ever looks for domain anchors — but listed
# so "does this title carry real domain evidence?" has an explicit, testable
# answer: a title made only of these words has none.
GENERIC_TOKENS: frozenset[str] = frozenset({
    "engineer", "engineers", "engineering", "system", "systems", "analysis",
    "analyst", "analysts", "analytical", "operations", "operational",
    "technical", "technology", "technologist", "process", "design", "designer",
    "testing", "test", "project", "program", "specialist", "associate",
    "professional", "consultant", "coordinator", "scientist", "science",
    "solutions", "solution", "applications", "application", "support",
    "integration", "implementation", "product", "infrastructure", "senior",
    "junior", "lead", "staff", "principal", "graduate", "intern", "level",
    "entry", "i", "ii", "iii", "iv", "new", "grad", "early", "career",
    "remote", "hybrid",
})

# Domain nouns that DO establish the candidate's universe. Multi-word entries
# are matched as phrases.
DATA_ANCHORS: tuple[str, ...] = (
    "data", "analytics", "analytic", "bi", "business intelligence",
    "reporting", "insight", "insights", "warehouse", "warehousing",
    "etl", "elt", "database", "sql", "statistics", "statistical",
    "quantitative", "econometric", "dashboard", "tableau", "power bi",
    "looker", "snowflake", "databricks", "spark", "hadoop", "dbt",
)

ML_ANCHORS: tuple[str, ...] = (
    "machine learning", "ml", "ai", "artificial intelligence", "deep learning",
    "nlp", "natural language", "llm", "genai", "generative ai",
    "data science", "data scientist", "applied scientist", "research scientist",
    "mlops", "recommendation", "recommender", "personalization",
)

# "infrastructure" is deliberately absent. The first measurement had it here
# and it admitted `Controls Engineer, Supercomputer Infrastructure` and
# `Operations Engineer (Physical Infrastructure)` — facilities roles — at HIGH,
# by suppressing their negative anchors. It is a generic word in exactly the
# sense "engineer" is. Genuine software infrastructure titles carry another
# anchor anyway: "ML Infrastructure" has "ml", "Cloud Infrastructure" has
# "cloud", "Data Infrastructure" has "data".
SOFTWARE_ANCHORS: tuple[str, ...] = (
    "software", "backend", "back end", "back-end", "frontend", "front end",
    "front-end", "full stack", "fullstack", "full-stack", "web", "api",
    "platform", "cloud", "distributed", "devops", "sde", "swe", "developer",
    "programmer", "microservices",
)

# Other occupational families. Domain nouns only — deliberately NOT job titles,
# so an unseen title in the same occupation is still caught. Subordinate to
# every positive anchor above.
NEGATIVE_ANCHORS: tuple[str, ...] = (
    # building trades / facilities
    "plumbing", "plumber", "hvac", "mechanical", "electrical", "civil",
    "structural", "structures", "construction", "facilities", "facility",
    "building", "architectural", "piping", "wiring", "welding", "machining",
    "millwright", "janitorial", "landscaping", "roofing", "solar",
    "refrigeration", "boiler", "elevator",
    # AV / broadcast / events
    "audio visual", "audio-visual", "av", "broadcast", "videography",
    "lighting", "staging", "event", "events",
    # aerospace / defence / automotive hardware
    "avionics", "aerospace", "spacecraft", "aircraft", "airframe", "flight",
    "satcom", "propulsion", "aerodynamics", "stress", "thermal", "payload",
    "missile", "munitions", "vehicle dynamics", "powertrain",
    # semiconductor / electronics hardware
    "asic", "vlsi", "rtl", "silicon", "semiconductor", "wafer", "foundry",
    "pcb", "analog", "photonics", "optical", "rf", "radio frequency",
    "antenna", "microelectronics", "lithography", "packaging",
    # industrial / process / energy
    "corrosion", "metallurg", "chemical", "petroleum", "drilling",
    "pipeline integrity", "controls engineer", "instrumentation",
    "reliability lab", "commissioning", "hse", "ehs", "transportation",
    "logistics", "warehouse operations", "supply chain", "procurement",
    # life sciences / clinical
    "clinical", "nursing", "nurse", "pharmac", "laboratory", "biolog",
    "chemistry", "formulation", "toxicolog", "histolog", "phlebotom",
    "medical device", "postmarket", "regulatory affairs",
    # earth / environment
    "geolog", "environmental", "surveying", "hydrolog", "seismic",
    # non-technical functions
    "legal", "paralegal", "attorney", "compliance", "audit", "sales",
    "account executive", "recruiting", "recruiter", "payroll", "culinary",
    "hospitality", "custodial", "security guard", "physical security",
    # data-centre facilities work. "Data" here names a building, not a
    # discipline — see `_DATA_CENTRE_RE` below.
    "data center", "data centre", "cabling", "foreman", "technician",
    # civil / transportation infrastructure and life-safety engineering — the
    # single most common out-of-scope family in production (olsson,
    # alfredbenesch, atwellgroup, apexcompanies, forgen, jensenhughes).
    # `Entry-Level Bridge Engineer` reached APPLY_FIRST 90 before this gate.
    "bridge", "bridges", "roadway", "highway", "geotechnical", "wastewater",
    "stormwater", "land development", "substation", "traffic",
    "fire protection", "fire alarm", "physical infrastructure",
    # semiconductor verification and test — the occupation, not the tools.
    # Measured over the frozen corpus: 32 titles, 29 out of scope, zero in the
    # positive-control set. "ML ASIC Design Engineer" is caught here; a data
    # role that merely mentions verification is protected by its own positive
    # anchor, as every negative anchor is.
    "design verification", "formal verification", "functional verification",
    "pre-silicon", "post-silicon", "physical design", "dft", "failure analysis",
    "static timing", "signal integrity", "mixed-signal", "mixed signal",
    # supplier / industrialisation engineering. titles.py already rejects
    # "industrial engineer" and "process engineer" as phrases; these are the
    # morphological variants it misses ("Supplier Industrialization Engineer").
    "supplier engineer", "supplier development", "industrialization",
    "industrialisation",
)

# Anchors too common in *genuine* data/ML/software JDs to count as JD-side
# evidence. Measured over 862 production JDs whose titles are in a target
# family: every anchor here appears in >= 1.0% of them — "building" in 66.5%
# ("building scalable systems"), "legal" 20.8% and "compliance" 14.3% (EEO
# boilerplate), "civil" 3.5% ("civil rights"). They remain valid in titles,
# where "Civil Design Engineer" means what it says.
_BOILERPLATE_PRONE: frozenset[str] = frozenset({
    "building", "recruiting", "recruiter", "legal", "compliance", "events",
    "event", "structures", "sales", "architectural", "logistics", "audit",
    "supply chain", "transportation", "civil", "stress", "electrical",
    "clinical", "construction", "silicon", "environmental", "payroll",
    "flight", "aerospace", "data center", "instrumentation", "av",
})

# Added to NEGATIVE_ANCHORS after the JD frequency measurement above, so their
# JD-side frequency is unmeasured. Title-only until someone measures them —
# "bridge the gap" and "web traffic" are exactly how genuine JDs would trip
# them.
_TITLE_ONLY: frozenset[str] = frozenset({
    "bridge", "bridges", "roadway", "highway", "geotechnical", "wastewater",
    "stormwater", "land development", "substation", "traffic",
    "fire protection", "fire alarm", "physical infrastructure",
    "design verification", "formal verification", "functional verification",
    "pre-silicon", "post-silicon", "physical design", "dft", "failure analysis",
    "static timing", "signal integrity", "mixed-signal", "mixed signal",
    "supplier engineer", "supplier development", "industrialization",
    "industrialisation",
})

JD_STRONG_ANCHORS: tuple[str, ...] = tuple(
    a for a in NEGATIVE_ANCHORS if a not in _BOILERPLATE_PRONE and a not in _TITLE_ONLY)

# Canonical skills excluded from the *relevance* count only — the matcher is
# untouched. This began as a workaround for extraction noise ("r" from "R&D"
# and single letters, "sigma" from "Six Sigma"). The ontology now context-gates
# both, so every remaining hit is the R language or Sigma Computing, and
# "sigma" has left the set: it decides no production row.
#
# "r" stays for a different reason. R is the analysis language of bench
# science, so a genuine "Python or R" is weak evidence of the candidate's
# occupation. Measured over 4,611 production JDs after the gate, the only
# generic-titled rows R would decide are a Reliability Engineer and three lab
# Scientists (LNP drug product, cell signalling, neurodegeneration); counting
# it lifts three of them from OUT_OF_SCOPE and one from AMBIGUOUS to ADJACENT.
_WEAK_RELEVANCE_SKILLS: frozenset[str] = frozenset({"r"})

# Low-level / systems specialisations. These ARE software or ML work, so they
# are never OUT_OF_SCOPE — but they are a different discipline from applied
# data / ML / product engineering, and the candidate's evidence does not reach
# them. They cap relevance at ADJACENT so the role stays visible and reviewable
# without displacing genuine target roles at the top of the board.
SPECIALISATION_ANCHORS: tuple[str, ...] = (
    "compiler", "kernel", "firmware", "embedded", "driver", "drivers",
    "bootloader", "assembly", "verilog", "vhdl", "fpga", "hardware",
    "operating system", "real time", "real-time", "cryptograph",
)

# "Data Center Cabling Foreman" is in production. Read naively its title
# carries the positive anchor "data" and would be admitted — the mirror image
# of the generic-word problem this module exists to fix. The phrase names a
# facility, so it is removed before positive evidence is collected, and is
# itself a negative anchor. "Software Engineer, Data Center" still passes: its
# positive evidence is "software", which is untouched.
_DATA_CENTRE_RE = re.compile(r"\bdata\s+cent(?:er|re)s?\b")

# Human data-annotation gig work: "Freelance AI Trainer Project", AI tutors,
# voice actors and language specialists recorded for model training. It is an
# occupation in its own right — crowdsourced labelling — and it is not AI
# engineering. Its titles carry the positive anchor "ai" for the same reason
# "Data Center Cabling Foreman" carries "data": the token names the subject
# matter being worked ON, not the discipline of the worker.
#
# Unlike the data-centre case, stripping the phrase is not enough — the rest of
# the title supplies its own anchors ("Web Development Specialist - Freelance
# AI Trainer Project" still has "web"). So this family is allowed to outrank
# positive evidence, but ONLY when the title classifier has not independently
# placed the role in one of the candidate's families. That guard is what keeps
# `Jr. AI Engineer - Data Annotation` — a real engineering job building
# annotation tooling — out of the block.
_ANNOTATION_WORK_RE = re.compile(
    r"\bai\s*[-/]?\s*(?:trainer|tutor|coach|teacher|instructor)s?\b"
    r"|\bdata\s+(?:annotat\w+|labell?\w+)\b"
    r"|\b(?:annotation|labell?ing)\s+(?:specialist|associate|analyst)\b"
    r"|\bvoice\s+actors?\b"
    r"|\btranscription(?:ist)?s?\b",
    re.IGNORECASE)

_WORD = re.compile(r"[a-z0-9+#.]+")


def _tokens(text: str) -> list[str]:
    return _WORD.findall((text or "").lower())


def _phrase_present(text: str, phrase: str) -> bool:
    """Whole-token phrase match, so 'ml' does not fire inside 'html'."""
    return re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text) is not None


def _hits(text: str, anchors: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(a for a in anchors if _phrase_present(text, a))


@dataclass(frozen=True)
class RelevanceResult:
    """Why this job is, or is not, in the candidate's career universe."""
    state: str
    reasons: tuple[str, ...] = ()
    positive_anchors: tuple[str, ...] = ()
    negative_anchors: tuple[str, ...] = ()
    specialisation_anchors: tuple[str, ...] = ()
    jd_skill_count: int = 0
    jd_usable: bool = False
    evidence: str = ""          # "title" | "title+jd" | "title-family" | "jd" | "none"
    jd_negative_anchors: tuple[str, ...] = ()

    @property
    def blocks(self) -> bool:
        return self.state == OUT_OF_SCOPE

    @property
    def rankable(self) -> bool:
        """May enter normal resume-match ranking."""
        return self.state in (TARGET, ADJACENT)

    def explain(self) -> str:
        bits = [f"role relevance: {self.state}"]
        if self.positive_anchors:
            bits.append(f"domain evidence: {', '.join(self.positive_anchors[:5])}")
        if self.evidence == "title-family":
            bits.append("family recognised by the title classifier")
        if self.negative_anchors:
            bits.append(f"other occupation: {', '.join(self.negative_anchors[:4])}")
        if self.jd_negative_anchors:
            bits.append(f"JD describes: {', '.join(self.jd_negative_anchors[:4])}")
        if self.specialisation_anchors:
            bits.append(f"specialisation: {', '.join(self.specialisation_anchors[:3])}")
        if self.evidence in ("jd", "title+jd"):
            bits.append(f"JD technical skills: {self.jd_skill_count}")
        return " · ".join(bits)


def title_domain_evidence(title: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Return (positive, negative, specialisation) anchors found in a title.

    Exposed separately so the audit can ask "what did the title actually say?"
    without re-running the whole decision.
    """
    t = " ".join(_tokens(title))
    t_pos = _DATA_CENTRE_RE.sub(" ", t)
    positive = (_hits(t_pos, DATA_ANCHORS) + _hits(t_pos, ML_ANCHORS)
                + _hits(t_pos, SOFTWARE_ANCHORS))
    negative = _hits(t, NEGATIVE_ANCHORS)
    special = _hits(t, SPECIALISATION_ANCHORS)
    return positive, negative, special


def _jd_skill_count(jd_text: str) -> int:
    """Technical skills the existing ontology finds in the JD, minus the
    occupationally weak ones (`_WEAK_RELEVANCE_SKILLS`).

    Uses the production extractor rather than a private word list, so the gate
    tracks the ontology automatically and cannot drift away from what the
    matcher sees.
    """
    if not jd_text:
        return 0
    try:
        from ..matching import ontology
        skills = set(ontology.extract_canonical_skills(jd_text))
    except Exception:
        return 0
    return len(skills - _WEAK_RELEVANCE_SKILLS)


def jd_occupation_evidence(jd_text: str) -> tuple[str, ...]:
    """Other-occupation anchors in a JD, restricted to boilerplate-safe ones."""
    return _hits(" ".join(_tokens(jd_text)), JD_STRONG_ANCHORS) if jd_text else ()


def analyze_relevance(title: str, jd_text: str = "", *,
                      role_family: str = "") -> RelevanceResult:
    """Decide whether a job is in the candidate's career universe.

    `role_family` is the existing `titles.analyze_title` verdict. Its analyst
    families corroborate relevance (step 3); `technical_other` and `unknown`
    carry no weight at all — they are exactly the buckets that produced the
    false positives.
    """
    from .titles import TARGET_FAMILIES, SECONDARY_FAMILIES, ADJACENT_FAMILIES

    positive, negative, special = title_domain_evidence(title)
    jd_usable = len(jd_text or "") >= MIN_USABLE_JD
    n_skills = _jd_skill_count(jd_text) if jd_usable else 0
    # `data_analytics` is SECONDARY in titles.py — a Phase 1 *scoring* standing
    # (no family bonus) — but src/profile.py lists "Data Analyst" among the
    # owner's target_roles, and it is the family the resume matches best.
    # Profile config is authoritative for relevance.
    in_universe = role_family in TARGET_FAMILIES or role_family in SECONDARY_FAMILIES

    # ── 0. Human data-annotation gig work ──────────────────────────────────
    # Checked before positive evidence because the positive token in these
    # titles describes the material being labelled, not the occupation.
    if not in_universe and _ANNOTATION_WORK_RE.search(" ".join(_tokens(title))):
        return RelevanceResult(
            OUT_OF_SCOPE,
            ("Title names human data-annotation / AI-trainer gig work, and the "
             "title classifier did not place the role in a candidate family",),
            (), ("ai trainer / data annotation gig work",), special,
            n_skills, jd_usable, "title")

    # ── 1. Positive title evidence wins, and suppresses negative anchors ────
    # This ordering is what lets "Data Scientist - AV Metrics" survive while
    # "AV Events Engineer" does not.
    if positive:
        if special:
            return RelevanceResult(
                ADJACENT,
                ("Domain matches, but the role is a low-level / systems "
                 "specialisation the candidate's evidence does not cover",),
                positive, (), special, n_skills, jd_usable, "title")
        return RelevanceResult(
            TARGET if in_universe else ADJACENT,
            ("Title carries candidate-domain evidence",),
            positive, (), (), n_skills, jd_usable,
            "title+jd" if jd_usable else "title")

    # ── 2. No positive evidence: another occupation blocks ──────────────────
    if negative:
        return RelevanceResult(
            OUT_OF_SCOPE,
            ("Title names a different occupational domain and carries no "
             "data / ML / software domain evidence",),
            (), negative, special, n_skills, jd_usable, "title")

    # ── 3. The existing title classifier recognised an analyst family ───────
    # Its phrase lists know "product analyst" and "operations research
    # analyst"; the anchor lists above deliberately do not duplicate them.
    if in_universe or role_family in ADJACENT_FAMILIES:
        return RelevanceResult(
            TARGET if in_universe else ADJACENT,
            (f"Title classifier places the role in the '{role_family}' family",),
            (), (), special, n_skills, jd_usable, "title-family")

    # ── 4. Generic-only title: the JD decides, when there is one ────────────
    if jd_usable:
        if n_skills >= MIN_JD_SKILLS_FOR_ADJACENT:
            return RelevanceResult(
                ADJACENT,
                ("Title is occupationally generic; the JD supplies the "
                 "technical evidence",),
                (), (), special, n_skills, True, "jd")
        jd_neg = jd_occupation_evidence(jd_text)
        if len(jd_neg) >= MIN_JD_OCCUPATION_ANCHORS:
            return RelevanceResult(
                OUT_OF_SCOPE,
                ("Generic title, no technical evidence in the JD, and the JD "
                 "describes a different occupation",),
                (), (), special, n_skills, True, "jd", jd_neg)
        return RelevanceResult(
            AMBIGUOUS,
            ("Title is occupationally generic and the JD establishes neither "
             "the candidate's domain nor a different one",),
            (), (), special, n_skills, True, "jd", jd_neg)

    # ── 5. Nothing to go on ────────────────────────────────────────────────
    return RelevanceResult(
        AMBIGUOUS,
        ("No domain evidence in the title and no usable job description",),
        (), (), special, 0, False, "none")
