"""Data-domain job title classifier — tuned for Data Analyst / Data Scientist / Data Engineer roles.

Scoring:
  yes   (score 70–100) — strong data role match
  maybe (score 40–69)  — data role with seniority or ambiguity; review manually
  no    (score  0–39)  — not a data role (software-only, ops, sales, QA, etc.)

Design rationale
-----------------
Only data-domain roles pass. Pure software engineering, DevOps, QA, security,
PM, sales, and non-data-facing roles all return "no" so the user only sees
Data Analyst, Data Scientist, Data Engineer, ML Engineer, BI Analyst, etc.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Data-domain STRONG includes  →  base score 90
# ---------------------------------------------------------------------------
DATA_STRONG = [
    # Core data roles
    "data analyst",
    "data analytics",
    "data scientist",
    "data science",
    "data engineer",
    "data engineering",
    "analytics engineer",
    "analytics analyst",
    # Business Intelligence
    "business intelligence",
    "bi analyst",
    "bi engineer",
    "bi developer",
    "bi developer",
    "intelligence analyst",
    # Machine Learning / AI (data side)
    "machine learning engineer",
    "ml engineer",
    "applied scientist",
    "research scientist",
    "decision scientist",
    "ai data",
    # Quantitative / Statistical
    "quantitative analyst",
    "quant analyst",
    "statistical analyst",
    "statistical modeler",
    "forecasting analyst",
    # Platform / Infrastructure / Quality (data)
    "data platform engineer",
    "data infrastructure engineer",
    "data reliability engineer",
    "data quality engineer",
    "data quality analyst",
    "data governance",
    "data management analyst",
    "data operations analyst",
    "data architect",
    "analytics architect",
    # ETL / Warehouse
    "etl engineer",
    "etl developer",
    "elt engineer",
    "data warehouse engineer",
    "data warehousing",
    "dwh engineer",
    "data modeler",
    "data modeling",
    # Insights / Reporting
    "insights analyst",
    "insights engineer",
    "reporting analyst",
    "product analyst",
    "growth analyst",
    "marketing analyst",
    "financial analyst",
    "operations analyst",
    "clinical data analyst",
    "research analyst",
    # AI/ML Data Engineering
    "feature engineer",
    "mlops engineer",
    "ml platform engineer",
    "ai engineer",
    # LLM / Generative AI (modern AI roles)
    "llm engineer",
    "llm data",
    "prompt engineer",
    "generative ai engineer",
    "gen ai engineer",
    "nlp engineer",
    "natural language processing engineer",
    "natural language processing scientist",
    "computer vision engineer",
    "computer vision scientist",
    "multimodal",
    "foundation model",
    # Consulting / advisory (data-focused)
    "analytics consultant",
    "data consultant",
    "data advisor",
]

# ---------------------------------------------------------------------------
# Data-domain WEAK includes  →  base score 55 (needs review)
# ---------------------------------------------------------------------------
DATA_WEAK = [
    "analytics",
    "data",
    "intelligence",
    "insights",
    "tableau",
    "power bi",
    "snowflake",
    "spark",
    "databricks",
    "warehouse",
    "pipeline",
    "etl",
    "elt",
    "dbt",
    "airflow",
    "kafka",
    "flink",
    "hadoop",
    # AI/LLM weak signals
    "generative ai",
    "gen ai",
    "large language model",
    "llm",
    "nlp",
    "ai analyst",
    "ai scientist",
    # Business/operations data-adjacent
    "business analyst",
    "business intelligence analyst",
    "operations research",
]

# ---------------------------------------------------------------------------
# Hard excludes — non-data roles → immediate "no"
# ---------------------------------------------------------------------------
HARD_EXCLUDES = [
    # Pure software engineering (no data modifier)
    "software engineer",
    "software developer",
    "software development engineer",
    "frontend engineer",
    "front-end engineer",
    "front end engineer",
    "backend engineer",
    "back-end engineer",
    "back end engineer",
    "full stack engineer",
    "fullstack engineer",
    "full-stack engineer",
    "mobile engineer",
    "ios engineer",
    "android engineer",
    "embedded engineer",
    "embedded software",
    "systems engineer",
    "site reliability",
    "sre",
    "devops",
    "platform engineer",
    "cloud engineer",
    "infrastructure engineer",
    "network engineer",
    "security engineer",
    "cybersecurity",
    "penetration tester",
    # QA / Testing
    "quality assurance",
    "qa engineer",
    "qa analyst",
    "test engineer",
    "quality engineer",
    "validation engineer",
    # Management / non-technical
    "product manager",
    "program manager",
    "project manager",
    "engineering manager",
    "scrum master",
    "agile coach",
    # Sales / Marketing / HR
    "sales",
    "account executive",
    "account manager",
    "solutions engineer",
    "pre-sales",
    "recruiter",
    "talent acquisition",
    "human resources",
    # Support / Ops
    "customer support",
    "technical support",
    "support engineer",
    "help desk",
    "it support",
    "it administrator",
    "systems administrator",
    "sysadmin",
    "database administrator",
    # Hardware / non-software
    "hardware engineer",
    "electrical engineer",
    "mechanical engineer",
    "manufacturing engineer",
    "supply chain",
]

HARD_EXCLUDE_REGEXES = [
    r"\bintern(ship)?\b",
    r"\bco[- ]?op\b",
    r"\bcoop\b",
    r"\bapprentice\b",
    r"\bpart[- ]time\b",
]

# ---------------------------------------------------------------------------
# Clearance / citizenship filters — ABSOLUTE, cannot be overridden.
# Removes jobs that require security clearance or US citizenship.
# ---------------------------------------------------------------------------
CLEARANCE_EXCLUDE_PHRASES = [
    "security clearance",
    "clearance required",
    "clearance preferred",
    "clearance eligible",
    "active clearance",
    "active secret",
    "secret clearance",
    "top secret",
    "ts/sci",
    "ts sci",
    "sci clearance",
    "dod clearance",
    "dod secret",
    "public trust",
    "polygraph",
    "us citizen",
    "u.s. citizen",
    "must be a citizen",
    "citizenship required",
    "citizenship eligibility",
    "must hold clearance",
]

CLEARANCE_EXCLUDE_REGEXES = [
    r"\bts[/\s\-]?sci\b",       # TS/SCI, TS SCI, TS-SCI
    r"\btop\s+secret\b",         # Top Secret
    r"\bpolygraph\b",            # Polygraph
    r"\bpublic\s+trust\b",       # Public Trust
    r"\bclearance\b",            # any "clearance" in title
    r"\bus\s+citizen",           # US citizen / US citizenship
    r"\bcitizenship\b",          # citizenship requirement
    r"\bsci\b",                  # SCI in title (often paired with TS)
]

# ---------------------------------------------------------------------------
# Seniority tokens — always clamp to "maybe" or "no"
# ---------------------------------------------------------------------------
SENIORITY_TOKENS = [
    "senior", "sr", "staff", "principal", "lead", "architect",
    "distinguished", "fellow", "director", "manager", "head of",
    "vp", "vice president",
]
VERY_SENIOR = frozenset(["director", "vp", "vice president", "head of", "fellow", "distinguished"])

# ---------------------------------------------------------------------------
# "data" safety-net: if the title contains "data" AND a hard-excluded term,
# the "data" wins for these specific combos (e.g. "Data Security Analyst")
# ---------------------------------------------------------------------------
DATA_SAFETY_NET_OVERRIDES = frozenset([
    "data security analyst",
    "data quality engineer",
    "data governance",
    "data management",
    "data operations",
    "data steward",
    "data catalog",
    "data platform engineer",
    "data platform",
    "data infrastructure",
    "data reliability engineer",
    # Product/program roles that are genuinely data-focused
    "data product manager",
    "data program manager",
    "analytics program manager",
    # AI roles that may hit SWE-adjacent hard-excludes
    "generative ai",
    "gen ai",
    "llm engineer",
    "prompt engineer",
    "ai data engineer",
])


@dataclass
class ClassifyResult:
    score: int   # 0-100
    label: str   # "yes" | "maybe" | "no"


def _norm(title: str) -> str:
    t = (title or "").strip().lower()
    return re.sub(r"\s+", " ", t)


def classify(title: str) -> ClassifyResult:
    """Score and label a job title for data-domain relevance."""
    t = _norm(title)
    if not t:
        return ClassifyResult(score=0, label="no")

    # ── ABSOLUTE FILTER: security clearance / citizenship ──────────────────
    # These are checked first and cannot be overridden by any safety-net.
    for phrase in CLEARANCE_EXCLUDE_PHRASES:
        if phrase in t:
            return ClassifyResult(score=0, label="no")
    for pat in CLEARANCE_EXCLUDE_REGEXES:
        if re.search(pat, t):
            return ClassifyResult(score=0, label="no")
    # ───────────────────────────────────────────────────────────────────────

    # Safety-net overrides that start with "data" but hit a hard-exclude phrase
    is_safety_net = any(override in t for override in DATA_SAFETY_NET_OVERRIDES)

    # Hard exclude regexes (internship etc.) — always reject, no override
    for pat in HARD_EXCLUDE_REGEXES:
        if re.search(pat, t):
            return ClassifyResult(score=0, label="no")

    # Hard exclude phrases — reject unless safety-net override
    if not is_safety_net:
        for phrase in HARD_EXCLUDES:
            if phrase in t:
                return ClassifyResult(score=0, label="no")

    # Base score from data-domain keyword match
    strong = any(p in t for p in DATA_STRONG)
    weak = any(p in t for p in DATA_WEAK)

    if not (strong or weak):
        return ClassifyResult(score=0, label="no")

    score = 90 if strong else 55

    # Seniority cap — senior/staff/principal → "maybe"; director/vp → "no"
    for tok in SENIORITY_TOKENS:
        if re.search(rf"\b{re.escape(tok)}\b", t):
            if tok in VERY_SENIOR:
                score = min(score, 34)
            else:
                score = min(score, 65)
            break

    score = max(0, min(score, 100))

    if score >= 70:
        label = "yes"
    elif score >= 40:
        label = "maybe"
    else:
        label = "no"

    return ClassifyResult(score=score, label=label)


def is_match(title: str) -> bool:
    return classify(title).label in ("yes", "maybe")
