"""Data-domain job title classifier — tuned for commercial analytics roles.

Scoring tiers:
  Tier 1 (score 92) — core sweet spot: Data Analyst, BI Analyst, Analytics Engineer
  Tier 2 (score 78) — strong fit: Data Engineer, Product Analyst, ETL/Warehouse roles
  Tier 3 (score 55) — review manually: Data Scientist, Quant Analyst, Business Analyst
  Tier 4 (score  0) — profile mismatch: ML Engineer, LLM Engineer, Applied Scientist, etc.

Labels:
  yes   (score 70–100) — alert immediately
  maybe (score 40–69)  — alert but review
  no    (score  0–39)  — suppressed
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Tier 1 — Core sweet spot  →  score 92
# ---------------------------------------------------------------------------
TIER_1_ROLES = [
    "data analyst",
    "data analytics",
    "analytics engineer",
    "analytics analyst",
    "business intelligence analyst",
    "bi analyst",
    "business intelligence engineer",
    "bi engineer",
    "bi developer",
    "intelligence analyst",
    "commercial analyst",
    "revenue analyst",
    "financial analyst",
    "reporting analyst",
    "insights analyst",
    "decision science analyst",
    "data governance",
    "data quality analyst",
    "data quality engineer",
    "data management analyst",
    "data operations analyst",
    "data steward",
    "data catalog",
]

# ---------------------------------------------------------------------------
# Tier 2 — Strong fit, slightly off-centre  →  score 78
# ---------------------------------------------------------------------------
TIER_2_ROLES = [
    "data engineer",
    "data engineering",
    "product analyst",
    "growth analyst",
    "marketing analyst",
    "operations analyst",
    "data warehouse engineer",
    "data warehousing",
    "dwh engineer",
    "etl engineer",
    "etl developer",
    "elt engineer",
    "data modeler",
    "data modeling",
    "data platform engineer",
    "data infrastructure engineer",
    "data reliability engineer",
    "data architect",
    "analytics architect",
    "data platform",
    "data infrastructure",
    "analytics consultant",
    "data consultant",
    "data advisor",
    "insights engineer",
    "clinical data analyst",
]

# ---------------------------------------------------------------------------
# Tier 3 — Visible but review manually  →  score 55
# ---------------------------------------------------------------------------
TIER_3_ROLES = [
    "data scientist",
    "data science",
    "decision scientist",
    "quantitative analyst",
    "quant analyst",
    "statistical analyst",
    "statistical modeler",
    "forecasting analyst",
    "research analyst",
    "business analyst",
    "operations research",
    "ai analyst",
    "ai scientist",
]

# ---------------------------------------------------------------------------
# Tier 4 — NOT your profile  →  score 0, never alert
# ---------------------------------------------------------------------------
PROFILE_MISMATCH = [
    "machine learning engineer",
    "ml engineer",
    "mlops engineer",
    "ml platform engineer",
    "applied scientist",
    "research scientist",
    "ai engineer",
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
    "feature engineer",
    "ai data",
]

# ---------------------------------------------------------------------------
# Weak data signals — score 40 (borderline maybe)
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
    "generative ai",
    "gen ai",
    "large language model",
    "llm",
    "nlp",
    "business intelligence analyst",
]

# Keep DATA_STRONG as an alias so any external imports don't break
DATA_STRONG = TIER_1_ROLES + TIER_2_ROLES

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

    # ── PROFILE MISMATCH: ML/LLM/AI engineering roles — score 0, never alert ─
    for phrase in PROFILE_MISMATCH:
        if phrase in t:
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

    # ── Tiered scoring ──────────────────────────────────────────────────────
    if any(p in t for p in TIER_1_ROLES):
        score = 92
    elif any(p in t for p in TIER_2_ROLES):
        score = 78
    elif any(p in t for p in TIER_3_ROLES):
        score = 55
    elif any(p in t for p in DATA_WEAK):
        score = 40
    else:
        return ClassifyResult(score=0, label="no")
    # ───────────────────────────────────────────────────────────────────────

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
