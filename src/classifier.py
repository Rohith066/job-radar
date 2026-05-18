"""Job title classifier — dual-track: Data Engineering + AI Engineering.

Rohith is now targeting TWO tracks simultaneously:
  Track A — Analytics / Platform Data Engineering
             (dbt, Airflow, Snowflake, PySpark, medallion lakehouse)
  Track B — LLM / Retrieval / Applied AI Engineering
             (RAG, FAISS, LangChain, Text2SQL, multi-agent systems)

Scoring tiers (both tracks share the same thresholds):
  Tier 1 (score 92) — primary targets for either track
  Tier 2 (score 78) — strong secondary fit
  Tier 3 (score 55) — worth reviewing manually
  Tier 4 (score  0) — PROFILE_MISMATCH: pure non-data roles, never alert

Labels:
  yes   (score 70–100) — alert immediately
  maybe (score 40–69)  — alert but review
  no    (score  0–39)  — suppressed
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Tier 1 — Primary targets  →  score 92
# ---------------------------------------------------------------------------

# Track A: Analytics & Platform Data Engineering
TIER_1_DE: list[str] = [
    "analytics engineer",
    "analytics engineering",
    "data engineer",
    "data engineering",
    "platform data engineer",
    "dbt engineer",
    "dbt developer",
    "dbt analyst",
    "data platform engineer",
    "etl engineer",
    "etl developer",
    "elt engineer",
    "data pipeline engineer",
    "data infrastructure engineer",
    "data reliability engineer",
    "data warehouse engineer",
    "data warehousing engineer",
    "dwh engineer",
    "data modeler",
    "data modeling engineer",
    "data architect",
    "analytics architect",
]

# Track B: LLM / Retrieval / Applied AI Engineering
TIER_1_AI: list[str] = [
    "llm engineer",
    "llm developer",
    "ai engineer",
    "applied ai engineer",
    "generative ai engineer",
    "gen ai engineer",
    "retrieval engineer",
    "rag engineer",
    "nlp engineer",
    "conversational ai engineer",
    "foundation model engineer",
    "ai data engineer",
    "prompt engineer",
    "ai/ml engineer",
    "ml systems engineer",
    "ai systems engineer",
    "llm systems engineer",
    "ai platform engineer",
    "machine learning platform engineer",
]

TIER_1_ROLES: list[str] = TIER_1_DE + TIER_1_AI

# ---------------------------------------------------------------------------
# Tier 2 — Strong secondary fit  →  score 78
# ---------------------------------------------------------------------------
TIER_2_ROLES: list[str] = [
    # Data analysis & BI — valid with DE background
    "data analyst",
    "data analytics",
    "business intelligence analyst",
    "bi analyst",
    "business intelligence engineer",
    "bi engineer",
    "bi developer",
    "analytics analyst",
    "insights analyst",
    "reporting analyst",
    # ML / AI adjacent
    "machine learning engineer",
    "ml engineer",
    "applied scientist",
    "data scientist",
    "data science",
    "mlops engineer",
    "ml platform engineer",
    # Data ops / quality
    "data quality engineer",
    "data governance",
    "data quality analyst",
    "data operations analyst",
    "data management analyst",
    "data steward",
    "data catalog",
    "data reliability",
    # Analytics roles
    "product analyst",
    "growth analyst",
    "marketing analyst",
    "operations analyst",
    "commercial analyst",
    "revenue analyst",
    "financial analyst",
    "decision science analyst",
    # Data platform / infrastructure
    "data platform",
    "data infrastructure",
    "data consultant",
    "analytics consultant",
    "insights engineer",
]

# ---------------------------------------------------------------------------
# Tier 3 — Visible but review manually  →  score 55
# ---------------------------------------------------------------------------
TIER_3_ROLES: list[str] = [
    "business analyst",
    "research scientist",
    "research engineer",
    "decision scientist",
    "quantitative analyst",
    "quant analyst",
    "statistical analyst",
    "forecasting analyst",
    "ai analyst",
    "ai scientist",
    "operations research",
    "clinical data analyst",
]

# ---------------------------------------------------------------------------
# Tier 4 — Genuinely NOT the profile  →  score 0, never alert
# Pure non-data roles only — LLM/AI/ML have been MOVED to Tier 1/2
# ---------------------------------------------------------------------------
PROFILE_MISMATCH: list[str] = [
    "computer vision engineer",
    "computer vision scientist",
    "robotics engineer",
    "hardware engineer",
    "embedded engineer",
    "electrical engineer",
    "mechanical engineer",
    # Note: LLM/AI/ML roles are now Tier 1/2 — do NOT add them back here
]

# ---------------------------------------------------------------------------
# Weak data signals — score 40 (borderline maybe)
# ---------------------------------------------------------------------------
DATA_WEAK: list[str] = [
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
    "retrieval",
    "embedding",
    "vector",
    "rag",
    "langchain",
    "openai",
    "anthropic",
]

# Keep alias so any external imports don't break
DATA_STRONG = TIER_1_ROLES + TIER_2_ROLES

# ---------------------------------------------------------------------------
# Hard excludes — non-data roles → immediate "no"
# ---------------------------------------------------------------------------
HARD_EXCLUDES: list[str] = [
    # Pure software engineering (no data / AI modifier)
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
    "embedded software",
    "systems engineer",
    "site reliability",
    "sre",
    "devops",
    "cloud engineer",
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
    # Hardware / manufacturing
    "electrical engineer",
    "mechanical engineer",
    "manufacturing engineer",
    "supply chain",
]

HARD_EXCLUDE_REGEXES: list[str] = [
    r"\bintern(ship)?\b",
    r"\bco[- ]?op\b",
    r"\bcoop\b",
    r"\bapprentice\b",
    r"\bpart[- ]time\b",
]

# ---------------------------------------------------------------------------
# Clearance / citizenship filters — ABSOLUTE, cannot be overridden
# ---------------------------------------------------------------------------
CLEARANCE_EXCLUDE_PHRASES: list[str] = [
    "security clearance", "clearance required", "clearance preferred",
    "clearance eligible", "active clearance", "active secret",
    "secret clearance", "top secret", "ts/sci", "ts sci", "sci clearance",
    "dod clearance", "dod secret", "public trust", "polygraph",
    "us citizen", "u.s. citizen", "must be a citizen",
    "citizenship required", "citizenship eligibility", "must hold clearance",
]

CLEARANCE_EXCLUDE_REGEXES: list[str] = [
    r"\bts[/\s\-]?sci\b",
    r"\btop\s+secret\b",
    r"\bpolygraph\b",
    r"\bpublic\s+trust\b",
    r"\bclearance\b",
    r"\bus\s+citizen",
    r"\bcitizenship\b",
    r"\bsci\b",
]

# ---------------------------------------------------------------------------
# Seniority tokens — clamp score into "maybe" or "no"
# ---------------------------------------------------------------------------
SENIORITY_TOKENS: list[str] = [
    "senior", "sr", "staff", "principal", "lead", "architect",
    "distinguished", "fellow", "director", "manager", "head of",
    "vp", "vice president",
]
VERY_SENIOR = frozenset(["director", "vp", "vice president", "head of", "fellow", "distinguished"])

# ---------------------------------------------------------------------------
# Safety-net overrides — "data" / "ai" / "llm" prefix rescues hard-excluded terms
# ---------------------------------------------------------------------------
DATA_SAFETY_NET_OVERRIDES = frozenset([
    # Data + hard-excluded combos
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
    "data product manager",
    "data program manager",
    "analytics program manager",
    # AI / LLM + hard-excluded combos (AI roles that use "engineer" broadly)
    "ai software engineer",
    "llm software engineer",
    "ml software engineer",
    "ai platform engineer",
    "ai systems engineer",
    "generative ai",
    "gen ai",
    "llm engineer",
    "ai engineer",
    "prompt engineer",
    "ai data engineer",
    "ml systems",
])


@dataclass
class ClassifyResult:
    score: int   # 0-100
    label: str   # "yes" | "maybe" | "no"
    track: str   # "de" | "ai" | "analyst" | "other"


def _norm(title: str) -> str:
    t = (title or "").strip().lower()
    return re.sub(r"\s+", " ", t)


def classify(title: str) -> ClassifyResult:
    """Score and label a job title. Returns track so email can show which resume to use."""
    t = _norm(title)
    if not t:
        return ClassifyResult(score=0, label="no", track="other")

    # ── ABSOLUTE: clearance / citizenship ─────────────────────────────────────
    for phrase in CLEARANCE_EXCLUDE_PHRASES:
        if phrase in t:
            return ClassifyResult(score=0, label="no", track="other")
    for pat in CLEARANCE_EXCLUDE_REGEXES:
        if re.search(pat, t):
            return ClassifyResult(score=0, label="no", track="other")

    # ── PROFILE MISMATCH (pure non-data roles) ─────────────────────────────────
    for phrase in PROFILE_MISMATCH:
        if phrase in t:
            return ClassifyResult(score=0, label="no", track="other")

    # ── Safety-net: data/ai prefix rescues hard-excluded terms ─────────────────
    is_safety_net = any(override in t for override in DATA_SAFETY_NET_OVERRIDES)

    # Hard exclude regexes (internship etc.) — always reject, no override
    for pat in HARD_EXCLUDE_REGEXES:
        if re.search(pat, t):
            return ClassifyResult(score=0, label="no", track="other")

    # Hard exclude phrases — reject unless safety-net override
    if not is_safety_net:
        for phrase in HARD_EXCLUDES:
            if phrase in t:
                return ClassifyResult(score=0, label="no", track="other")

    # ── Tiered scoring ─────────────────────────────────────────────────────────
    track = "other"
    if any(p in t for p in TIER_1_DE):
        score = 92
        track = "de"
    elif any(p in t for p in TIER_1_AI):
        score = 92
        track = "ai"
    elif any(p in t for p in TIER_2_ROLES):
        score = 78
        track = "analyst" if any(w in t for w in ("analyst", "scientist", "bi ")) else "de"
    elif any(p in t for p in TIER_3_ROLES):
        score = 55
        track = "analyst"
    elif any(p in t for p in DATA_WEAK):
        score = 40
        track = "other"
    else:
        return ClassifyResult(score=0, label="no", track="other")

    # Seniority cap
    for tok in SENIORITY_TOKENS:
        if re.search(rf"\b{re.escape(tok)}\b", t):
            if tok in VERY_SENIOR:
                score = min(score, 34)
            else:
                score = min(score, 65)
            break

    score = max(0, min(score, 100))
    label = "yes" if score >= 70 else "maybe" if score >= 40 else "no"
    return ClassifyResult(score=score, label=label, track=track)


def is_match(title: str) -> bool:
    return classify(title).label in ("yes", "maybe")
