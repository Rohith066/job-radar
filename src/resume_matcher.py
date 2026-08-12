"""Resume-vs-JD matching engine.

Scores how well a job description matches your master resume.
Uses two complementary signals then combines them:

  1. Skill match (weight 50%)
     Extract every recognisable skill/tool from the JD, check which
     ones appear in your resume.  Required skills (signals like
     "required", "must have", "you will need") are weighted 2×.

  2. TF-IDF cosine similarity (weight 50%)
     sklearn's TfidfVectorizer on the full texts.
     Picks up context (domain language, verbs, phrasing) that a
     keyword list misses.

Output: ResumeMatchResult with an overall 0-100 score.

Integration points
------------------
  • main.py: called after YES/MAYBE filter, JDs fetched in parallel
  • ML scorer: high match (≥ 70) → synthetic "interested" feedback
    so the ML model bootstraps without waiting for manual input
  • Email / dashboard: shows match % badge on each job card
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Experience requirement extraction
# ---------------------------------------------------------------------------

# Max years of experience we accept. Jobs requiring strictly MORE are dropped.
MAX_EXPERIENCE_YEARS = 4

# Word → digit map for written-out numbers
_WORD_NUMS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}

# Matches patterns like:
#   "5+ years", "5-7 years", "5 to 7 years", "minimum 5 years",
#   "at least 5 years", "five years of experience", "5 years experience"
_EXP_RE = re.compile(
    r"""
    (?:
        # "minimum/at least/minimum of N"
        (?:minimum\s+(?:of\s+)?|at\s+least\s+|(?:a\s+)?minimum\s+of\s+)?
        # digit or word number
        (?P<lo>\d+|one|two|three|four|five|six|seven|eight|nine|ten)
        # optional range "- N" or "to N"
        (?:\s*[-–to]+\s*(?P<hi>\d+|one|two|three|four|five|six|seven|eight|nine|ten))?
        # optional plus
        \s*\+?
        \s*[-–]?\s*
        # "year(s)" must follow within a few words
        (?:years?|yrs?)
        \s*
        (?:of\s+)?
        # must be experience context
        (?:experience|work\s+experience|professional\s+experience|industry\s+experience|
           relevant\s+experience|related\s+experience|hands[\s-]on\s+experience)?
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


def extract_required_experience(jd_text: str) -> Optional[int]:
    """Return the minimum years of experience required by the JD.

    Returns None if no experience requirement is found (job passes through).
    When a range is found (e.g. "3-5 years"), returns the lower bound (3).
    """
    if not jd_text:
        return None

    best: Optional[int] = None
    for m in _EXP_RE.finditer(jd_text):
        lo_raw = m.group("lo") or ""
        lo = _WORD_NUMS.get(lo_raw.lower(), None)
        if lo is None:
            try:
                lo = int(lo_raw)
            except ValueError:
                continue
        # Ignore unrealistically large values (e.g. "10,000 years")
        if lo > 20:
            continue
        # Take the minimum across all mentions — job may list different
        # requirements for different sections; we want the lowest bar
        if best is None or lo < best:
            best = lo
    return best


def experience_passes_filter(jd_text: str, max_years: int = MAX_EXPERIENCE_YEARS) -> tuple[bool, Optional[int]]:
    """Return (passes, required_years).

    passes=True  → job is within our experience range or requirement unknown
    passes=False → job explicitly requires more than max_years
    """
    required = extract_required_experience(jd_text)
    if required is None:
        return True, None
    return required <= max_years, required


# ---------------------------------------------------------------------------
# Skill taxonomy  — comprehensive data/analytics keyword list
# ---------------------------------------------------------------------------
_SKILLS: list[str] = [
    # Languages
    "python", "sql", "r", "scala", "java", "julia", "bash", "shell",
    # Databases / warehouses
    "snowflake", "bigquery", "redshift", "synapse", "databricks",
    "sql server", "mysql", "postgresql", "postgres", "oracle",
    "sqlite", "duckdb", "athena", "hive", "presto", "trino",
    # Cloud
    "aws", "azure", "gcp", "google cloud", "s3", "glue", "lambda",
    "azure data factory", "azure synapse", "azure databricks",
    # ETL / orchestration
    "airflow", "dbt", "spark", "pyspark", "kafka", "flink",
    "hadoop", "nifi", "fivetran", "stitch", "talend",
    "ssis", "informatica", "matillion", "airbyte",
    # BI / visualisation
    "power bi", "tableau", "looker", "qlik", "metabase",
    "superset", "grafana", "plotly", "streamlit", "excel",
    "powerpoint", "google sheets", "data studio", "looker studio",
    # Python libraries
    "pandas", "numpy", "matplotlib", "seaborn", "plotly",
    "scikit-learn", "sklearn", "scipy", "statsmodels", "xgboost",
    "lightgbm", "pytorch", "tensorflow", "keras",
    # Statistics / ML methods
    "regression", "classification", "clustering", "forecasting",
    "time series", "a/b test", "experimentation", "hypothesis test",
    "statistical significance", "confidence interval",
    "logistic regression", "random forest", "gradient boosting",
    "arima", "prophet",
    # Analytics concepts
    "kpi", "metrics", "dashboard", "reporting", "analytics",
    "data modeling", "dimensional modeling", "star schema",
    "data warehouse", "data lake", "data lakehouse",
    "data governance", "data quality", "data lineage",
    "data pipeline", "etl", "elt",
    "cohort analysis", "funnel analysis", "conversion",
    "retention", "churn", "revenue", "cac", "ltv",
    # Soft / domain
    "stakeholder", "presentation", "communication",
    "agile", "scrum", "jira", "confluence",
    "financial analysis", "commercial analytics",
    "product analytics", "marketing analytics",
    # ── LLM / AI Engineering (Track B) ──────────────────────────────────────
    "langchain", "llm", "large language model",
    "rag", "retrieval augmented generation",
    "faiss", "vector database", "vector store", "vector search",
    "embedding", "embeddings", "semantic search",
    "text2sql", "text to sql",
    "openai", "anthropic", "hugging face", "transformers",
    "ollama", "mistral", "llama", "gpt",
    "prompt engineering", "function calling", "multi-agent",
    "chunking", "metadata filtering", "pymupdf",
    "pytorch", "torch",
    "pinecone", "weaviate", "chroma", "qdrant",
    "retrieval", "reranking", "mmr retrieval",
    "document ingestion", "document intelligence",
    "knowledge graph", "knowledge base",
    # ── Data Engineering extras (Track A) ───────────────────────────────────
    "medallion architecture", "bronze silver gold",
    "data contract", "data contracts",
    "dbt cloud", "dbt core", "dbt models", "dbt tests",
    "delta lake", "apache iceberg", "hudi",
    "aws glue", "aws lambda",
    "partitioning", "clustering", "materialized view",
    "stored procedure", "window function",
]
_SKILLS_SET = set(_SKILLS)

# Sentence patterns that signal a skill is REQUIRED (weight × 2)
_REQUIRED_PATTERNS = re.compile(
    r"\b(required|must.have|must.be|you.will.need|essential|"
    r"minimum.requirements?|basic.qualifications?|required.experience|"
    r"required.skills?|you.have|you.bring)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ResumeMatchResult:
    overall_score: int              # 0-100 combined score
    skill_score: int                # 0-100 skill-overlap component
    tfidf_score: int                # 0-100 TF-IDF cosine component
    matched_skills: list[str] = field(default_factory=list)
    missing_skills: list[str] = field(default_factory=list)
    jd_skill_count: int = 0         # total skills found in JD
    has_jd: bool = False            # False when JD was unavailable
    required_experience: Optional[int] = None   # years required per JD; None = not stated
    experience_ok: bool = True      # False → job filtered out (over-experience requirement)
    top_bullets: list[str] = field(default_factory=list)  # top matching resume bullets

    # ── Hybrid matcher additions (see src/matching/) ─────────────────────────
    # Populated when USE_HYBRID_MATCHER is on. Older fields above are kept
    # populated for backward compatibility: `tfidf_score` carries the
    # document-similarity component whichever backend produced it.
    exact_coverage: int = 0         # % of weighted JD skills matched literally
    equivalent_coverage: int = 0    # % matched via alias/acronym/subsumption
    semantic_coverage: int = 0      # % matched via embeddings only
    related_only_skills: list[str] = field(default_factory=list)  # sibling tech, NOT credit
    missing_required: list[str] = field(default_factory=list)     # unsatisfied required skills
    jd_terminology: list = field(default_factory=list)  # (jd_phrase, resume_evidence)
    semantic_used: bool = False     # True when the embedding layer actually ran


# ---------------------------------------------------------------------------
# Resume loading
# ---------------------------------------------------------------------------

_resume_text: Optional[str] = None


def load_resume(path: str) -> str:
    """Load master resume text from file. Caches after first load."""
    global _resume_text
    if _resume_text is not None:
        return _resume_text
    p = Path(path)
    if not p.exists():
        log.warning("Master resume not found at %s — resume matching disabled", path)
        _resume_text = ""
        return ""
    _resume_text = p.read_text(encoding="utf-8").strip()
    log.info("Loaded master resume: %d chars from %s", len(_resume_text), path)
    return _resume_text


def _resume_path_from_env() -> str:
    return os.environ.get("RESUME_PATH", "config/master_resume.txt")


# ---------------------------------------------------------------------------
# Skill extraction helpers
# ---------------------------------------------------------------------------

# Short/ambiguous skills that must match as whole words (avoid "r" matching
# every word, "go"/"c" matching substrings, etc.)
_WORD_BOUNDARY_SKILLS = frozenset({
    "r", "go", "c", "c++", "c#", "java", "scala", "sql", "aws", "gcp",
    "etl", "elt", "llm", "rag", "nlp", "ml", "ai", "bi", "spark", "kafka",
    "git", "gpt", "dbt", "s3", "excel", "hive", "ssis",
})


def _extract_skills_from_text(text: str) -> list[str]:
    """Return all recognised skill tokens found in text (lowercased).

    Short/ambiguous skills are matched on word boundaries to avoid false
    positives like 'r' matching every word containing the letter r.
    """
    t = text.lower()
    found = []
    for skill in _SKILLS:
        if skill in _WORD_BOUNDARY_SKILLS:
            if re.search(rf"(?<![a-z0-9]){re.escape(skill)}(?![a-z0-9])", t):
                found.append(skill)
        elif skill in t:
            found.append(skill)
    return found


def _is_required_context(text: str, skill: str) -> bool:
    """Rough heuristic: check whether the skill appears in a 'required' section."""
    idx = text.lower().find(skill)
    if idx < 0:
        return False
    # Look at the 300 chars before the skill mention
    window = text[max(0, idx - 300): idx]
    return bool(_REQUIRED_PATTERNS.search(window))


# ---------------------------------------------------------------------------
# Resume bullet extraction (for "lead with these" highlights in email)
# ---------------------------------------------------------------------------

# Lines that are resume scaffolding, not achievements — never surface these
_NON_BULLET_RE = re.compile(
    r"@|"                                    # email
    r"\+?\d[\d\-\s().]{7,}|"                  # phone number
    r"linkedin|github|"                       # profile links
    r"\b(coursework|education|summary|technical skills|experience|projects)\b",
    re.IGNORECASE,
)


# Entry headers ("Data Analyst | XIT Solutions | Hyderabad | Jan 2022 - Dec 2023",
# "M.S. Data Analytics Engineering | George Mason University | 2025") and skill
# lines ("Languages: Python, SQL, ...") are structure, not achievements.
_HEADER_LINE_RE = re.compile(
    r"\|"                                                   # pipe-separated header
    r"|^[A-Z][A-Za-z &/]{2,28}:\s"                          # "Languages: ..." skill line
    r"|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4}\b",  # date ranges
    re.IGNORECASE,
)


def _extract_resume_bullets(resume_text: str) -> list[str]:
    """Pull achievement bullet lines from resume text, skipping contact
    headers, section titles, entry headers, and skill dumps."""
    bullets = []
    for line in resume_text.splitlines():
        line = line.strip()
        if not line or len(line) < 28 or len(line) > 260:
            continue
        # Skip contact headers / section titles / profile links / entry headers
        if _NON_BULLET_RE.search(line) or _HEADER_LINE_RE.search(line):
            continue
        # Lines starting with common bullet symbols
        if line[0] in "•-*→▪◦►–":
            clean = line.lstrip("•-*→▪◦►–– ").strip()
            if len(clean) >= 25 and not _NON_BULLET_RE.search(clean):
                bullets.append(clean)
        # Lines with quantifiable metrics (%, $, K, or 2+ digit numbers)
        elif re.search(r"\d+\s*%|\$\s*\d|\d+\s*[Kk]\b|\d{2,}", line):
            bullets.append(line)
    return list(dict.fromkeys(bullets))  # deduplicate, preserve order


def top_resume_bullets(resume_text: str, jd_text: str, n: int = 3) -> list[str]:
    """Return the N resume bullets most semantically relevant to this JD."""
    bullets = _extract_resume_bullets(resume_text)
    if not bullets:
        return []
    jd_words = set(re.findall(r"\b[a-z]{3,}\b", jd_text.lower()))

    def _score(b: str) -> float:
        b_words = set(re.findall(r"\b[a-z]{3,}\b", b.lower()))
        overlap = len(b_words & jd_words)
        has_metric = bool(re.search(r"\d+\s*%|\$\s*\d|\d+\s*[Kk]\b", b))
        return overlap / (len(b_words) + 1) * (1.5 if has_metric else 1.0)

    return sorted(bullets, key=_score, reverse=True)[:n]


# ---------------------------------------------------------------------------
# LinkedIn DM generator
# ---------------------------------------------------------------------------

# Generic terms that read as filler in outreach ("my background in analytics and
# dashboard"). Named tools are far more persuasive, so rank those first.
_WEAK_DM_SKILLS = frozenset({
    "analytics", "dashboard", "reporting", "metrics", "kpi", "data quality",
    "data modeling", "stakeholder", "communication", "presentation", "etl",
    "elt", "data pipeline", "machine learning", "statistics", "excel",
    "data contract", "data contracts", "agile", "scrum", "data warehouse",
})


# Skills whose conventional casing isn't Title Case.
_SKILL_CASING = {
    "dbt": "dbt", "sql": "SQL", "aws": "AWS", "gcp": "GCP", "etl": "ETL",
    "elt": "ELT", "rag": "RAG", "llm": "LLM", "nlp": "NLP", "api": "API",
    "rest api": "REST API", "ci/cd": "CI/CD", "faiss": "FAISS", "kpi": "KPI",
    "bi": "BI", "ml": "ML", "ai": "AI", "s3": "S3", "duckdb": "DuckDB",
    "postgresql": "PostgreSQL", "mysql": "MySQL", "pyspark": "PySpark",
    "mlops": "MLOps", "mlflow": "MLflow", "fastapi": "FastAPI",
    "power bi": "Power BI", "text2sql": "Text2SQL", "openai": "OpenAI",
    "pytorch": "PyTorch", "tensorflow": "TensorFlow", "scikit-learn": "scikit-learn",
    "langchain": "LangChain", "github actions": "GitHub Actions",
}


def _display_skill(s: str) -> str:
    """Render a skill token with its conventional casing."""
    return _SKILL_CASING.get(s, s.title() if s.islower() else s)


def _rank_dm_skills(matched_skills: list[str]) -> list[str]:
    """Order matched skills so named technologies come before generic terms."""
    strong = [s for s in matched_skills if s not in _WEAK_DM_SKILLS]
    weak = [s for s in matched_skills if s in _WEAK_DM_SKILLS]
    return strong + weak


def generate_linkedin_dm(company: str, title: str, matched_skills: list[str]) -> str:
    """Build a personalized, ready-to-copy LinkedIn outreach DM."""
    ranked = _rank_dm_skills(matched_skills)
    if ranked:
        skill_str = " and ".join(_display_skill(s) for s in ranked[:2])
        skill_line = f"My hands-on work with {skill_str} lines up closely with what the role calls for."
    else:
        skill_line = "My data engineering background aligns well with this role."
    return (
        f"Hi [Name], I came across the {title} opening at {company} and I'm genuinely excited. "
        f"{skill_line} "
        f"Would you be open to a quick 15-min chat? "
        f"I'd love to learn more about the team and share how I can contribute."
    )


# ---------------------------------------------------------------------------
# TF-IDF helper
# ---------------------------------------------------------------------------

def _tfidf_similarity(text_a: str, text_b: str) -> float:
    """Return cosine similarity [0, 1] between two texts using TF-IDF."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        import numpy as np
    except ImportError:
        return 0.0

    if not text_a.strip() or not text_b.strip():
        return 0.0

    vec = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1, 2),
        max_features=5_000,
        sublinear_tf=True,
    )
    try:
        tfidf = vec.fit_transform([text_a, text_b])
        sim = cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0]
        return float(sim)
    except Exception as e:
        log.debug("TF-IDF error: %s", e)
        return 0.0


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------

def _use_hybrid() -> bool:
    """Hybrid matcher is on by default; set USE_HYBRID_MATCHER=0 to fall back."""
    return os.environ.get("USE_HYBRID_MATCHER", "1").strip().lower() not in ("0", "false", "no")


def score_resume_vs_jd(
    resume_text: str,
    jd_text: str,
    job_title: str = "",
) -> ResumeMatchResult:
    """Score a JD against the resume.  Returns ResumeMatchResult(0) if either is empty.

    Delegates to the hybrid matcher (src/matching/) unless USE_HYBRID_MATCHER=0,
    in which case the original TF-IDF + substring path runs unchanged. All
    legacy fields stay populated either way.
    """
    if not resume_text or not jd_text:
        return ResumeMatchResult(
            overall_score=0, skill_score=0, tfidf_score=0, has_jd=bool(jd_text)
        )

    if _use_hybrid():
        return _score_hybrid(resume_text, jd_text, job_title)

    # ── Experience filter ────────────────────────────────────────────────────
    exp_ok, req_years = experience_passes_filter(jd_text)

    # ── Resume bullet highlights ─────────────────────────────────────────────
    bullets = top_resume_bullets(resume_text, jd_text, n=3)

    # ── Skill match ─────────────────────────────────────────────────────────
    jd_skills   = _extract_skills_from_text(jd_text)
    jd_skill_set = set(jd_skills)

    matched: list[str] = []
    missing: list[str] = []
    weighted_total = 0.0
    weighted_matched = 0.0

    for skill in jd_skill_set:
        in_jd_required = _is_required_context(jd_text, skill)
        weight = 2.0 if in_jd_required else 1.0
        weighted_total += weight
        if skill in resume_text.lower():
            matched.append(skill)
            weighted_matched += weight
        else:
            missing.append(skill)

    skill_score = int((weighted_matched / weighted_total * 100) if weighted_total > 0 else 0)

    # ── TF-IDF similarity ────────────────────────────────────────────────────
    # Prepend job title to JD for stronger title-level signal
    jd_full = f"{job_title}\n{jd_text}"
    tfidf_raw = _tfidf_similarity(resume_text, jd_full)
    # Scale: raw cosine sim for resume vs JD typically tops out at 0.4-0.6
    # Normalise into 0-100 with a gentle ceiling at raw=0.5 → score=100
    tfidf_score = min(100, int(tfidf_raw * 200))

    # ── Combined score ───────────────────────────────────────────────────────
    overall = int(0.50 * skill_score + 0.50 * tfidf_score)
    overall = max(0, min(100, overall))

    return ResumeMatchResult(
        overall_score=overall,
        skill_score=skill_score,
        tfidf_score=tfidf_score,
        matched_skills=sorted(matched),
        missing_skills=sorted(missing),
        jd_skill_count=len(jd_skill_set),
        has_jd=True,
        required_experience=req_years,
        experience_ok=exp_ok,
        top_bullets=bullets,
    )


def _score_hybrid(resume_text: str, jd_text: str, job_title: str) -> ResumeMatchResult:
    """Hybrid path: deterministic + family veto + optional semantic layer.

    Maps HybridResult onto ResumeMatchResult so existing consumers
    (tailor.py, notifier, batch_score_jobs) keep working unchanged.
    """
    from .matching import match as hybrid_match

    exp_ok, req_years = experience_passes_filter(jd_text)
    bullets = top_resume_bullets(resume_text, jd_text, n=3)
    r = hybrid_match(resume_text, jd_text, job_title=job_title)

    return ResumeMatchResult(
        overall_score=r.overall_score,
        skill_score=r.skill_fit,
        tfidf_score=r.doc_similarity,   # doc-similarity component (semantic or TF-IDF)
        matched_skills=r.matched_skills,
        missing_skills=r.missing_skills,
        jd_skill_count=len(r.matches),
        has_jd=True,
        required_experience=req_years,
        experience_ok=exp_ok,
        top_bullets=bullets,
        exact_coverage=r.exact_coverage,
        equivalent_coverage=r.equivalent_coverage,
        semantic_coverage=r.semantic_coverage,
        related_only_skills=sorted(m.display for m in r.related_only),
        missing_required=sorted(m.display for m in r.missing_required),
        jd_terminology=r.jd_terminology,
        semantic_used=r.semantic_used,
    )


# ---------------------------------------------------------------------------
# JD fetching (for sources that don't embed description in the API)
# ---------------------------------------------------------------------------

def fetch_jd_text(url: str, timeout: int = 15) -> str:
    """HTTP-fetch a job posting page and return plain text of the JD section.

    Returns empty string on any error so the caller can gracefully skip.
    Tries a set of common ATS CSS selectors before falling back to full body.
    """
    if not url:
        return ""
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Try common ATS containers in order of specificity
        selectors = [
            "#content",                          # Greenhouse
            ".posting-description",              # Lever
            '[data-qa="job-description"]',       # Lever v2
            ".job-description",
            ".description",
            "#job-description",
            "article",
            "main",
        ]
        for sel in selectors:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) > 100:
                return el.get_text(separator=" ", strip=True)

        # Fallback: full body text (strip nav / header / footer)
        for tag in soup(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        return soup.get_text(separator=" ", strip=True)[:8_000]

    except Exception as e:
        log.debug("JD fetch failed for %s: %s", url, e)
        return ""


# ---------------------------------------------------------------------------
# Batch scorer — called from main.py
# ---------------------------------------------------------------------------

def _load_resume_file(path: str) -> str:
    """Load a resume file by path. Returns empty string if not found."""
    p = Path(path)
    if not p.exists():
        return ""
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def batch_score_jobs(jobs: list, resume_path: str = "") -> list:
    """Score jobs against both resumes (DE + AI), pick whichever matches better.

    Dual-resume logic:
      1. Loads config/resume_de.txt  (Data Engineering track)
      2. Loads config/resume_ai.txt  (AI Engineering track)
      3. For each job, scores against both; uses the higher score
      4. Sets job.resume_track = "de" | "ai" so email shows which resume to send
    Falls back to master_resume.txt if track-specific files don't exist.
    """
    if not resume_path:
        resume_path = _resume_path_from_env()

    # Load all available resumes
    resume_de  = _load_resume_file("config/resume_de.txt")
    resume_ai  = _load_resume_file("config/resume_ai.txt")
    resume_main = load_resume(resume_path)

    candidates: list[tuple[str, str]] = []  # (resume_text, track_label)
    if resume_de:
        candidates.append((resume_de, "de"))
    if resume_ai:
        candidates.append((resume_ai, "ai"))
    if resume_main and not candidates:
        candidates.append((resume_main, "main"))

    if not candidates:
        log.info("Resume matching skipped — no resume files found")
        return jobs

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _score_one(job) -> tuple:
        """Fetch JD once, then score against all resume candidates. Return best."""
        jd = job.description or ""
        if not jd:
            jd = fetch_jd_text(job.url)
            if jd:
                job.description = jd

        best_result = None
        best_track  = ""
        for resume_text, track in candidates:
            result = score_resume_vs_jd(resume_text, jd, job_title=job.title)
            if best_result is None or result.overall_score > best_result.overall_score:
                best_result = result
                best_track  = track

        return job, best_result, best_track

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_score_one, j): j for j in jobs}
        for fut in as_completed(futures):
            try:
                job, result, track = fut.result()
                job.resume_match  = result.overall_score
                job.experience_ok = result.experience_ok
                job.top_bullets   = result.top_bullets
                job.resume_track  = track
                if result.matched_skills:
                    job.linkedin_dm = generate_linkedin_dm(
                        job.company, job.title, result.matched_skills
                    )
                if result.has_jd:
                    if not result.experience_ok:
                        log.info(
                            "Experience filter: skipping %s — %s (requires %d yrs, max %d)",
                            job.company, job.title,
                            result.required_experience or 0, MAX_EXPERIENCE_YEARS,
                        )
                    else:
                        log.debug(
                            "Resume match [%s]: %s — %s → %d%% (skills %d%%, tfidf %d%%, "
                            "matched: %s)",
                            track, job.company, job.title,
                            result.overall_score, result.skill_score, result.tfidf_score,
                            ", ".join(result.matched_skills[:5]) or "none",
                        )
            except Exception as e:
                log.debug("Resume match error for job: %s", e)

    return jobs
