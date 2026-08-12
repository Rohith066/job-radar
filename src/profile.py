"""Candidate profile — Rohith Bayya.

Dual-track job search:
  Track A — Analytics & Platform Data Engineering (dbt, Airflow, Snowflake, PySpark)
  Track B — LLM / Retrieval / Applied AI Engineering (RAG, FAISS, LangChain, Text2SQL)
"""
from __future__ import annotations

PROFILE = {
    "name": "Rohith Bayya",
    "email": "rohithbr6@gmail.com",
    "location": "New York, NY",
    "experience_years": 3,
    "education": "M.S. Data Analytics Engineering — George Mason University (2025)",
    "target_roles": [
        # Track A — Data Engineering
        "Analytics Engineer",
        "Data Engineer",
        "Platform Data Engineer",
        "dbt Engineer",
        # Track B — AI Engineering
        "LLM Engineer",
        "AI Engineer",
        "Retrieval Engineer",
        "Applied AI Engineer",
        # Secondary
        "Data Analyst",
        "ML Engineer",
    ],
    "tracks": {
        "de": "Analytics & Platform Data Engineering",
        "ai": "LLM / Retrieval / Applied AI Engineering",
    },
}

# ---------------------------------------------------------------------------
# Skill sets — used for keyword-based bonus scoring
# ---------------------------------------------------------------------------

# Track A — Data Engineering skills (direct project + work experience)
SKILLS_STRONG: set[str] = {
    # Core languages
    "python", "sql", "pyspark", "bash",
    # Orchestration & transformation
    "airflow", "dbt", "dbt cloud",
    # Warehousing & storage
    "snowflake", "aws s3", "delta lake", "duckdb", "medallion",
    "data lakehouse", "data lake", "data warehouse",
    # SQL techniques
    "window functions", "stored procedures", "materialized views", "cte", "query optimization",
    # Data quality
    "data quality", "schema validation", "data lineage", "kpi governance",
    "anomaly detection", "data contracts",
    # Analytics / ML
    "mlflow", "arima", "k-means", "feature pipeline",
    "pandas", "numpy", "scikit-learn", "sklearn",
    # Tooling
    "docker", "git", "streamlit", "jupyter",
    "etl", "elt", "pipeline",
    # Track B — AI / LLM Engineering (hands-on project experience)
    "langchain", "faiss", "rag", "llm",
    "openai", "anthropic", "hugging face", "transformers",
    "text2sql", "embedding", "semantic search",
    "multi-agent", "function calling", "prompt engineering",
    "retrieval augmented generation", "vector search",
    "ollama", "mistral", "llama",
    "pymupdf", "chunking", "metadata filtering",
    "pytorch", "torch",
}

# Secondary / familiar tools
SKILLS_MODERATE: set[str] = {
    "kafka", "hadoop", "flink", "spark",
    "bigquery", "redshift", "azure",
    "tableau", "power bi", "looker",
    "databricks",
    "mysql", "postgresql", "postgres",
    "docker", "kubernetes",
    "excel",
    "a/b test", "experimentation",
    "classification", "regression", "clustering",
    "time series",
    "aws", "gcp", "google cloud",
    "pinecone", "weaviate", "chroma",   # vector DBs
    "langsmith", "weights & biases", "mlops",
}

# ---------------------------------------------------------------------------
# Skill-match bonus scorer
# ---------------------------------------------------------------------------

_STRONG_BONUS = 8
_MODERATE_BONUS = 3


def skill_bonus(text: str, cap: int = 20) -> int:
    """Return a skill-match bonus (0–cap) based on skills found in `text`."""
    t = (text or "").lower()
    bonus = 0
    for skill in SKILLS_STRONG:
        if skill in t:
            bonus += _STRONG_BONUS
    for skill in SKILLS_MODERATE:
        if skill in t:
            bonus += _MODERATE_BONUS
    return min(bonus, cap)


# ---------------------------------------------------------------------------
# Notification header helpers
# ---------------------------------------------------------------------------

def profile_summary_html() -> str:
    tracks = " &bull; ".join(PROFILE["tracks"].values())
    return (
        f"<p style='font-size:12px;color:#666;margin:0 0 8px'>"
        f"Matched for <strong>{PROFILE['name']}</strong> — "
        f"<em>{tracks}</em></p>"
    )


def profile_summary_text() -> str:
    return (
        f"Profile: {PROFILE['name']} | "
        f"Tracks: {' | '.join(PROFILE['tracks'].values())}"
    )
