"""Candidate profile — Rohith Bayya.

Used to:
  1. Compute a skill-match bonus on top of the title score
  2. Inject personalised context into email / Slack notifications
  3. Serve as a single source of truth for target role configuration

No external dependencies — pure Python dicts & sets.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Core profile
# ---------------------------------------------------------------------------
PROFILE = {
    "name": "Rohith Bayya",
    "email": "rohithbr6@gmail.com",
    "location": "Fairfax, VA",
    "experience_years": 3,
    "education": "M.S. Data Analytics Engineering — George Mason University (2025)",
    "target_roles": [
        "Data Analyst",
        "Analytics Engineer",
        "Business Intelligence Analyst",
        "Business Intelligence Engineer",
        "Commercial Analyst",
        "Revenue Analyst",
        "Financial Analyst",
        "Reporting Analyst",
        "Product Analyst",
        "Data Engineer",
        "Data Warehouse Engineer",
    ],
}

# ---------------------------------------------------------------------------
# Skill sets — used for keyword-based bonus scoring
# ---------------------------------------------------------------------------

# Primary tools: direct experience
SKILLS_STRONG: set[str] = {
    "sql", "python", "r",
    "power bi", "dax",
    "pandas", "numpy", "scikit-learn", "sklearn",
    "azure", "azure sql", "azure data lake",
    "aws", "s3", "redshift",
    "snowflake",
    "pyspark", "spark", "databricks",
    "plotly", "streamlit",
    "arima", "forecasting",
    "etl", "elt", "pipeline",
    "docker", "git",
    "duckdb",
    "tableau",
    "sql server", "t-sql",
    "logistic regression", "regression",
    "k-means", "clustering",
    "classification",
    "data warehouse", "dwh",
    "data modeling",
    # Moved from moderate — have project evidence for both
    "dbt",
    "airflow",
    # Commercial analytics signals — core to target roles
    "data lineage",
    "data governance",
    "kpi",
    "a/b test",
    "experimentation",
    "financial analytics",
}

# Familiar / secondary exposure
SKILLS_MODERATE: set[str] = {
    "kafka",
    "hadoop",
    "flink",
    "bigquery",
    "looker",
    "qlik",
    "excel",
    "power query",
    "gcp",
    "google cloud",
    "mysql",
    "postgresql",
    "postgres",
    "metabase",
    "mixpanel",
    "segment",
    "amplitude",
}

# ---------------------------------------------------------------------------
# Skill-match bonus scorer
# ---------------------------------------------------------------------------

_STRONG_BONUS = 8    # points per strong skill found in job text
_MODERATE_BONUS = 3  # points per moderate skill found


def skill_bonus(text: str, cap: int = 20) -> int:
    """Return a skill-match bonus (0–cap) based on skills found in `text`.

    Intended for job titles or any available job text. In practice, most
    job boards only expose titles/locations, so the bonus is mainly useful
    when description text is available.
    """
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
    return (
        f"<p style='font-size:12px;color:#666;margin:0 0 8px'>"
        f"Matched for <strong>{PROFILE['name']}</strong> — "
        f"targeting <em>{', '.join(PROFILE['target_roles'][:4])}…</em></p>"
    )


def profile_summary_text() -> str:
    return (
        f"Profile: {PROFILE['name']} | "
        f"Target: {', '.join(PROFILE['target_roles'][:3])}…"
    )
