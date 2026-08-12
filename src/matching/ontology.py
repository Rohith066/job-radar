"""Canonical skill ontology: aliases, acronyms, and skill families.

Three jobs:

1. **Canonicalisation** — map surface forms to one canonical id, so
   "Amazon Web Services", "AWS" and "aws" all resolve to ``aws``.

2. **Equivalence** — two surface forms sharing a canonical id are the *same*
   skill (EQUIVALENT), even with zero lexical overlap.

3. **Family veto** — two canonical skills can be closely related without being
   interchangeable. ``docker`` and ``kubernetes`` are both containerisation;
   an embedding model rates them highly similar. But a JD requiring Kubernetes
   is *not* satisfied by Docker experience. Sharing a family with a *different*
   canonical id therefore classifies as RELATED_ONLY and earns zero credit.

The family relation is what keeps semantic matching honest.
"""
from __future__ import annotations

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Aliases: surface form -> canonical id
# Only genuine equivalences belong here. Anything that is merely *similar*
# belongs in FAMILIES instead.
# ---------------------------------------------------------------------------
ALIASES: dict[str, str] = {
    # Cloud
    "amazon web services": "aws",
    "aws": "aws",
    "microsoft azure": "azure",
    "azure": "azure",
    "google cloud platform": "gcp",
    "google cloud": "gcp",
    "gcp": "gcp",
    # Data processing
    "apache spark": "spark",
    "spark": "spark",
    "pyspark": "pyspark",
    "spark sql": "spark",
    "apache kafka": "kafka",
    "kafka": "kafka",
    "apache airflow": "airflow",
    "airflow": "airflow",
    "apache flink": "flink",
    "flink": "flink",
    "databricks": "databricks",
    "delta lake": "delta_lake",
    "deltalake": "delta_lake",
    # Warehouses
    "snowflake": "snowflake",
    "amazon redshift": "redshift",
    "redshift": "redshift",
    "google bigquery": "bigquery",
    "bigquery": "bigquery",
    "big query": "bigquery",
    "postgresql": "postgres",
    "postgres": "postgres",
    "postgre sql": "postgres",
    # Transformation
    "dbt": "dbt",
    "data build tool": "dbt",
    "etl": "etl",
    "extract transform load": "etl",
    "extract, transform and load": "etl",
    "extract, transform, load": "etl",
    "extract transform and load": "etl",
    "elt": "elt",
    "extract load transform": "elt",
    "data pipeline": "data_pipeline",
    "data pipelines": "data_pipeline",
    "etl pipeline": "etl",
    "etl pipelines": "etl",
    # ML / AI
    "machine learning": "machine_learning",
    "ml": "machine_learning",
    "deep learning": "deep_learning",
    "dl": "deep_learning",
    "natural language processing": "nlp",
    "nlp": "nlp",
    "large language model": "llm",
    "large language models": "llm",
    "llm": "llm",
    "llms": "llm",
    "retrieval augmented generation": "rag",
    "retrieval-augmented generation": "rag",
    "rag": "rag",
    "scikit-learn": "sklearn",
    "scikit learn": "sklearn",
    "sklearn": "sklearn",
    "pytorch": "pytorch",
    "torch": "pytorch",
    "tensorflow": "tensorflow",
    "tf": "tensorflow",
    "hugging face": "huggingface",
    "huggingface": "huggingface",
    "langchain": "langchain",
    "faiss": "faiss",
    "mlflow": "mlflow",
    "mlops": "mlops",
    # Infra
    "docker": "docker",
    "kubernetes": "kubernetes",
    "k8s": "kubernetes",
    "terraform": "terraform",
    "infrastructure as code": "iac",
    "iac": "iac",
    "ci/cd": "cicd",
    "cicd": "cicd",
    "continuous integration": "cicd",
    "github actions": "github_actions",
    "jenkins": "jenkins",
    # BI
    "tableau": "tableau",
    "power bi": "power_bi",
    "powerbi": "power_bi",
    "looker": "looker",
    "sigma": "sigma",
    "qlik": "qlik",
    # Languages
    "python": "python",
    "sql": "sql",
    "structured query language": "sql",
    "scala": "scala",
    "java": "java",
    "bash": "bash",
    "shell scripting": "bash",
    "r": "r",
    # Practices
    "data modeling": "data_modeling",
    "data modelling": "data_modeling",
    "dimensional modeling": "dimensional_modeling",
    "star schema": "dimensional_modeling",
    "data quality": "data_quality",
    "data governance": "data_governance",
    "data lineage": "data_lineage",
    "data contracts": "data_contracts",
    "data contract": "data_contracts",
    "a/b testing": "ab_testing",
    "a/b test": "ab_testing",
    "ab testing": "ab_testing",
    "experimentation": "experimentation",
    "fastapi": "fastapi",
    "rest api": "rest_api",
    "rest apis": "rest_api",
    "restful api": "rest_api",
}

# ---------------------------------------------------------------------------
# Families: canonical id -> family name.
#
# Members of a family are substitutable *categories*, NOT substitutable skills.
# Two different canonical ids in the same family => RELATED_ONLY.
# ---------------------------------------------------------------------------
FAMILIES: dict[str, str] = {
    # Cloud providers — knowing AWS does not mean knowing Azure
    "aws": "cloud_platform",
    "azure": "cloud_platform",
    "gcp": "cloud_platform",
    # Container / orchestration — Docker != Kubernetes
    "docker": "containerization",
    "kubernetes": "containerization",
    # BI tools — Tableau != Power BI
    "tableau": "bi_tool",
    "power_bi": "bi_tool",
    "looker": "bi_tool",
    "sigma": "bi_tool",
    "qlik": "bi_tool",
    # Warehouses
    "snowflake": "warehouse",
    "redshift": "warehouse",
    "bigquery": "warehouse",
    "databricks": "warehouse",
    # Deep-learning frameworks — PyTorch != TensorFlow
    "pytorch": "dl_framework",
    "tensorflow": "dl_framework",
    # Orchestrators
    "airflow": "orchestrator",
    "dagster": "orchestrator",
    "prefect": "orchestrator",
    # Stream processors
    "kafka": "streaming",
    "flink": "streaming",
    # CI systems
    "github_actions": "ci_system",
    "jenkins": "ci_system",
    # Relational stores
    "postgres": "rdbms",
    "mysql": "rdbms",
}

# ---------------------------------------------------------------------------
# Hierarchical relations: child -> parent.
#
# Unlike families (siblings, not substitutable), these are genuine
# specialisations. PySpark IS Spark usage, so PySpark on a resume satisfies a
# Spark requirement. The reverse does not hold and is intentionally not encoded.
# ---------------------------------------------------------------------------
SUBSUMES: dict[str, str] = {
    "pyspark": "spark",
    "spark": "spark",
}

# Longest-first so "amazon web services" wins over "aws" inside a longer phrase
_SORTED_ALIASES = sorted(ALIASES.keys(), key=len, reverse=True)

# Short/ambiguous surface forms that must match on word boundaries
_BOUNDARY_REQUIRED = frozenset({
    "r", "ml", "dl", "tf", "aws", "gcp", "sql", "etl", "elt", "llm", "rag",
    "nlp", "k8s", "iac", "java", "scala", "dbt", "spark", "kafka", "sigma",
})


def canonical(surface: str) -> Optional[str]:
    """Map a surface form to its canonical skill id, or None if unknown."""
    s = re.sub(r"\s+", " ", (surface or "").strip().lower())
    return ALIASES.get(s)


def extract_canonical_skills(text: str) -> dict[str, str]:
    """Find every known skill in ``text``.

    Returns ``{canonical_id: first_surface_form_seen}``. The surface form is
    retained so downstream output can echo the JD's own vocabulary.
    """
    if not text:
        return {}
    low = re.sub(r"\s+", " ", text.lower())
    found: dict[str, str] = {}
    for alias in _SORTED_ALIASES:
        if alias in _BOUNDARY_REQUIRED:
            hit = re.search(rf"(?<![a-z0-9+#]){re.escape(alias)}(?![a-z0-9+#])", low)
        else:
            hit = re.search(re.escape(alias), low)
        if hit:
            cid = ALIASES[alias]
            found.setdefault(cid, alias)
    return found


def same_family(a: str, b: str) -> bool:
    """True when two *different* canonical skills belong to one family.

    This is the veto that prevents a semantically similar sibling technology
    from satisfying an explicit requirement.
    """
    if a == b:
        return False
    fa, fb = FAMILIES.get(a), FAMILIES.get(b)
    return fa is not None and fa == fb


def family_of(cid: str) -> Optional[str]:
    return FAMILIES.get(cid)


def subsumes(resume_skill: str, jd_skill: str) -> bool:
    """True when holding ``resume_skill`` genuinely covers ``jd_skill``.

    Directional: PySpark covers Spark, Spark does not cover PySpark.
    """
    if resume_skill == jd_skill:
        return True
    return SUBSUMES.get(resume_skill) == jd_skill


def display_name(cid: str) -> str:
    """Human-readable label for a canonical id."""
    special = {
        "aws": "AWS", "gcp": "GCP", "sql": "SQL", "etl": "ETL", "elt": "ELT",
        "nlp": "NLP", "llm": "LLM", "rag": "RAG", "dbt": "dbt", "cicd": "CI/CD",
        "iac": "Infrastructure as Code", "power_bi": "Power BI", "gcp_": "GCP",
        "machine_learning": "Machine Learning", "deep_learning": "Deep Learning",
        "data_pipeline": "data pipelines", "ab_testing": "A/B testing",
        "rest_api": "REST APIs", "github_actions": "GitHub Actions",
        "delta_lake": "Delta Lake", "data_modeling": "data modeling",
        "dimensional_modeling": "dimensional modeling",
        "data_quality": "data quality", "data_governance": "data governance",
        "data_lineage": "data lineage", "data_contracts": "data contracts",
        "sklearn": "scikit-learn", "huggingface": "Hugging Face",
        "mlops": "MLOps", "fastapi": "FastAPI", "pytorch": "PyTorch",
        "tensorflow": "TensorFlow", "bigquery": "BigQuery",
    }
    if cid in special:
        return special[cid]
    return cid.replace("_", " ").title()
