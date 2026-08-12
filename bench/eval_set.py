"""Hand-labelled evaluation set for resume ↔ JD skill matching.

Each case pairs a resume snippet with a JD snippet and labels, per skill,
whether the resume *genuinely satisfies* that requirement.

Labelling rule — deliberately strict, matching how a human screener reads:
  True  = the resume demonstrates this specific skill (literally, via an alias,
          or via unambiguous evidence of the same activity)
  False = it does not. A sibling technology does NOT count. Docker experience
          does not satisfy a Kubernetes requirement.

Categories covered: synonyms, acronyms, subsumption, title wording, exact
technology requirements, related-but-not-equivalent, and true absences.
"""
from __future__ import annotations

# (case_id, category, resume_text, jd_text, {skill_surface: satisfied})
CASES: list[tuple[str, str, str, str, dict[str, bool]]] = [
    (
        "syn_aws", "synonym",
        "Deployed pipelines on Amazon Web Services using S3 and Lambda.",
        "Requirements: experience with AWS.",
        {"aws": True},
    ),
    (
        "syn_aws_rev", "synonym",
        "Built data infrastructure on AWS with EC2 and S3 storage.",
        "Required: Amazon Web Services experience.",
        {"amazon web services": True},
    ),
    (
        "acr_ml", "acronym",
        "Applied machine learning models for churn prediction and segmentation.",
        "Required: ML experience in production.",
        {"ml": True},
    ),
    (
        "acr_ml_rev", "acronym",
        "Built ML pipelines for ranking and recommendation systems.",
        "Requirements: machine learning background.",
        {"machine learning": True},
    ),
    (
        "acr_nlp", "acronym",
        "Developed natural language processing pipelines for document search.",
        "Required: NLP experience.",
        {"nlp": True},
    ),
    (
        "sub_pyspark", "subsumption",
        "Wrote PySpark jobs to transform 10M+ records nightly.",
        "Required: Apache Spark experience.",
        {"apache spark": True},
    ),
    (
        "sem_pipelines", "semantic",
        "Developed scalable ETL workflows moving data from APIs into Snowflake.",
        "Required: experience building scalable data pipelines.",
        {"data pipelines": True},
    ),
    (
        "sem_etl_elt", "semantic",
        "Built extract, transform and load processes across three source systems.",
        "Required: strong ETL background.",
        {"etl": True},
    ),
    (
        "rel_docker_k8s", "related_not_equivalent",
        "Containerised services with Docker and shipped images to a registry.",
        "Required: Kubernetes for container orchestration.",
        {"kubernetes": False},
    ),
    (
        "rel_aws_azure", "related_not_equivalent",
        "Provisioned infrastructure on AWS using Terraform and EC2.",
        "Required: Microsoft Azure data platform experience.",
        {"azure": False},
    ),
    (
        "rel_tableau_pbi", "related_not_equivalent",
        "Built 15+ Power BI executive dashboards on weekly cadences.",
        "Required: Tableau dashboard development.",
        {"tableau": False},
    ),
    (
        "rel_torch_tf", "related_not_equivalent",
        "Trained models in PyTorch for text classification tasks.",
        "Required: TensorFlow model development.",
        {"tensorflow": False},
    ),
    (
        "rel_snow_bq", "related_not_equivalent",
        "Modelled data in Snowflake with dbt incremental models.",
        "Required: Google BigQuery experience.",
        {"bigquery": False},
    ),
    (
        "miss_kafka", "true_missing",
        "Built batch reporting pipelines in SQL and Python for finance teams.",
        "Required: Apache Kafka streaming experience.",
        {"apache kafka": False},
    ),
    (
        "miss_terraform", "true_missing",
        "Wrote SQL transformations and Power BI dashboards for stakeholders.",
        "Required: Terraform infrastructure as code.",
        {"terraform": False},
    ),
    (
        "miss_scala", "true_missing",
        "Primary languages are Python and SQL for data engineering work.",
        "Required: Scala development experience.",
        {"scala": False},
    ),
    (
        "exact_dbt", "exact",
        "Built dbt models with schema tests and snapshots enforcing contracts.",
        "Required: dbt for transformation and testing.",
        {"dbt": True},
    ),
    (
        "exact_airflow", "exact",
        "Orchestrated transformations via Airflow DAGs with task dependencies.",
        "Required: Apache Airflow orchestration.",
        {"apache airflow": True},
    ),
    (
        "exact_snowflake", "exact",
        "Tuned Snowflake queries with partitioning and clustering.",
        "Required: Snowflake warehouse experience.",
        {"snowflake": True},
    ),
    (
        "mixed_de", "mixed",
        "Built ETL workflows on Amazon Web Services using PySpark. Modelled data "
        "in Snowflake with dbt. Containerised the stack with Docker. Wrote "
        "advanced SQL with window functions and CTEs.",
        "Required: AWS, Apache Spark, Snowflake, dbt, SQL. Must have Kubernetes "
        "and Tableau experience. Preferred: data pipelines at scale.",
        {
            "aws": True, "apache spark": True, "snowflake": True, "dbt": True,
            "sql": True, "kubernetes": False, "tableau": False,
            "data pipelines": True,
        },
    ),
    (
        "mixed_ai", "mixed",
        "Built a RAG service with FAISS retrieval over MiniLM embeddings and a "
        "local LLM. Trained classifiers in PyTorch. Used LangChain for agent "
        "routing and Python throughout.",
        "Required: LLM systems, RAG, Python, PyTorch. Must have TensorFlow. "
        "Preferred: natural language processing.",
        {
            "llm": True, "rag": True, "python": True, "pytorch": True,
            "tensorflow": False, "natural language processing": True,
        },
    ),
]


def flatten() -> list[tuple[str, str, str, str, str, bool]]:
    """Yield (case_id, category, resume, jd, skill_surface, satisfied)."""
    out = []
    for cid, cat, resume, jd, labels in CASES:
        for skill, satisfied in labels.items():
            out.append((cid, cat, resume, jd, skill, satisfied))
    return out
