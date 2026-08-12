"""Tests for the hybrid resume ↔ JD matcher.

Run:  python3 -m pytest tests/ -q
      python3 tests/test_matching.py     (no pytest needed)

Tests that require the optional semantic backend skip cleanly when
sentence-transformers is not installed, so CI stays green without torch.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.matching import match, ontology, semantic          # noqa: E402
from src.matching.config import MatchConfig                 # noqa: E402
from src.matching.hybrid import is_required, split_sentences  # noqa: E402


# ── Ontology: canonicalisation ───────────────────────────────────────────────

def test_alias_resolution() -> None:
    assert ontology.canonical("Amazon Web Services") == "aws"
    assert ontology.canonical("AWS") == "aws"
    assert ontology.canonical("ML") == "machine_learning"
    assert ontology.canonical("machine learning") == "machine_learning"
    assert ontology.canonical("k8s") == "kubernetes"
    assert ontology.canonical("totally unknown thing") is None


def test_extract_prefers_longest_surface_form() -> None:
    got = ontology.extract_canonical_skills("We use Amazon Web Services daily")
    assert got.get("aws") == "amazon web services"


def test_short_tokens_need_word_boundaries() -> None:
    # "r" must not match inside "reporting"; "ml" not inside "html"
    assert "r" not in ontology.extract_canonical_skills("built reporting dashboards")
    assert "machine_learning" not in ontology.extract_canonical_skills("wrote html templates")
    assert "r" in ontology.extract_canonical_skills("used R and Python")


# ── Ontology: the family veto ────────────────────────────────────────────────

def test_same_family_pairs_are_related_not_equal() -> None:
    assert ontology.same_family("docker", "kubernetes")
    assert ontology.same_family("aws", "azure")
    assert ontology.same_family("tableau", "power_bi")
    assert ontology.same_family("pytorch", "tensorflow")
    # identical skills are not "related" — they are the same
    assert not ontology.same_family("aws", "aws")
    # unrelated skills share no family
    assert not ontology.same_family("python", "kubernetes")


def test_subsumption_is_directional() -> None:
    assert ontology.subsumes("pyspark", "spark")      # PySpark covers Spark
    assert not ontology.subsumes("spark", "pyspark")  # but not the reverse


# ── Classification ───────────────────────────────────────────────────────────

def test_equivalent_via_alias() -> None:
    r = match("Deployed services on Amazon Web Services.", "Required: AWS experience.")
    m = next(x for x in r.matches if x.canonical == "aws")
    assert m.kind == "EQUIVALENT"
    assert m.satisfied


def test_exact_match() -> None:
    r = match("Built dbt models with schema tests.", "Required: dbt.")
    m = next(x for x in r.matches if x.canonical == "dbt")
    assert m.kind == "EXACT"
    assert m.satisfied


def test_subsumption_counts_as_equivalent() -> None:
    r = match("Wrote PySpark transformation jobs.", "Required: Apache Spark.")
    m = next(x for x in r.matches if x.canonical == "spark")
    assert m.kind == "EQUIVALENT"
    assert m.satisfied


def test_docker_does_not_satisfy_kubernetes() -> None:
    """The central guarantee: a sibling technology never satisfies a requirement."""
    r = match("Containerised the app with Docker.", "Required: Kubernetes.")
    m = next(x for x in r.matches if x.canonical == "kubernetes")
    assert m.kind == "RELATED_ONLY"
    assert not m.satisfied
    assert m.credit == 0.0
    assert m.related_via == "docker"
    assert "Kubernetes" in r.missing_skills


def test_aws_does_not_satisfy_azure() -> None:
    r = match("Provisioned AWS infrastructure with Terraform.", "Required: Azure.")
    m = next(x for x in r.matches if x.canonical == "azure")
    assert m.kind == "RELATED_ONLY"
    assert not m.satisfied


def test_powerbi_does_not_satisfy_tableau() -> None:
    r = match("Built 15+ Power BI dashboards.", "Required: Tableau.")
    m = next(x for x in r.matches if x.canonical == "tableau")
    assert m.kind == "RELATED_ONLY"
    assert not m.satisfied


# ── Scoring behaviour ────────────────────────────────────────────────────────

def test_required_miss_caps_overall_score() -> None:
    """Document similarity must not paper over explicit required-skill misses."""
    resume = "Expert in Kubernetes, Docker, Terraform, AWS, Python, SQL, Spark."
    jd = ("Required: Tableau. Required: Azure. Required: BigQuery. "
          "Required: TensorFlow. Required: Kafka.")
    r = match(resume, jd)
    assert len(r.missing_required) >= 4
    # 5 missed requirements → ceiling of 100 - 12*5 = 40
    assert r.overall_score <= 40


def test_credit_weights_are_configurable() -> None:
    resume = "Built dbt models."
    jd = "Required: dbt."
    strict = MatchConfig(credit_exact=0.5)
    assert match(resume, jd, config=strict).skill_fit == 50
    assert match(resume, jd).skill_fit == 100


def test_empty_inputs_are_safe() -> None:
    assert match("", "Required: AWS").overall_score == 0
    assert match("Python developer", "").overall_score == 0
    assert not match("Python developer", "").has_jd


# ── Required vs preferred detection ──────────────────────────────────────────

def test_required_vs_preferred_context() -> None:
    jd = "Requirements: Python. Nice to have: Scala."
    assert is_required(jd, "python")
    assert not is_required(jd, "scala")


def test_evidence_points_back_to_resume() -> None:
    resume = "Built dbt models with schema tests and snapshots enforcing contracts."
    r = match(resume, "Required: dbt.")
    m = next(x for x in r.matches if x.canonical == "dbt")
    assert "dbt" in m.evidence.lower()


def test_split_sentences_strips_bullets() -> None:
    got = split_sentences("• Built pipelines that processed ten million records daily")
    assert got and not got[0].startswith("•")


# ── Semantic layer (skipped when unavailable) ────────────────────────────────

def test_semantic_layer_when_available() -> None:
    if not semantic.is_available():
        print("  (skipped: sentence-transformers not installed)")
        return
    resume = "Developed scalable ETL workflows moving data from APIs into Snowflake."
    r = match(resume, "Required: experience building scalable data pipelines.")
    m = next(x for x in r.matches if x.canonical == "data_pipeline")
    assert m.kind == "SEMANTIC"
    assert m.evidence


def test_semantic_never_overrides_family_veto() -> None:
    """Even with embeddings on, PyTorch must not satisfy TensorFlow."""
    if not semantic.is_available():
        print("  (skipped: sentence-transformers not installed)")
        return
    r = match("Trained models in PyTorch.", "Required: TensorFlow.")
    m = next(x for x in r.matches if x.canonical == "tensorflow")
    assert m.kind == "RELATED_ONLY"
    assert not m.satisfied


def test_degrades_without_semantic_backend() -> None:
    """Deterministic layers must work with the semantic layer disabled."""
    r = match("Deployed on Amazon Web Services.", "Required: AWS.", use_semantic=False)
    m = next(x for x in r.matches if x.canonical == "aws")
    assert m.satisfied
    assert not r.semantic_used


# ── Backward compatibility with the legacy interface ─────────────────────────

def test_score_resume_vs_jd_keeps_legacy_fields() -> None:
    from src.resume_matcher import score_resume_vs_jd
    r = score_resume_vs_jd("Built dbt models on Snowflake.", "Required: dbt and Snowflake.")
    for attr in ("overall_score", "skill_score", "tfidf_score",
                 "matched_skills", "missing_skills", "has_jd"):
        assert hasattr(r, attr), attr
    assert 0 <= r.overall_score <= 100
    assert r.has_jd


if __name__ == "__main__":
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in fns:
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {name}  {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {name}  {type(e).__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
