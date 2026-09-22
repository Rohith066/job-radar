"""Proof that the PRODUCTION path — `python -m src.main` — runs the relevance gate.

`tests/test_role_relevance.py` already tests `analyze_relevance` as a function
and the dashboard as a reader. Neither proves the orchestrator consults it, and
for six months it did not: `src/screening/relevance.py` existed while
`origin/main`'s `src/main.py` never imported it, so every CI run screened
without the gate. A unit test of the gate cannot catch that. These tests drive
the real `_dispatch_results` and the real `screen_job`.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import pytest

from src import main as main_mod
from src.classifier import classify
from src.config import Config
from src.database import Database
from src.main import _dispatch_results, screen_job
from src.screening import reasons as R
from src.screening.relevance import ADJACENT, AMBIGUOUS, OUT_OF_SCOPE, TARGET
from src.screening.scoring import APPLY_NOW, REJECT, STRONG
from src.sources.base import Job

NOW = datetime.now(timezone.utc)


class CapturingNotifier:
    def __init__(self):
        self.calls = []
        self._notifiers = []

    def notify(self, yes_jobs, maybe_jobs, *, subject_prefix="", mode="", source_errors=None):
        self.calls.append({"yes": list(yes_jobs), "maybe": list(maybe_jobs)})
        return []


def mk(key, title, *, location="Remote - US", description="", company="Acme",
       source="greenhouse", hours_old=2):
    return Job(
        key=key, source=source, company=company, title=title, location=location,
        url=f"http://example.test/{key}",
        posted=(NOW - timedelta(hours=hours_old)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
        description=description,
    )


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("src.main.batch_score_jobs", lambda jobs, resume_path="": jobs)
    monkeypatch.setattr("src.main.ml_rescore", lambda jobs, db=None: jobs)
    monkeypatch.setattr("src.main.ghost_check",
                        lambda j, db=None: type("G", (), {"level": "", "reasons": []})())
    db = Database(str(tmp_path / "t.db"))
    yield Config(), db, CapturingNotifier()
    db.close()


def run(cfg, db, notifier, jobs):
    for j in jobs:
        cr = classify(j.title)
        j.score, j.label = cr.score, cr.label
    _dispatch_results(all_jobs=jobs, errors=[], db=db, notifier=notifier, mode="boards",
                      dry_run=False, no_notify=False, test_notify=False, cfg=cfg)


def stored_keys(db):
    return {r[0] for r in db._conn.execute("SELECT key FROM jobs").fetchall()}


def alerted_keys(notifier):
    out = set()
    for c in notifier.calls:
        out |= {j.key for j in c["yes"]} | {j.key for j in c["maybe"]}
    return out


# ---------------------------------------------------------------------------
# 1. Production main imports and calls role relevance
# ---------------------------------------------------------------------------
def test_main_imports_analyze_relevance():
    assert hasattr(main_mod, "analyze_relevance"), (
        "src/main.py must import analyze_relevance; without it CI screens "
        "without the occupational gate, which is how it ran for six months."
    )


def test_screen_job_source_calls_relevance_and_passes_it_to_scoring():
    src = inspect.getsource(screen_job)
    assert "analyze_relevance(" in src
    assert "relevance=relevance" in src, (
        "score_job must receive the relevance verdict — computing it and "
        "dropping it on the floor is the failure mode this test exists for."
    )


def test_screen_job_records_the_relevance_verdict_on_the_job():
    j = mk("rel-1", "Data Engineer", description="We build data pipelines in Python and SQL.")
    opp = screen_job(j)
    assert j.role_relevance in (TARGET, ADJACENT, AMBIGUOUS, OUT_OF_SCOPE)
    assert j.relevance_explanation
    assert opp.priority != REJECT


def test_dispatch_actually_invokes_the_gate(env, monkeypatch):
    """Spy on the real call site rather than trusting the import."""
    cfg, db, notifier = env
    seen = []
    real = main_mod.analyze_relevance

    def spy(title, jd_text="", *, role_family=""):
        seen.append(title)
        return real(title, jd_text, role_family=role_family)

    monkeypatch.setattr(main_mod, "analyze_relevance", spy)
    run(cfg, db, notifier, [mk("spy-1", "Data Engineer", description="Python, SQL, Airflow.")])
    assert "Data Engineer" in seen


# ---------------------------------------------------------------------------
# 2. A clear OUT_OF_SCOPE job cannot reach an alertable band
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("title", [
    "Entry-Level Bridge Engineer",
    "Plumbing Engineer I",
    "Data Center Cabling Foreman",
    "Roadway Design Engineer I",
])
def test_out_of_scope_occupation_is_rejected_end_to_end(env, title):
    cfg, db, notifier = env
    run(cfg, db, notifier, [mk("oos", title, description="Entry level. Great benefits. US based.")])
    assert "oos" not in stored_keys(db), f"{title!r} was stored as a current opportunity"
    assert "oos" not in alerted_keys(notifier), f"{title!r} was alerted on"


def test_out_of_scope_rejection_is_attributed_to_relevance(env):
    j = mk("oos-why", "Entry-Level Bridge Engineer", description="Entry level civil role.")
    opp = screen_job(j)
    assert opp.priority == REJECT
    assert R.RELEVANCE_OUT_OF_SCOPE in opp.reason_codes


def test_freshness_and_us_location_cannot_rescue_a_different_occupation(env):
    """The gate is a hard exclusion, not a score term."""
    cfg, db, notifier = env
    run(cfg, db, notifier, [mk("oos-fresh", "Entry-Level Bridge Engineer",
                               location="Austin, TX", hours_old=0,
                               description="Brand new posting. Entry level. Full benefits.")])
    assert "oos-fresh" not in stored_keys(db)


# ---------------------------------------------------------------------------
# 3 / 4 / 5. TARGET stays eligible, ADJACENT stays eligible, AMBIGUOUS reviews
# ---------------------------------------------------------------------------
def test_target_job_remains_eligible(env):
    cfg, db, notifier = env
    run(cfg, db, notifier, [mk("tgt", "Data Engineer",
                               description="Build data pipelines with Python, SQL, Airflow and dbt.")])
    assert "tgt" in stored_keys(db)
    assert "tgt" in alerted_keys(notifier)


def test_adjacent_job_remains_eligible(env):
    cfg, db, notifier = env
    j = mk("adj", "Business Intelligence Engineer",
           description="Build dashboards in Tableau over a SQL warehouse.")
    run(cfg, db, notifier, [j])
    assert "adj" in stored_keys(db)


def test_specialisation_is_visible_but_review_only(env):
    """ADJACENT-by-specialisation must stay on the board, capped below STRONG."""
    j = mk("spec", "Machine Learning Compiler Engineer",
           description="Work on kernel scheduling and compiler passes for ML accelerators.")
    opp = screen_job(j)
    assert j.role_relevance == ADJACENT
    assert opp.priority != REJECT
    assert opp.priority not in (APPLY_NOW,), "a low-level specialisation must not top the board"


def test_ambiguous_job_is_retained_but_capped_below_strong(env):
    cfg, db, notifier = env
    j = mk("amb", "Systems Engineer", description="")
    opp = screen_job(j)
    assert j.role_relevance == AMBIGUOUS
    assert opp.priority != REJECT, "missing evidence is not negative evidence"
    assert opp.priority not in (APPLY_NOW, STRONG)
    run(cfg, db, notifier, [mk("amb", "Systems Engineer")])
    assert "amb" in stored_keys(db), "ambiguous jobs stay reviewable"


# ---------------------------------------------------------------------------
# 14. Boundary positive controls — the titles that broke naive token rules
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("title", [
    "Data Engineer II, Data Center Capacity Delivery",
    "Site Industrial Data Engineer",
    "Data Scientist II, Amazon Fulfillment Technology",
    "Jr. AI Engineer - Data Annotation",
    "Data Scientist - AV Metrics & Evaluation Analytics",
    "Software Engineer, Data Center",
])
def test_boundary_controls_are_not_blocked(title):
    j = mk("ctl", title, description="SQL, Python and data pipeline work.")
    opp = screen_job(j)
    assert j.role_relevance != OUT_OF_SCOPE, f"{title!r} was wrongly blocked"
    assert opp.priority != REJECT or "SENIOR" in " ".join(opp.reason_codes), (
        f"{title!r} was rejected for a reason other than seniority"
    )


def test_site_reliability_engineer_is_not_blocked_by_the_hardware_anchors():
    """`reliability engineer` was measured and deliberately NOT adopted as an
    anchor precisely because it would catch SRE."""
    j = mk("sre", "Site Reliability Engineer", description="Run distributed services on Kubernetes.")
    screen_job(j)
    assert j.role_relevance != OUT_OF_SCOPE


# ---------------------------------------------------------------------------
# 15. The greenhouse:agency noise family
# ---------------------------------------------------------------------------
AGENCY_TITLES = [
    "Croatian Language Specialist - Freelance AI Trainer Project",
    "Vietnamese Voice Actor - Freelance AI Trainer Project",
    "Web Development Specialist - Freelance AI Trainer Project",
    "Database & Network Specialist - AI Trainer Project",
    "STEM Specialist (Fluent in German) - Freelance AI Trainer Project",
]


@pytest.mark.parametrize("title", AGENCY_TITLES)
def test_annotation_gigwork_is_out_of_scope(title):
    j = mk("ag", title, company="agency",
           description="Help train AI models. Flexible hours. Work from home.")
    opp = screen_job(j)
    assert j.role_relevance == OUT_OF_SCOPE, f"{title!r} reached the board"
    assert opp.priority == REJECT


def test_annotation_gigwork_does_not_block_real_annotation_engineering():
    """The guard that keeps the gig-work family from over-reaching."""
    j = mk("ann", "Jr. AI Engineer - Data Annotation",
           description="Build annotation tooling and evaluation pipelines in Python.")
    screen_job(j)
    assert j.role_relevance == TARGET


def test_agency_board_titles_never_reach_storage(env):
    cfg, db, notifier = env
    jobs = [mk(f"ag{i}", t, company="agency",
               description="Help train AI models. Flexible hours.")
            for i, t in enumerate(AGENCY_TITLES)]
    run(cfg, db, notifier, jobs)
    assert stored_keys(db) == set()
    assert alerted_keys(notifier) == set()
