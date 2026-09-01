"""Phase 2 correctness-audit regressions.

Pins the defects found in the final audit: duplicate ML training events,
missing score provenance, and the feedback-semantics mapping.
"""
from __future__ import annotations

import sqlite3
import tempfile, os

import pytest

from src.database import Database
from src.apply.provenance import profile_version
from src.apply.queue import (
    ApplicationQueue, IllegalTransition,
    NEW, SHORTLISTED, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED,
)
from src.apply.fit import analyze_fit
from src.apply.priority import (
    application_priority, ROLE_FAMILY_WEIGHTS, W_SCREENING, W_FIT, FIT_SWING,
    W_US_CONFIRMED, W_AMBIGUOUS_LOC, W_WORK_AUTH_RISK, W_PHD_REQUIRED,
    APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW, REJECT,
)

# The only feedback actions the pre-existing ML re-scorer recognises.
ML_POSITIVE = ("applied", "interested")
ML_NEGATIVE = ("dismissed",)
ML_IGNORED = ("responded", "rejected", "followed_up", "offer")


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "t.db"))
    for k in ("k1", "k2", "k3", "k4", "k5"):
        d.mark_job_seen(key=k, source="gh", company="Acme", title="Data Engineer I",
                        location="Remote - US", url=f"u/{k}", posted="", score=90, label="yes")
    yield d
    d.close()


def _actions(db, key):
    """Actions visible to the preference model — legacy + user state, deduped."""
    return [r[0] for r in db._conn.execute(
        f"SELECT action FROM ({db._feedback_union_sql()}) WHERE job_key=? ORDER BY created_at",
        (key,))]


# ── Feedback mapping semantics ────────────────────────────────────────────
@pytest.mark.parametrize("status,expected", [
    (SHORTLISTED, ["interested"]),
    (APPLIED,     ["applied"]),
    (INTERVIEW,   ["responded"]),
    (REJECTED,    ["rejected"]),
    (SKIPPED,     ["dismissed"]),
    (NO_RESPONSE, []),
    (NEW,         []),
])
def test_status_to_feedback_mapping(db, status, expected):
    ApplicationQueue(db).set_status("k1", status, force=True)
    assert _actions(db, "k1") == expected


def test_employer_rejection_is_not_a_user_negative_label(db):
    """'rejected' must stay outside the ML label set: the employer said no, the
    user did not dislike the job."""
    q = ApplicationQueue(db)
    q.set_status("k2", APPLIED); q.set_status("k2", REJECTED)
    acts = _actions(db, "k2")
    assert "rejected" in acts
    assert "dismissed" not in acts
    assert "rejected" in ML_IGNORED


def test_skipped_is_distinct_from_employer_rejection(db):
    q = ApplicationQueue(db)
    q.set_status("k3", SKIPPED)
    assert _actions(db, "k3") == ["dismissed"]
    assert "dismissed" in ML_NEGATIVE


def test_shortlisted_is_a_user_preference_not_a_success_signal(db):
    q = ApplicationQueue(db)
    q.set_status("k4", SHORTLISTED)
    assert _actions(db, "k4") == ["interested"]
    assert "interested" in ML_POSITIVE
    assert "responded" not in _actions(db, "k4")


def test_no_response_writes_no_feedback_event(db):
    """The existing action vocabulary has no no-response concept; inventing one
    would teach the ML model new semantics silently."""
    q = ApplicationQueue(db)
    q.set_status("k5", APPLIED)
    q.set_status("k5", NO_RESPONSE)
    assert _actions(db, "k5") == ["applied"]
    assert q.get("k5").status == NO_RESPONSE      # still distinctly represented
    assert q.get("k5").outcome_at


# ── Duplicate training events ─────────────────────────────────────────────
def test_repeated_status_does_not_duplicate_training_events(db):
    q = ApplicationQueue(db)
    for _ in range(4):
        q.set_status("k1", APPLIED, force=True)
    assert _actions(db, "k1") == ["applied"], "one job must not become several ML samples"


def test_each_action_is_written_at_most_once_per_job(db):
    q = ApplicationQueue(db)
    for st in (SHORTLISTED, APPLIED, INTERVIEW):
        q.set_status("k2", st)
    q.set_status("k2", NEW, force=True)
    q.set_status("k2", SHORTLISTED)
    acts = _actions(db, "k2")
    assert acts.count("interested") == 1
    assert acts.count("applied") == 1


# ── Provenance ────────────────────────────────────────────────────────────
def test_profile_version_is_deterministic_and_resume_sensitive():
    a = profile_version("resume text")
    assert a == profile_version("resume text")
    assert a != profile_version("resume text plus kubernetes")


def test_profile_version_stores_no_resume_content():
    v = profile_version("SECRETPHRASE in the resume")
    assert "SECRET" not in v and len(v) <= 16


def test_snapshot_records_provenance(db):
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED, priority=91, fit=84, screening=95, profile_version="p1-abc123")
    assert q.get("k1").profile_version == "p1-abc123"


def test_provenance_column_added_to_a_pre_provenance_db(tmp_path):
    path = tmp_path / "old.db"
    d = Database(str(path)); d.close()
    conn = sqlite3.connect(path)
    conn.execute("ALTER TABLE applications RENAME TO applications_old")
    conn.execute("""CREATE TABLE applications (job_key TEXT PRIMARY KEY, status TEXT NOT NULL,
        priority_at_decision INTEGER, fit_at_decision INTEGER, screening_at_decision INTEGER,
        note TEXT NOT NULL DEFAULT '', shortlisted_at TEXT, applied_at TEXT, outcome_at TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    conn.commit(); conn.close()
    d2 = Database(str(path))
    cols = {r[1] for r in d2._conn.execute("PRAGMA table_info(applications)")}
    assert "profile_version" in cols
    d2.close()


# ── Transition scenarios ──────────────────────────────────────────────────
def test_scenario_new_shortlisted_applied_interview(db):
    q = ApplicationQueue(db)
    assert q.status_of("k1") == NEW
    q.set_status("k1", SHORTLISTED)
    assert "k1" not in q.excluded_keys()
    q.set_status("k1", APPLIED)
    assert "k1" in q.excluded_keys()
    q.set_status("k1", INTERVIEW)
    assert q.metrics()["interviews"] == 1


def test_scenario_new_shortlisted_skipped(db):
    q = ApplicationQueue(db)
    q.set_status("k2", SHORTLISTED); q.set_status("k2", SKIPPED)
    assert "k2" in q.excluded_keys()
    assert q.metrics()["counts"][SKIPPED] == 1


def test_scenario_new_applied_rejected(db):
    q = ApplicationQueue(db)
    q.set_status("k3", APPLIED); q.set_status("k3", REJECTED)
    assert "k3" in q.excluded_keys()
    m = q.metrics()
    assert m["applications_total"] == 1 and m["interviews"] == 0


def test_scenario_applied_no_response(db):
    q = ApplicationQueue(db)
    q.set_status("k4", APPLIED); q.set_status("k4", NO_RESPONSE)
    assert q.get("k4").status == NO_RESPONSE
    assert q.metrics()["applications_total"] == 1


def test_illegal_backwards_transitions_fail(db):
    q = ApplicationQueue(db)
    q.set_status("k5", APPLIED)
    for bad in (NEW, SHORTLISTED):
        with pytest.raises(IllegalTransition):
            q.set_status("k5", bad)


def test_user_action_does_not_mutate_the_job_assessment(db):
    before = dict(db._conn.execute("SELECT * FROM jobs WHERE key='k1'").fetchone())
    ApplicationQueue(db).set_status("k1", SKIPPED)
    after = dict(db._conn.execute("SELECT * FROM jobs WHERE key='k1'").fetchone())
    for f in ("score", "label", "priority", "opportunity_score", "resume_match"):
        assert before[f] == after[f], f"user action changed the system assessment: {f}"


# ── Scoring arithmetic and invariants ─────────────────────────────────────
def _p(screening, sp, matched, *, family="software_engineering", loc="US_REMOTE", jd=None):
    jd = jd if jd is not None else (
        "Requirements:\n- 0-2 years of professional experience\n- Python\n- SQL\n"
        "Preferred qualifications:\n- Kubernetes\n")
    fit = analyze_fit(jd_text=jd, matched_canonicals=set(matched), role_family=family)
    return fit, application_priority(screening_score=screening, screening_priority=sp, fit=fit,
                                     role_family=family, location_class=loc)


def test_documented_formula_reproduces_the_implementation():
    """Hand-compute the full pipeline and compare with production output."""
    for screening, matched, loc in [(95, {"python", "sql"}, "US_REMOTE"),
                                    (100, {"python"}, "US"),
                                    (60, set(), "AMBIGUOUS"),
                                    (40, {"python", "sql", "kubernetes"}, "US")]:
        fit, ap = _p(screening, "APPLY_NOW", matched, loc=loc)
        blended = W_SCREENING * screening + W_FIT * fit.resume_fit_score
        blended = max(screening - FIT_SWING, min(screening + FIT_SWING, blended))
        expected = int(round(blended))
        expected += ROLE_FAMILY_WEIGHTS.get("software_engineering", 0)
        expected += (W_US_CONFIRMED if loc in ("US", "US_REMOTE")
                     else W_AMBIGUOUS_LOC if loc == "AMBIGUOUS" else 0)
        expected += W_WORK_AUTH_RISK if fit.work_auth_fit == "risk" else 0
        expected += W_PHD_REQUIRED if fit.education_fit == "phd_required" else 0
        expected = max(0, min(100, expected))
        assert ap.application_priority_score == expected, (screening, matched, loc)


def test_phase1_reject_is_never_promoted():
    _, ap = _p(0, "REJECT", {"python", "sql", "kubernetes"})
    assert ap.priority == REJECT and ap.application_priority_score == 0


def test_apply_now_never_falls_below_medium():
    for matched in (set(), {"python"}, {"python", "sql"}):
        _, ap = _p(95, "APPLY_NOW", matched)
        assert ap.priority in (APPLY_FIRST, HIGH, MEDIUM)


def test_one_missing_preferred_skill_has_small_effect():
    _, full = _p(90, "APPLY_NOW", {"python", "sql", "kubernetes"})
    _, gap = _p(90, "APPLY_NOW", {"python", "sql"})
    assert full.application_priority_score - gap.application_priority_score <= 4


def test_one_missing_required_skill_does_not_destroy_a_strong_match():
    jd = ("Requirements:\n- 0-2 years of professional experience\n- Python\n- SQL\n- Java\n")
    _, ap = _p(95, "APPLY_NOW", {"python", "sql"}, jd=jd)
    assert ap.priority in (APPLY_FIRST, HIGH, MEDIUM)


def test_missing_jd_is_neutral_not_negative():
    _, with_jd = _p(90, "APPLY_NOW", {"python", "sql", "kubernetes"})
    _, no_jd = _p(90, "APPLY_NOW", set(), jd="")
    assert no_jd.fit_adjustment == 0
    assert no_jd.application_priority_score == 90 + ROLE_FAMILY_WEIGHTS["software_engineering"]


def test_ms_or_phd_does_not_trigger_the_phd_penalty():
    fit, ap = _p(80, "APPLY_NOW", {"python"},
                 jd="Requirements:\n- MS or PhD in a quantitative field\n- Python\n")
    assert fit.education_fit == "met"
    assert not any("PhD" in w for w in ap.warnings)


def test_phd_only_requirement_is_still_recognised():
    fit, ap = _p(80, "APPLY_NOW", {"python"},
                 jd="Requirements:\n- PhD in Machine Learning required\n- Python\n")
    assert fit.education_fit == "phd_required"
    assert any("PhD" in w for w in ap.warnings)


def test_no_first_class_family_has_an_implicit_penalty():
    for fam in ("data_engineering", "data_science", "ml_ai",
                "software_engineering", "backend", "fullstack"):
        assert ROLE_FAMILY_WEIGHTS[fam] == 0, f"{fam} carries a family prior"


def test_role_family_weights_remain_configurable():
    assert isinstance(ROLE_FAMILY_WEIGHTS, dict)
    assert "adjacent_analysis" in ROLE_FAMILY_WEIGHTS
