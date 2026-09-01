"""Phase 2.1 — durable user state survives CI replacing the discovery DB.

The failure this prevents: `state/jobs.db` is a tracked binary that CI rewrites
and pushes 25-30x/day. Application decisions used to live in that file, so a
routine pull could destroy them.
"""
from __future__ import annotations

import os
import shutil
import sqlite3

import pytest

from src.database import Database
from src.apply import user_state as us
from src.apply.queue import (
    ApplicationQueue, IllegalTransition,
    NEW, SHORTLISTED, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED,
)
from src.apply.observe import snapshot


def _make_discovery(path, jobs, *, with_phase1_cols=True):
    """Build a discovery DB the way CI would ship one."""
    d = Database(str(path), attach_user_state=False)
    for key, company, title in jobs:
        d.mark_job_seen(key=key, source="gh", company=company, title=title,
                        location="Remote - US", url=f"http://x/{key}", posted="",
                        score=90, label="yes",
                        **({"role_family": "data_engineering", "priority": "APPLY_NOW",
                            "opportunity_score": 95} if with_phase1_cols else {}))
    d.close()
    return path


@pytest.fixture
def paths(tmp_path):
    return tmp_path / "disc.db", tmp_path / "user_state.db"


# ── Path resolution ───────────────────────────────────────────────────────
def test_default_path_is_outside_the_repository(monkeypatch):
    monkeypatch.delenv(us.ENV_VAR, raising=False)
    p = us.user_state_path()
    assert p == (us.DEFAULT_DIR / us.DEFAULT_NAME).resolve()
    assert "job-radar" not in str(p), "user state must not live inside the repo"


def test_environment_override(monkeypatch, tmp_path):
    monkeypatch.setenv(us.ENV_VAR, str(tmp_path / "custom.db"))
    assert us.user_state_path() == (tmp_path / "custom.db").resolve()


def test_explicit_argument_wins(monkeypatch, tmp_path):
    monkeypatch.setenv(us.ENV_VAR, str(tmp_path / "env.db"))
    assert us.user_state_path(str(tmp_path / "arg.db")) == (tmp_path / "arg.db").resolve()


# ── Ownership: user actions never touch the discovery DB ──────────────────
def test_user_action_does_not_modify_the_discovery_file(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    before = disc.read_bytes()
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    ApplicationQueue(db).set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    db.close()
    assert disc.read_bytes() == before, "a user action modified the discovery database"
    assert ustate.exists()


def test_application_rows_land_in_user_state_only(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    ApplicationQueue(db).set_status("k1", APPLIED)
    db.close()
    c = sqlite3.connect(ustate)
    assert c.execute("SELECT COUNT(*) FROM applications").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM user_feedback").fetchone()[0] == 1
    c.close()
    c = sqlite3.connect(f"file:{disc}?mode=ro&immutable=1", uri=True)
    assert c.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == 0
    c.close()


# ── THE CENTRAL ACCEPTANCE TEST ───────────────────────────────────────────
def test_user_history_survives_repeated_discovery_replacement(tmp_path):
    """A -> B -> C -> D: CI replaces the discovery DB four times over."""
    ustate = tmp_path / "user_state.db"
    versions = {}
    for tag in ("A", "B", "C", "D"):
        p = tmp_path / f"disc_{tag}.db"
        _make_discovery(p, [("k1", "Acme", "Data Engineer I"),
                            (f"only_{tag}", "Beta", "Software Engineer I")])
        versions[tag] = p

    live = tmp_path / "state.db"
    shutil.copy(versions["A"], live)

    db = Database(str(live), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    q.set_status("k1", SHORTLISTED, priority=91, fit=84, screening=95,
                 profile_version="p1-abc", note="referred by a friend")
    q.set_status("k1", APPLIED)
    app_before = q.get("k1")
    db.close()

    for tag in ("B", "C", "D"):
        shutil.copy(versions[tag], live)          # CI commits a new state file
        db = Database(str(live), user_state=str(ustate), readonly=True)
        q = ApplicationQueue(db)
        a = q.get("k1")
        assert a is not None, f"application lost after replacement {tag}"
        assert a.status == APPLIED
        assert a.note == "referred by a friend"
        assert (a.priority_at_decision, a.fit_at_decision, a.screening_at_decision) == (91, 84, 95)
        assert a.profile_version == "p1-abc"
        assert a.applied_at == app_before.applied_at
        assert a.shortlisted_at == app_before.shortlisted_at
        assert "k1" in q.excluded_keys(), "applied job must stay suppressed"
        assert q.metrics()["applications_total"] == 1
        db.close()

    db = Database(str(live), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    q.set_status("k1", INTERVIEW)
    assert q.metrics()["interviews"] == 1
    db.close()


def test_history_survives_a_job_disappearing_from_discovery(tmp_path):
    ustate = tmp_path / "user_state.db"
    live = tmp_path / "state.db"
    _make_discovery(live, [("gone", "Acme", "Data Engineer I")])

    db = Database(str(live), user_state=str(ustate), readonly=True)
    ApplicationQueue(db).set_status("gone", APPLIED, priority=91, fit=84, screening=95)
    db.close()

    live.unlink()
    _make_discovery(live, [("other", "Beta", "Software Engineer I")])   # 'gone' is gone

    db = Database(str(live), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    a = q.get("gone")
    assert a.status == APPLIED
    assert a.company == "Acme" and a.title == "Data Engineer I"     # snapshot kept it readable
    assert q.metrics()["applications_total"] == 1
    q.set_status("gone", INTERVIEW)                                  # still trackable
    assert q.get("gone").status == INTERVIEW
    db.close()


# ── Legacy compatibility ──────────────────────────────────────────────────
def test_legacy_feedback_is_copied_not_moved(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    d = Database(str(disc), attach_user_state=False)
    d.record_feedback("k1", "applied")
    d.close()

    db = Database(str(disc), user_state=str(ustate))
    rows = db._conn.execute("SELECT action, origin FROM userstate.user_feedback").fetchall()
    assert [tuple(r) for r in rows] == [("applied", "legacy")]
    assert db._conn.execute("SELECT COUNT(*) FROM main.feedback").fetchone()[0] == 1, \
        "legacy history must be copied, never moved"
    db.close()


def test_legacy_import_is_idempotent_and_not_double_counted(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    d = Database(str(disc), attach_user_state=False)
    d.record_feedback("k1", "applied")
    d.close()
    for _ in range(3):
        db = Database(str(disc), user_state=str(ustate))
        db.close()
    db = Database(str(disc), user_state=str(ustate))
    assert db._conn.execute("SELECT COUNT(*) FROM userstate.user_feedback").fetchone()[0] == 1
    # One action contributes exactly one preference sample across both sources.
    assert len([r for r in db.get_feedback_jobs() if r["action"] == "applied"]) == 1
    db.close()


def test_preference_model_sees_one_sample_per_action(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    for _ in range(4):
        q.set_status("k1", APPLIED, force=True)
    assert len([r for r in db.get_feedback_jobs() if r["action"] == "applied"]) == 1
    db.close()


def test_employer_outcomes_keep_non_preference_semantics(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED); q.set_status("k1", REJECTED)
    actions = {r["action"] for r in db.get_feedback_jobs()}
    assert "rejected" in actions
    assert "dismissed" not in actions, "employer rejection must not become a user-negative label"
    db.close()


# ── Queue overlay and follow-up across the two databases ──────────────────
def test_queue_suppression_uses_user_state(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I"), ("k2", "Beta", "Software Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED); q.set_status("k2", SKIPPED)
    assert q.excluded_keys() == {"k1", "k2"}
    db.close()


def test_followup_uses_user_state_status(paths, monkeypatch):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED)
    db._conn.execute("UPDATE userstate.user_feedback SET created_at='2020-01-01' "
                     "WHERE job_key='k1' AND action='applied'")
    db._conn.commit()
    assert "k1" in {r["job_key"] for r in db.get_followup_due()}
    q.set_status("k1", NO_RESPONSE)
    assert "k1" not in {r["job_key"] for r in db.get_followup_due()}
    db.close()


# ── Migration scenarios ───────────────────────────────────────────────────
def test_opens_against_a_discovery_db_without_phase1_columns(paths):
    """A stale checkout predating Phase 1 must not break user actions."""
    disc, ustate = paths
    conn = sqlite3.connect(disc)
    conn.executescript("""CREATE TABLE jobs (key TEXT PRIMARY KEY, source TEXT, company TEXT,
        title TEXT, location TEXT, url TEXT, posted TEXT, score INTEGER, label TEXT,
        first_seen TEXT, last_seen TEXT);
        CREATE TABLE boards (board_id TEXT PRIMARY KEY, platform TEXT, first_seen TEXT, last_seen TEXT);
        CREATE TABLE cursors (name TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE feedback (job_key TEXT, action TEXT, created_at TEXT, notes TEXT);""")
    conn.execute("INSERT INTO jobs(key,company,title) VALUES('k1','Acme','Data Engineer I')")
    conn.commit(); conn.close()

    db = Database(str(disc), user_state=str(ustate), readonly=True)
    a = ApplicationQueue(db).set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    assert a.status == APPLIED
    assert a.company == "Acme"          # snapshot still captured what exists
    assert a.role_family == ""          # column absent, tolerated
    db.close()


def test_reopening_an_existing_user_state_db(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    ApplicationQueue(db).set_status("k1", APPLIED); db.close()
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    assert ApplicationQueue(db).get("k1").status == APPLIED
    db.close()


def test_zero_applications_is_fine(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    assert ApplicationQueue(db).metrics()["applications_total"] == 0
    db.close()


# ── Backup ────────────────────────────────────────────────────────────────
def test_backup_creates_a_readable_copy_outside_the_repo(paths, tmp_path):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    ApplicationQueue(db).set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    db.close()

    out = us.backup(ustate, dest_dir=tmp_path / "backups")
    assert out.exists() and out.stat().st_size > 0
    assert "job-radar" not in str(out)
    c = sqlite3.connect(f"file:{out}?mode=ro&immutable=1", uri=True)
    assert c.execute("SELECT status FROM applications WHERE job_key='k1'").fetchone()[0] == APPLIED
    c.close()


def test_backup_without_a_database_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        us.backup(tmp_path / "missing.db")


# ── Observation collector ─────────────────────────────────────────────────
def test_observe_is_read_only_and_complete(paths):
    disc, ustate = paths
    _make_discovery(disc, [("k1", "Acme", "Data Engineer I"), ("k2", "Beta", "Software Engineer I")])
    before = disc.read_bytes()
    db = Database(str(disc), user_state=str(ustate), readonly=True)
    q = ApplicationQueue(db)
    q.set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    q.set_status("k2", SKIPPED, priority=60, fit=50, screening=62)
    snap = snapshot(db, days=7)
    db.close()

    assert disc.read_bytes() == before, "observation must not modify the discovery DB"
    for k in ("timestamp", "jobs", "applications", "funnel",
              "conversion_by_band", "conversion_by_role_family", "integrity"):
        assert k in snap
    assert snap["funnel"]["applied"] == 1
    assert snap["funnel"]["skipped"] == 1
    assert snap["integrity"]["duplicate_applications"] == 0
    assert "workflow" not in snap, "collector must not fabricate CI data it cannot see"


def test_observe_survives_discovery_replacement(tmp_path):
    ustate = tmp_path / "user_state.db"
    live = tmp_path / "state.db"
    _make_discovery(live, [("k1", "Acme", "Data Engineer I")])
    db = Database(str(live), user_state=str(ustate), readonly=True)
    ApplicationQueue(db).set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    db.close()

    live.unlink(); _make_discovery(live, [("z", "Zeta", "Backend Engineer I")])
    db = Database(str(live), user_state=str(ustate), readonly=True)
    snap = snapshot(db)
    assert snap["funnel"]["applied"] == 1, "metrics must survive CI state churn"
    db.close()
