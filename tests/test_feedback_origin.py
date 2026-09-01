"""System predictions must never become user preference labels.

`_dispatch_results` auto-writes an `interested` event whenever a resume match
clears AUTO_INTERESTED_THRESHOLD. The preference model treats `interested` as a
positive label, so before this correction the model could train on its own
output. These tests make the producer/consumer boundary permanent.
"""
from __future__ import annotations

import shutil
import sqlite3

import pytest

from src.database import Database
from src.apply import user_state as us
from src.apply.queue import (
    ApplicationQueue, SHORTLISTED, APPLIED, SKIPPED, INTERVIEW, REJECTED,
)


def _discovery(path, jobs=(("k1", "Acme", "Data Engineer I"),)):
    d = Database(str(path), attach_user_state=False)
    for key, company, title in jobs:
        d.mark_job_seen(key=key, source="gh", company=company, title=title,
                        location="Remote - US", url=f"http://x/{key}", posted="",
                        score=90, label="yes")
    d.close()
    return path


def _preference_samples(db):
    """Exactly what the preference model trains on."""
    return [r for r in db.get_feedback_jobs()
            if r["action"] in ("applied", "interested", "dismissed")]


@pytest.fixture
def paths(tmp_path):
    return tmp_path / "disc.db", tmp_path / "user_state.db"


# ── The core anti-self-training property ──────────────────────────────────
def test_system_interested_does_not_become_a_preference_sample(paths):
    disc, ustate = paths
    _discovery(disc)
    db = Database(str(disc), user_state=str(ustate))
    before = len(_preference_samples(db))

    for _ in range(5):
        db.record_feedback("k1", "interested", origin="system")

    assert len(_preference_samples(db)) == before, \
        "a system prediction entered the user preference dataset"
    # It is preserved, just not as preference data.
    assert db.system_signal_stats().get("interested") == 5
    db.close()


def test_explicit_shortlist_does_become_a_preference_sample(paths):
    disc, ustate = paths
    _discovery(disc)
    db = Database(str(disc), user_state=str(ustate))
    before = len(_preference_samples(db))
    ApplicationQueue(db).set_status("k1", SHORTLISTED)
    after = _preference_samples(db)
    assert len(after) == before + 1
    assert any(r["action"] == "interested" for r in after)
    db.close()


def test_system_and_user_interested_are_stored_separately(paths):
    disc, ustate = paths
    _discovery(disc)
    db = Database(str(disc), user_state=str(ustate))
    db.record_feedback("k1", "interested", origin="system")
    ApplicationQueue(db).set_status("k1", SHORTLISTED)
    assert db._conn.execute(
        "SELECT COUNT(*) FROM main.feedback WHERE action='interested'").fetchone()[0] == 1
    assert db._conn.execute(
        "SELECT COUNT(*) FROM userstate.user_feedback WHERE action='interested'").fetchone()[0] == 1
    assert len(_preference_samples(db)) == 1, "only the user action may train the model"
    db.close()


@pytest.mark.parametrize("status,action", [
    (SHORTLISTED, "interested"), (APPLIED, "applied"), (SKIPPED, "dismissed"),
])
def test_user_actions_write_to_user_state_not_discovery(paths, status, action):
    disc, ustate = paths
    _discovery(disc)
    db = Database(str(disc), user_state=str(ustate))
    ApplicationQueue(db).set_status("k1", status, force=True)
    assert db._conn.execute(
        f"SELECT COUNT(*) FROM userstate.user_feedback WHERE action='{action}'"
    ).fetchone()[0] == 1
    assert db._conn.execute("SELECT COUNT(*) FROM main.feedback").fetchone()[0] == 0
    db.close()


def test_cli_feedback_flags_are_user_generated(paths):
    """`python -m src.main --applied` is a human decision."""
    disc, ustate = paths
    _discovery(disc)
    db = Database(str(disc), user_state=str(ustate))
    db.record_feedback("k1", "applied")            # default origin='user'
    assert db._conn.execute("SELECT COUNT(*) FROM main.feedback").fetchone()[0] == 0
    assert len(_preference_samples(db)) == 1
    db.close()


# ── Bounded legacy import ─────────────────────────────────────────────────
def test_legacy_import_runs_once_and_is_marked(paths):
    disc, ustate = paths
    _discovery(disc)
    d = Database(str(disc), attach_user_state=False)
    for i in range(10):
        d._conn.execute("INSERT INTO feedback(job_key,action,created_at,notes) "
                        "VALUES('k1','applied',?,'')", (f"2026-08-{i+1:02d}",))
    d._conn.commit(); d.close()

    for _ in range(5):                       # reopen repeatedly
        db = Database(str(disc), user_state=str(ustate))
        db.close()

    db = Database(str(disc), user_state=str(ustate))
    assert us.import_complete(db._conn)
    assert db._conn.execute(
        "SELECT COUNT(*) FROM userstate.user_feedback").fetchone()[0] == 1  # deduped (k1, applied)
    assert len(_preference_samples(db)) == 1
    db.close()


def test_legacy_import_never_pulls_ambiguous_interested(paths):
    disc, ustate = paths
    _discovery(disc)
    d = Database(str(disc), attach_user_state=False)
    d._conn.execute("INSERT INTO feedback(job_key,action,created_at,notes) "
                    "VALUES('k1','interested','2026-08-01','')")
    d._conn.execute("INSERT INTO feedback(job_key,action,created_at,notes) "
                    "VALUES('k1','applied','2026-08-02','')")
    d._conn.commit(); d.close()

    db = Database(str(disc), user_state=str(ustate))
    imported = [r[0] for r in db._conn.execute(
        "SELECT action FROM userstate.user_feedback")]
    assert imported == ["applied"], "ambiguous legacy 'interested' must not be imported"
    db.close()


def test_system_feedback_added_after_migration_is_never_imported(paths):
    disc, ustate = paths
    _discovery(disc)
    db = Database(str(disc), user_state=str(ustate))       # marks import complete
    baseline = len(_preference_samples(db))
    db.close()

    d = Database(str(disc), attach_user_state=False)
    for i in range(20):
        d._conn.execute("INSERT INTO feedback(job_key,action,created_at,notes) "
                        "VALUES('k1','interested',?,'')", (f"2026-09-{i+1:02d}",))
    d._conn.commit(); d.close()

    db = Database(str(disc), user_state=str(ustate))
    assert len(_preference_samples(db)) == baseline, \
        "post-migration system feedback leaked into the preference dataset"
    db.close()


# ── A -> B -> C -> D with system feedback injected ────────────────────────
def test_durability_with_system_feedback_injected(tmp_path):
    ustate = tmp_path / "user_state.db"
    live = tmp_path / "state.db"

    def build(tag, auto_interested):
        p = tmp_path / f"disc_{tag}.db"
        _discovery(p, [("X", "Acme", "Data Engineer I"), (f"o{tag}", "Beta", "Software Engineer I")])
        d = Database(str(p), attach_user_state=False)
        for i in range(auto_interested):
            d._conn.execute("INSERT INTO feedback(job_key,action,created_at,notes) "
                            f"VALUES('o{tag}','interested','2026-09-{i%28+1:02d}','')")
        d._conn.commit(); d.close()
        return p

    versions = {t: build(t, n) for t, n in (("A", 0), ("B", 5), ("C", 20), ("D", 3))}

    shutil.copy(versions["A"], live)
    db = Database(str(live), user_state=str(ustate))
    q = ApplicationQueue(db)
    q.set_status("X", APPLIED, priority=91, fit=84, screening=95, profile_version="p1-x")
    baseline_pref = len(_preference_samples(db))
    assert baseline_pref == 1
    db.close()

    for tag in ("B", "C", "D"):
        shutil.copy(versions[tag], live)          # CI replaces discovery state
        db = Database(str(live), user_state=str(ustate))
        a = ApplicationQueue(db).get("X")
        assert a is not None and a.status == APPLIED, f"user history lost at {tag}"
        assert a.priority_at_decision == 91 and a.profile_version == "p1-x"
        assert ApplicationQueue(db).metrics()["applications_total"] == 1
        assert len(_preference_samples(db)) == baseline_pref, \
            f"automatic interested events became preference samples at {tag}"
        db.close()

    # Explicit user actions still register normally after all that churn.
    db = Database(str(live), user_state=str(ustate))
    ApplicationQueue(db).set_status("oD", SHORTLISTED)
    assert len(_preference_samples(db)) == baseline_pref + 1
    db.close()


# ── Observation reports user behaviour only ───────────────────────────────
def test_observe_excludes_system_signals_from_user_funnel(paths):
    from src.apply.observe import snapshot
    disc, ustate = paths
    _discovery(disc, [("k1", "Acme", "Data Engineer I"), ("k2", "Beta", "Software Engineer I")])
    db = Database(str(disc), user_state=str(ustate))
    for _ in range(30):
        db.record_feedback("k2", "interested", origin="system")
    ApplicationQueue(db).set_status("k1", APPLIED, priority=91, fit=84, screening=95)
    snap = snapshot(db)
    assert snap["funnel"]["applied"] == 1
    assert snap["funnel"]["shortlisted"] == 0, "system signals leaked into the user funnel"
    assert snap["user_actions"] == 1
    db.close()
