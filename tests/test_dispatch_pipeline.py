"""End-to-end test of the result dispatcher.

Exercises the real `_dispatch_results` with synthetic jobs, a temporary
database and a capturing notifier — no network, no production state, no
credentials. This is what proves the routing wiring works, not just the pure
scoring functions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config import Config
from src.database import Database
from src.main import _dispatch_results
from src.sources.base import Job

NOW = datetime.now(timezone.utc)


class CapturingNotifier:
    def __init__(self):
        self.calls = []
        self._notifiers = []

    def notify(self, yes_jobs, maybe_jobs, *, subject_prefix="", mode="", source_errors=None):
        self.calls.append({"yes": list(yes_jobs), "maybe": list(maybe_jobs)})
        return []


def mk(key, title, location, description="", hours_old=2):
    return Job(
        key=key, source="greenhouse", company="Acme", title=title,
        location=location, url=f"http://example.test/{key}",
        posted=(NOW - timedelta(hours=hours_old)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
        description=description,
    )


@pytest.fixture
def env(tmp_path, monkeypatch):
    # Neutralise the network-dependent and model-dependent stages so the test
    # measures routing, not the matcher.
    monkeypatch.setattr("src.main.batch_score_jobs", lambda jobs, resume_path="": jobs)
    monkeypatch.setattr("src.main.ml_rescore", lambda jobs, db=None: jobs)
    monkeypatch.setattr("src.main.ghost_check",
                        lambda j, db=None: type("G", (), {"level": "", "reasons": []})())
    db = Database(str(tmp_path / "t.db"))
    yield Config(), db, CapturingNotifier()
    db.close()


def _run(cfg, db, notifier, jobs):
    from src.classifier import classify
    for j in jobs:
        cr = classify(j.title)
        j.score, j.label = cr.score, cr.label
    _dispatch_results(
        all_jobs=jobs, errors=[], db=db, notifier=notifier, mode="boards",
        dry_run=False, no_notify=False, test_notify=False, cfg=cfg,
    )


def test_priority_routing_end_to_end(env):
    cfg, db, notifier = env
    jobs = [
        mk("a", "New Grad Software Engineer", "Remote - US", "0-2 years of experience", 2),
        mk("b", "Senior Software Engineer", "Austin, TX", "5+ years of experience", 1),
        mk("c", "Software Engineering Manager", "US Remote", "", 1),
        mk("d", "Data Engineer", "Remote", "", 2),
        mk("e", "Software Engineer IV", "Seattle, WA", "", 1),
    ]
    _run(cfg, db, notifier, jobs)

    assert notifier.calls, "an alert should have been sent"
    alerted = notifier.calls[0]
    titles = {j.title for j in alerted["yes"] + alerted["maybe"]}

    assert "New Grad Software Engineer" in titles
    for suppressed in ("Senior Software Engineer", "Software Engineering Manager",
                       "Software Engineer IV"):
        assert suppressed not in titles, f"{suppressed} reached the user"


def test_rejected_jobs_are_not_persisted(env):
    cfg, db, notifier = env
    _run(cfg, db, notifier, [mk("m", "Software Engineering Manager", "US Remote")])
    assert db._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_screening_fields_are_persisted(env):
    cfg, db, notifier = env
    _run(cfg, db, notifier,
         [mk("a", "New Grad Software Engineer", "Remote - US", "0-2 years of experience")])
    row = db._conn.execute("SELECT * FROM jobs WHERE key='a'").fetchone()
    assert row["priority"] == "APPLY_NOW"
    assert row["opportunity_score"] >= 85
    assert row["location_class"] == "US_REMOTE"
    assert row["seniority"] == "entry"
    assert row["role_family"] == "software_engineering"
    assert row["experience_min"] == 0
    assert "NEW_GRAD_EXPLICIT" in row["classification_reasons"]


def test_confirmed_non_us_is_dropped(env):
    cfg, db, notifier = env
    _run(cfg, db, notifier, [
        mk("us", "New Grad Software Engineer", "Austin, TX", "0-2 years of experience"),
        mk("ca", "New Grad Software Engineer", "Toronto, Canada", "0-2 years of experience"),
    ])
    keys = {r[0] for r in db._conn.execute("SELECT key FROM jobs")}
    assert "us" in keys
    assert "ca" not in keys


def test_ambiguous_location_survives(env):
    """A bare 'Remote' must reach the user rather than being discarded."""
    cfg, db, notifier = env
    _run(cfg, db, notifier,
         [mk("r", "Junior Software Engineer", "Remote", "1-2 years of experience")])
    row = db._conn.execute("SELECT * FROM jobs WHERE key='r'").fetchone()
    assert row is not None
    assert row["location_class"] == "AMBIGUOUS"


def test_low_priority_is_stored_but_not_alerted(env):
    cfg, db, notifier = env
    jobs = [
        mk("hot", "New Grad Software Engineer", "Remote - US", "0-2 years of experience", 1),
        mk("cold", "Data Analyst", "Remote", "", 2),
    ]
    _run(cfg, db, notifier, jobs)
    stored = {r[0] for r in db._conn.execute("SELECT key FROM jobs")}
    assert "hot" in stored
    if notifier.calls:
        alerted = {j.key for j in notifier.calls[0]["yes"] + notifier.calls[0]["maybe"]}
        low = db._conn.execute("SELECT key FROM jobs WHERE priority='LOW'").fetchall()
        for (k,) in low:
            assert k not in alerted


def test_every_alerted_job_carries_reasons(env):
    cfg, db, notifier = env
    _run(cfg, db, notifier, [
        mk("a", "New Grad Software Engineer", "Remote - US", "0-2 years of experience"),
        mk("b", "Software Engineer I", "Austin, Texas", "1-2 years of experience"),
        mk("c", "Data Engineer", "Chicago, IL", "2-3 years of relevant experience"),
    ])
    assert notifier.calls
    for j in notifier.calls[0]["yes"] + notifier.calls[0]["maybe"]:
        assert j.classification_reasons, f"{j.title} alerted with no reasons"
        assert j.opportunity_score > 0
        assert j.priority
