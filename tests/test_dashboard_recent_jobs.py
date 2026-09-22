"""The dashboard recomputes the title verdict instead of trusting stored labels.

Rows first seen before Phase 1 screening carry a legacy `label` that predates
the seniority veto (their `priority` is blank), so "Senior Manager, Data
Platform Engineering" reached the dashboard's Strong Matches on a stored 'yes'.
"""
from __future__ import annotations

import pytest

from src import dashboard
from src.database import Database
from src.screening import OUT_OF_SCOPE, analyze_relevance, analyze_title

DATA_JD = ("Build and operate batch and streaming data pipelines in Python and SQL "
           "on Spark and Airflow; model data in the warehouse with dbt.")

VETOED = ["Senior Manager, Data Platform Engineering",
          "Lead Analytics Architect and Engineer"]


@pytest.fixture
def db(tmp_path, monkeypatch):
    d = Database(str(tmp_path / "disc.db"), user_state=str(tmp_path / "us.db"))
    monkeypatch.setattr(dashboard, "_db", d)
    yield d
    d.close()


def _seen(db, key, title, label="yes"):
    # priority left blank, as on every row stored before Phase 1 screening
    db.mark_job_seen(key=key, source="test", company="Acme", title=title,
                     location="Remote", url=f"https://example.com/{key}",
                     posted="", score=90, label=label, description=DATA_JD)


def test_legacy_yes_label_cannot_bypass_the_title_veto(db):
    _seen(db, "ok", "Data Engineer")
    for i, t in enumerate(VETOED):
        # Only the title veto can withhold these; relevance alone keeps them.
        ta = analyze_title(t)
        assert ta.classification == "NO"
        assert analyze_relevance(t, DATA_JD, role_family=ta.role_family).state != OUT_OF_SCOPE
        _seen(db, f"veto{i}", t)

    assert [j["title"] for j in dashboard._get_recent_jobs()] == ["Data Engineer"]


def test_vetoed_job_with_feedback_stays_visible(db):
    # An applied job must keep its outcome buttons whatever its title verdict.
    _seen(db, "applied", VETOED[0])
    _seen(db, "untouched", VETOED[1])
    db.record_feedback("applied", "applied")

    jobs = dashboard._get_recent_jobs()
    assert [(j["key"], j["feedback"]) for j in jobs] == [("applied", "applied")]
