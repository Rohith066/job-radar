"""State backward-compatibility tests.

A database written by the previous version must open, migrate, and keep every
existing row readable. These run against a temporary DB — never production
state.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.database import Database

# The exact `jobs` schema that shipped before Phase 1.
LEGACY_SCHEMA = """
CREATE TABLE jobs (
    key          TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    company      TEXT NOT NULL,
    title        TEXT NOT NULL,
    location     TEXT NOT NULL DEFAULT '',
    url          TEXT NOT NULL DEFAULT '',
    posted       TEXT NOT NULL DEFAULT '',
    score        INTEGER NOT NULL DEFAULT 0,
    label        TEXT NOT NULL DEFAULT 'no',
    work_type    TEXT NOT NULL DEFAULT '',
    salary       TEXT NOT NULL DEFAULT '',
    resume_match INTEGER NOT NULL DEFAULT 0,
    description  TEXT NOT NULL DEFAULT '',
    first_seen   TEXT NOT NULL,
    last_seen    TEXT NOT NULL
);
CREATE TABLE boards (
    board_id     TEXT PRIMARY KEY,
    platform     TEXT NOT NULL,
    company      TEXT NOT NULL DEFAULT '',
    url          TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'active',
    last_checked TEXT,
    job_count    INTEGER NOT NULL DEFAULT 0,
    fail_count   INTEGER NOT NULL DEFAULT 0,
    fail_reason  TEXT NOT NULL DEFAULT '',
    first_seen   TEXT NOT NULL,
    last_seen    TEXT NOT NULL
);
CREATE TABLE cursors (name TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '0');
CREATE TABLE feedback (
    job_key TEXT NOT NULL, action TEXT NOT NULL,
    created_at TEXT NOT NULL, notes TEXT NOT NULL DEFAULT ''
);
"""

NEW_COLUMNS = [
    "opportunity_score", "priority", "location_class", "seniority",
    "role_family", "experience_min", "experience_max", "classification_reasons",
]


@pytest.fixture
def legacy_db(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_SCHEMA)
    conn.execute(
        "INSERT INTO jobs(key,source,company,title,location,url,posted,score,label,"
        "work_type,salary,resume_match,description,first_seen,last_seen) "
        "VALUES('gh:1','greenhouse','Acme','Data Engineer','Austin, TX','http://x',"
        "'2026-08-01',92,'yes','Remote','',77,'jd text','2026-08-01','2026-08-01')"
    )
    conn.execute("INSERT INTO cursors(name,value) VALUES('boards_main','120')")
    conn.commit()
    conn.close()
    return str(path)


def test_legacy_database_opens_and_migrates(legacy_db):
    db = Database(legacy_db)
    cols = {r[1] for r in db._conn.execute("PRAGMA table_info(jobs)")}
    for col in NEW_COLUMNS:
        assert col in cols, f"migration did not add {col}"
    db.close()


def test_legacy_rows_survive_migration(legacy_db):
    db = Database(legacy_db)
    row = db._conn.execute("SELECT * FROM jobs WHERE key='gh:1'").fetchone()
    assert row["title"] == "Data Engineer"
    assert row["score"] == 92
    assert row["label"] == "yes"
    assert row["resume_match"] == 77
    # New columns default rather than nulling the row out.
    assert row["opportunity_score"] == 0
    assert row["priority"] == ""
    assert row["experience_min"] is None
    db.close()


def test_cursor_survives_migration(legacy_db):
    """Board sweep position must not reset — that would re-bootstrap boards."""
    db = Database(legacy_db)
    assert db.get_cursor("boards_main") == 120
    db.close()


def test_migration_is_idempotent(legacy_db):
    Database(legacy_db).close()
    db = Database(legacy_db)
    assert db._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
    db.close()


def test_new_fields_round_trip(legacy_db):
    db = Database(legacy_db)
    db.mark_job_seen(
        key="gh:2", source="greenhouse", company="Acme",
        title="New Grad Software Engineer", location="Remote - US",
        url="http://y", posted="2026-08-31", score=95, label="yes",
        opportunity_score=91, priority="APPLY_NOW", location_class="US_REMOTE",
        seniority="entry", role_family="software_engineering",
        experience_min=0, experience_max=2,
        classification_reasons=["NEW_GRAD_EXPLICIT", "US_REMOTE_CONFIRMED"],
    )
    row = db._conn.execute("SELECT * FROM jobs WHERE key='gh:2'").fetchone()
    assert row["opportunity_score"] == 91
    assert row["priority"] == "APPLY_NOW"
    assert row["location_class"] == "US_REMOTE"
    assert row["seniority"] == "entry"
    assert row["experience_min"] == 0
    assert row["experience_max"] == 2
    assert row["classification_reasons"] == "NEW_GRAD_EXPLICIT,US_REMOTE_CONFIRMED"
    db.close()


def test_mark_job_seen_still_works_without_new_fields(legacy_db):
    """Callers that predate Phase 1 must keep working unchanged."""
    db = Database(legacy_db)
    db.mark_job_seen(
        key="gh:3", source="lever", company="Beta", title="Data Engineer",
        location="Remote", url="http://z", posted="", score=78, label="yes",
    )
    row = db._conn.execute("SELECT * FROM jobs WHERE key='gh:3'").fetchone()
    assert row["priority"] == ""
    assert row["opportunity_score"] == 0
    db.close()


def test_fresh_database_has_the_new_columns(tmp_path):
    db = Database(str(tmp_path / "fresh.db"))
    cols = {r[1] for r in db._conn.execute("PRAGMA table_info(jobs)")}
    for col in NEW_COLUMNS:
        assert col in cols
    db.close()
