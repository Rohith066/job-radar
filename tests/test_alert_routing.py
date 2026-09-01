"""Alert routing, ranking, and bootstrap-preservation tests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.main import screen_job, priority_rank_key, _process_one_board, load_boards_csv
from src.screening.scoring import APPLY_NOW, STRONG, REVIEW, LOW, REJECT
from src.sources.base import Job

NOW = datetime.now(timezone.utc)


def mk(title, location, description="", hours_old=2, country_focus=""):
    j = Job(key=title, source="greenhouse", company="Acme", title=title,
            location=location, url="http://x",
            posted=(NOW - timedelta(hours=hours_old)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            description=description)
    j.country_focus = country_focus
    result = screen_job(j)
    j.opportunity_score = result.score
    j.priority = result.priority
    j.classification_reasons = list(result.reason_codes)
    return j


def route(jobs):
    """Mirror of the routing in _dispatch_results."""
    yes = sorted([j for j in jobs if j.priority in (APPLY_NOW, STRONG)], key=priority_rank_key)
    maybe = sorted([j for j in jobs if j.priority == REVIEW], key=priority_rank_key)
    return yes, maybe


def test_screen_job_populates_the_job_record():
    j = mk("New Grad Software Engineer", "Remote - US", "0-2 years of experience")
    assert j.priority == APPLY_NOW
    assert j.seniority == "entry"
    assert j.role_family == "software_engineering"
    assert j.location_class == "US_REMOTE"
    assert j.experience_min == 0
    assert j.classification_reasons


def test_apply_now_and_strong_go_to_the_immediate_section():
    jobs = [
        mk("New Grad Software Engineer", "Remote - US", "0-2 years of experience"),
        mk("Software Engineer", "New York, NY", "2-3 years of relevant experience"),
    ]
    yes, maybe = route(jobs)
    assert len(yes) == 2
    assert maybe == []


def test_review_goes_to_the_digest():
    j = mk("Data Engineer", "Remote", "", hours_old=2, country_focus="US")
    if j.priority == REVIEW:
        yes, maybe = route([j])
        assert maybe == [j] and yes == []


def test_low_priority_generates_no_alert():
    # Secondary family on an unscoped remote: the recall floor covers only
    # first-class target families, so this legitimately lands in LOW.
    j = mk("Data Analyst", "Remote", "", hours_old=24 * 30, country_focus="Global")
    assert j.priority == LOW
    yes, maybe = route([j])
    assert yes == [] and maybe == []


def test_rejected_jobs_never_alert():
    for title in ("Senior Software Engineer", "Software Engineering Manager",
                  "Software Engineer IV"):
        j = mk(title, "Remote - US", "0-2 years of experience", hours_old=0.5)
        assert j.priority == REJECT
        yes, maybe = route([j])
        assert yes == [] and maybe == []


def test_ranking_puts_apply_now_above_strong():
    strong = mk("Software Engineer", "New York, NY", "2-3 years of relevant experience")
    apply_now = mk("New Grad Software Engineer", "Remote - US", "0-2 years of experience")
    ordered = sorted([strong, apply_now], key=priority_rank_key)
    assert ordered[0] is apply_now


def test_ranking_breaks_ties_on_score_then_recency():
    older = mk("Software Engineer I", "Austin, TX", "1-2 years of experience", hours_old=40)
    newer = mk("Software Engineer I", "Austin, TX", "1-2 years of experience", hours_old=2)
    ordered = sorted([older, newer], key=priority_rank_key)
    assert ordered[0] is newer


# ── Bootstrap suppression must be unchanged ───────────────────────────────
class _StubSource:
    board_id = "greenhouse:acme"

    def fetch(self, seen_keys, timeout=30):
        return [Job(key="k1", source="greenhouse", company="Acme",
                    title="Data Engineer", location="Austin, TX", url="http://x")]


class _StubDB:
    def __init__(self, bootstrapped):
        self._bootstrapped = bootstrapped
        self.upserts = []

    def is_board_dead(self, board_id):
        return False

    def is_board_bootstrapped(self, board_id):
        return self._bootstrapped

    def upsert_board(self, **kw):
        self.upserts.append(kw)


@pytest.fixture
def board():
    return {"company": "Acme", "platform": "greenhouse",
            "board_url": "https://boards.greenhouse.io/acme", "country_focus": "US"}


def test_first_run_of_a_board_emits_no_jobs(board, monkeypatch):
    """A newly discovered board must not email its entire history."""
    monkeypatch.setattr("src.main._board_source_for", lambda b: _StubSource())
    db = _StubDB(bootstrapped=False)
    jobs, err = _process_one_board(board, db, timeout=5)
    assert jobs == []
    assert err is None
    assert db.upserts, "the board should still be recorded"


def test_subsequent_runs_emit_jobs(board, monkeypatch):
    monkeypatch.setattr("src.main._board_source_for", lambda b: _StubSource())
    db = _StubDB(bootstrapped=True)
    jobs, err = _process_one_board(board, db, timeout=5)
    assert len(jobs) == 1
    assert err is None


def test_board_metadata_is_stamped_onto_jobs(board, monkeypatch):
    """country_focus must reach the job without touching the ATS adapters."""
    monkeypatch.setattr("src.main._board_source_for", lambda b: _StubSource())
    db = _StubDB(bootstrapped=True)
    jobs, _ = _process_one_board(board, db, timeout=5)
    assert jobs[0].country_focus == "US"


# ── The boards CSV keeps its metadata ─────────────────────────────────────
def test_boards_csv_preserves_country_focus(tmp_path):
    csv_path = tmp_path / "boards.csv"
    csv_path.write_text(
        "company_name,platform,board_url,country_focus,notes\n"
        "Acme,greenhouse,https://boards.greenhouse.io/acme,US,verified\n"
        "Globex,lever,https://jobs.lever.co/globex,Global,\n"
        "Initech,greenhouse,https://boards.greenhouse.io/initech,,\n"
    )
    rows = load_boards_csv(str(csv_path))
    assert len(rows) == 3
    focus = {r["company"]: r["country_focus"] for r in rows}
    assert focus == {"Acme": "US", "Globex": "Global", "Initech": ""}


def test_boards_csv_still_dedupes_and_filters(tmp_path):
    csv_path = tmp_path / "boards.csv"
    csv_path.write_text(
        "company_name,platform,board_url,country_focus,ok\n"
        "Acme,greenhouse,https://boards.greenhouse.io/acme,US,true\n"
        "Acme,greenhouse,https://boards.greenhouse.io/acme,US,true\n"
        "Dead,greenhouse,https://boards.greenhouse.io/dead,US,false\n"
    )
    rows = load_boards_csv(str(csv_path))
    assert len(rows) == 1
    assert rows[0]["company"] == "Acme"
