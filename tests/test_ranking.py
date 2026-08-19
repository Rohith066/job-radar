"""Regression tests for alert ordering.

Contract under test (see main.fit_rank_key):
  PRIMARY   hybrid resume-match score, descending
  SECONDARY posted/discovered time, descending
  Unscored jobs sort AFTER all scored jobs, among themselves by recency.

These exist because the previous behaviour ranked purely by recency, which let
a 5-minute-old weak match outrank a 2-hour-old strong one at the top of the
email. Every hard filter and the freshness window already run before ranking,
so recency is not the useful signal at this stage — fit is.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.main import fit_rank_key
from src.notifier import fit_band
from src.matching.config import BAND_MODERATE, BAND_STRONG
from src.sources.base import Job


def mk(resume_match: int = 0, posted: str = "", title: str = "Data Engineer") -> Job:
    """Minimal Job carrying only what ranking reads."""
    return Job(
        key=f"k{resume_match}{posted}{title}", source="test", company="Co",
        title=title, location="Remote, US", url="https://example.com",
        posted=posted, score=80, label="yes", resume_match=resume_match,
    )


def ago(**kw) -> str:
    """ISO timestamp N units in the past — parseable by _parse_posted."""
    return (datetime.now(timezone.utc) - timedelta(**kw)).strftime("%Y-%m-%dT%H:%M:%S%z")


def order(jobs: list[Job]) -> list[int]:
    return [j.resume_match for j in sorted(jobs, key=fit_rank_key)]


# ── PRIMARY: fit score descending ────────────────────────────────────────────

def test_sorts_by_fit_not_recency():
    """The spec case: a newer weak match must not outrank an older strong one."""
    jobs = [
        mk(78, ago(minutes=10)),
        mk(91, ago(hours=3)),
        mk(86, ago(hours=1)),
    ]
    assert order(jobs) == [91, 86, 78]


def test_newest_job_with_lowest_fit_ranks_last():
    jobs = [
        mk(92, ago(hours=2)),
        mk(89, ago(minutes=20)),
        mk(87, ago(hours=1)),
        mk(81, ago(minutes=5)),
    ]
    assert order(jobs) == [92, 89, 87, 81]


# ── SECONDARY: recency breaks ties ───────────────────────────────────────────

def test_equal_fit_breaks_tie_on_recency():
    older, newer = mk(91, ago(hours=2)), mk(91, ago(minutes=20))
    ranked = sorted([older, newer], key=fit_rank_key)
    assert ranked[0] is newer, "on equal fit, the newer job must come first"


def test_tie_break_is_stable_across_many():
    jobs = [mk(85, ago(hours=h)) for h in (5, 1, 3)]
    ranked = sorted(jobs, key=fit_rank_key)
    assert [j.posted for j in ranked] == [
        jobs[1].posted, jobs[2].posted, jobs[0].posted
    ]


# ── Missing / unscored jobs ──────────────────────────────────────────────────

def test_unscored_jobs_sort_after_scored():
    jobs = [mk(0, ago(minutes=1)), mk(70, ago(days=2)), mk(0, ago(hours=5))]
    ranked = sorted(jobs, key=fit_rank_key)
    assert ranked[0].resume_match == 70, "a scored job outranks any unscored job"
    assert all(j.resume_match == 0 for j in ranked[1:])


def test_unscored_jobs_ordered_by_recency_among_themselves():
    old, new = mk(0, ago(hours=10)), mk(0, ago(minutes=5))
    ranked = sorted([old, new], key=fit_rank_key)
    assert ranked[0] is new


def test_missing_resume_match_attribute_does_not_crash():
    """Legacy Job objects may predate the resume_match field."""
    j = mk(50, ago(hours=1))
    delattr_ok = True
    try:
        object.__setattr__(j, "resume_match", None)  # simulate null from DB
    except Exception:
        delattr_ok = False
    assert delattr_ok
    fit_rank_key(j)  # must not raise


def test_unparseable_posted_date_does_not_crash():
    jobs = [mk(90, "not a date"), mk(95, ago(hours=1))]
    assert order(jobs) == [95, 90]


def test_empty_list():
    assert sorted([], key=fit_rank_key) == []


# ── Fit band labelling uses the calibrated constants ─────────────────────────

def test_fit_band_uses_calibrated_thresholds():
    assert fit_band(BAND_STRONG) == "STRONG FIT"
    assert fit_band(BAND_STRONG + 5) == "STRONG FIT"
    assert fit_band(BAND_STRONG - 1) == "MODERATE FIT"
    assert fit_band(BAND_MODERATE) == "MODERATE FIT"
    assert fit_band(BAND_MODERATE - 1) == "WEAK FIT"


def test_fit_band_empty_for_unscored():
    assert fit_band(0) == ""


# ── Ranking must not disturb filtering ───────────────────────────────────────

def test_ranking_preserves_every_job():
    jobs = [mk(n, ago(hours=i)) for i, n in enumerate([90, 0, 75, 88, 0])]
    assert len(sorted(jobs, key=fit_rank_key)) == len(jobs)


def test_ranking_does_not_mutate_jobs():
    jobs = [mk(90, ago(hours=1)), mk(70, ago(hours=2))]
    before = [(j.resume_match, j.posted, j.label, j.score) for j in jobs]
    sorted(jobs, key=fit_rank_key)
    after = [(j.resume_match, j.posted, j.label, j.score) for j in jobs]
    assert before == after, "ranking is read-only"


def test_email_renders_fit_headline_and_gaps():
    """The score/band and match detail must reach the rendered email."""
    from src.notifier import _build_html
    j = mk(92, ago(hours=2), title="Analytics Engineer")
    j.matched_skills = ["Python", "SQL", "dbt"]
    j.missing_required = ["Looker"]
    html = _build_html([j], [], "main")
    assert "92 &mdash; STRONG FIT" in html or "92 — STRONG FIT" in html
    assert "Matched:" in html and "dbt" in html
    assert "Gap:" in html and "Looker" in html
