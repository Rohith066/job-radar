"""Job Radar — main orchestrator and CLI entry point.

Improvements over the original watcher.py:
- Modular source architecture (each source is an independent class)
- SQLite state (no more unboundedly growing JSON files)
- Concurrent main-source fetching via ThreadPoolExecutor
- Per-platform semaphores in boards mode
- HTML emails + optional Slack/Discord webhooks
- Structured logging (replace print statements)
- Rich CLI output with live progress
- Score-based job ranking
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from .classifier import is_match, classify
from .profile import skill_bonus
from .config import Config
from .database import Database
from .notifier import CompositeNotifier, EmailNotifier, SlackNotifier, DiscordNotifier
from .sources.base import Job, is_us_location
from .utils.salary import detect_work_type, salary_passes_filter
from .ml.scorer import ml_rescore, get_model_info
from .dashboard import run_dashboard
from .company_filter import company_score_adjustment
from .resume_matcher import batch_score_jobs
from .sources.eightfold import EightfoldSource
from .sources.amazon import AmazonSource
from .sources.goldman import GoldmanSachsSource
from .sources.ibm import IBMSource
from .sources.oracle import OracleSource
from .sources.meta import MetaSource
from .sources.google import GoogleSource
from .sources.apple import AppleSource
from .sources.netflix import NetflixSource
from .sources.stripe import StripeSource
from .sources.linkedin import LinkedInSource
from .sources.greenhouse import GreenhouseSource, _board_id as gh_board_id
from .sources.lever import LeverSource, _board_id as lever_board_id
from .sources.smartrecruiters import SmartRecruitersSource, _board_id as sr_board_id
from .sources.workday import WorkdaySource, _board_id as wd_board_id

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

SUPPORTED_BOARD_PLATFORMS = ("greenhouse", "lever", "smartrecruiters", "workday")

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-7s %(name)s — %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S")
    # Quiet noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Board CSV loader
# ---------------------------------------------------------------------------

def load_boards_csv(path: str) -> list[dict]:
    raw = (path or "").strip()
    if not raw:
        raise FileNotFoundError("No boards CSV path specified.")

    p = Path(os.path.expanduser(raw))
    if not p.is_absolute():
        for base in (Path.cwd(), Path(ROOT_DIR)):
            candidate = base / p
            if candidate.exists():
                p = candidate
                break

    if not p.exists():
        raise FileNotFoundError(f"Boards CSV not found: {path}")

    rows: list[dict] = []
    with open(p, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            company = (r.get("company_name") or r.get("company") or "").strip()
            platform = (r.get("platform") or "").strip().lower()
            url = (r.get("board_url") or r.get("url") or "").strip()
            ok_val = (r.get("ok") or "").strip().lower()
            if ok_val and ok_val not in ("true", "1", "yes"):
                continue
            if not company or not platform or not url:
                continue
            rows.append({"company": company, "platform": platform, "board_url": url.rstrip("/")})

    # Deduplicate on (platform, url)
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for r in rows:
        k = (r["platform"], r["board_url"])
        if k not in seen:
            seen.add(k)
            deduped.append(r)

    return deduped


def _resolve_boards_csv(cfg_path: str) -> str:
    """Try several fallback locations for the boards CSV."""
    candidates = [
        cfg_path,
        os.environ.get("BOARDS_CSV", ""),
        "data/boards/JOB_BOARDS_PURE_WORKING_SUPPORTED_round2.csv",
        "data/boards/JOB_BOARDS_PURE_WORKING_round2.csv",
        "data/boards/JOB_BOARDS_OK_PRODUCTION.csv",
    ]
    for raw in candidates:
        if not raw:
            continue
        p = Path(os.path.expanduser(raw))
        if not p.is_absolute():
            for base in (Path.cwd(), Path(ROOT_DIR)):
                candidate = base / p
                if candidate.exists():
                    return str(candidate)
        elif p.exists():
            return str(p)
    raise FileNotFoundError("Could not locate a boards CSV file. Specify --boards-csv or set BOARDS_CSV.")


# ---------------------------------------------------------------------------
# Notifier factory
# ---------------------------------------------------------------------------

def build_notifier(cfg: Config) -> CompositeNotifier:
    notifiers = []

    email = EmailNotifier(
        user=cfg.email.user,
        password=cfg.email.password,
        to=cfg.email.to,
        smtp_host=cfg.email.smtp_host,
        smtp_port=cfg.email.smtp_port,
    )
    if email.is_configured():
        notifiers.append(email)
    else:
        log.warning("Email not configured — no email alerts will be sent.")

    slack = SlackNotifier(cfg.slack.webhook_url)
    if slack.is_configured():
        notifiers.append(slack)

    discord = DiscordNotifier(cfg.discord.webhook_url)
    if discord.is_configured():
        notifiers.append(discord)

    return CompositeNotifier(notifiers)


# ---------------------------------------------------------------------------
# Job age filter — drop stale listings older than MAX_JOB_AGE_DAYS
# ---------------------------------------------------------------------------

MAX_JOB_AGE_DAYS = 7   # only alert on jobs posted within the last 7 days

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%d",
    "%B %d, %Y",       # "March 14, 2026"
    "%b %d, %Y",       # "Mar 14, 2026"
    "%d %B %Y",        # "14 March 2026"
    "%d %b %Y",        # "14 Mar 2026"
]

# Relative date patterns: "2 days ago", "3 hours ago", "1 week ago", "just now", etc.
_RELATIVE_RE = re.compile(
    r"""
    (?:
        (?P<num>\d+)\s*
        (?P<unit>second|minute|hour|day|week|month)s?\s+ago
      | (?P<today>today|just\s+now|moments?\s+ago)
      | (?P<yesterday>yesterday)
      | (?P<daysago>\d+)[dD]\s*(?:ago)?       # "3d ago" or "3d"
      | (?P<hoursago>\d+)[hH]\s*(?:ago)?      # "5h ago" or "5h"
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _parse_posted(posted: str) -> Optional[datetime]:
    if not posted:
        return None
    now = datetime.now(timezone.utc)
    s = str(posted).strip()

    # Try absolute date formats first
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s[:26], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            continue

    # Try relative date strings
    m = _RELATIVE_RE.search(s)
    if m:
        if m.group("today"):
            return now
        if m.group("yesterday"):
            return now - timedelta(days=1)
        if m.group("daysago"):
            return now - timedelta(days=int(m.group("daysago")))
        if m.group("hoursago"):
            return now - timedelta(hours=int(m.group("hoursago")))
        num  = int(m.group("num"))
        unit = m.group("unit").lower()
        delta = {
            "second": timedelta(seconds=num),
            "minute": timedelta(minutes=num),
            "hour":   timedelta(hours=num),
            "day":    timedelta(days=num),
            "week":   timedelta(weeks=num),
            "month":  timedelta(days=num * 30),
        }.get(unit)
        if delta:
            return now - delta

    return None


def _is_too_old(posted: str, max_days: int = MAX_JOB_AGE_DAYS) -> bool:
    """Return True if job was posted more than max_days ago.

    If the date cannot be parsed at all, the job is filtered OUT (strict mode)
    to prevent old jobs with unparseable dates from slipping through forever.
    """
    dt = _parse_posted(posted)
    if dt is None:
        # No posted date at all → treat as brand new (source doesn't provide dates)
        return False
    return dt < datetime.now(timezone.utc) - timedelta(days=max_days)


def _dedup_jobs(jobs: list[Job]) -> list[Job]:
    """Remove duplicate jobs with identical (company, title) — keeps highest score."""
    best: dict[tuple, Job] = {}
    for j in jobs:
        fp = (j.company.strip().lower(), j.title.strip().lower())
        if fp not in best or j.score > best[fp].score:
            best[fp] = j
    return list(best.values())


# ---------------------------------------------------------------------------
# Main mode: company career pages
# ---------------------------------------------------------------------------

def run_main(cfg: Config, db: Database, notifier: CompositeNotifier, *, dry_run: bool, no_notify: bool, test_notify: bool) -> None:
    """Fetch jobs from configured company sources concurrently."""
    timeout = cfg.http_timeout

    # Build source list based on config
    sources = []
    if cfg.source("microsoft").enabled:
        sources.append(EightfoldSource("microsoft", max_jobs=cfg.source("microsoft").max_jobs))
    if cfg.source("nvidia").enabled:
        sources.append(EightfoldSource("nvidia", max_jobs=cfg.source("nvidia").max_jobs))
    if cfg.source("amazon").enabled:
        sources.append(AmazonSource(max_jobs=cfg.source("amazon").max_jobs))
    if cfg.source("goldman_sachs").enabled:
        sources.append(GoldmanSachsSource(max_jobs=cfg.source("goldman_sachs").max_jobs))
    if cfg.source("ibm").enabled:
        sources.append(IBMSource(max_jobs=cfg.source("ibm").max_jobs))
    if cfg.source("oracle").enabled:
        sources.append(OracleSource(max_jobs=cfg.source("oracle").max_jobs))
    if cfg.source("meta").enabled:
        sources.append(MetaSource(max_jobs=cfg.source("meta").max_jobs))
    if cfg.source("google").enabled:
        sources.append(GoogleSource(max_jobs=cfg.source("google").max_jobs))
    if cfg.source("apple").enabled:
        sources.append(AppleSource(max_jobs=cfg.source("apple").max_jobs))
    if cfg.source("netflix").enabled:
        sources.append(NetflixSource(max_jobs=cfg.source("netflix").max_jobs))
    if cfg.source("stripe").enabled:
        sources.append(StripeSource(max_jobs=cfg.source("stripe").max_jobs))
    if cfg.source("linkedin").enabled:
        sources.append(LinkedInSource(max_jobs=cfg.source("linkedin").max_jobs))

    if not sources:
        log.warning("No sources enabled.")
        return

    log.info("Running MAIN mode — %d source(s)", len(sources))

    # Fetch all sources concurrently
    all_jobs: list[Job] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=min(len(sources), 12)) as pool:
        future_map = {
            pool.submit(_fetch_source, src, db, timeout): src.name
            for src in sources
        }
        for fut in as_completed(future_map):
            src_name = future_map[fut]
            try:
                jobs = fut.result()
                all_jobs.extend(jobs)
                log.info("%-20s fetched %d jobs", src_name, len(jobs))
            except Exception as exc:
                err = f"{src_name}: {type(exc).__name__}: {exc}"
                errors.append(err)
                log.error("Source failed — %s", err)

    _dispatch_results(
        all_jobs=all_jobs, errors=errors, db=db, notifier=notifier,
        mode="main", dry_run=dry_run, no_notify=no_notify, test_notify=test_notify,
        cfg=cfg,
    )


def _fetch_source(source, db: Database, timeout: int) -> list[Job]:
    seen_keys = db.get_seen_keys(source.name)
    return source.fetch(seen_keys=seen_keys, timeout=timeout)


# ---------------------------------------------------------------------------
# Boards mode: ATS board sweep
# ---------------------------------------------------------------------------

_BOARD_SEMAPHORES: dict[str, threading.Semaphore] = {
    "greenhouse": threading.Semaphore(8),
    "lever": threading.Semaphore(8),
    "smartrecruiters": threading.Semaphore(6),
    "workday": threading.Semaphore(4),
}


def run_boards(
    cfg: Config,
    db: Database,
    notifier: CompositeNotifier,
    *,
    boards_csv: str,
    batch_size: int,
    timeout: int,
    workers: int,
    dry_run: bool,
    no_notify: bool,
    test_notify: bool,
    run_until_wrap: bool = False,
    max_iterations: int = 2000,
    export_dead_csv: str = "",
) -> None:
    boards = load_boards_csv(boards_csv)
    boards = [b for b in boards if b.get("platform") in SUPPORTED_BOARD_PLATFORMS]

    if not boards:
        log.error("No supported boards found in CSV.")
        return

    platform_counts = Counter(b["platform"] for b in boards)
    log.info(
        "Boards CSV: %d supported rows | %s",
        len(boards),
        " ".join(f"{p}={c}" for p, c in sorted(platform_counts.items())),
    )

    cursor_key = "boards_main"
    n = len(boards)

    def run_one_batch() -> int:
        cursor = db.get_cursor(cursor_key)
        start = cursor % n
        end = min(start + max(batch_size, 1), n)
        batch = boards[start:end]

        log.info("Processing batch [%d:%d] of %d boards", start, end, n)

        t0 = time.time()
        all_jobs, errors = _process_boards_batch(batch, db, timeout, workers)
        elapsed = time.time() - t0
        log.info("Batch done in %.1fs — %d jobs fetched, %d errors", elapsed, len(all_jobs), len(errors))

        for err in errors:
            log.warning("Board error: %s", err)

        _dispatch_results(
            all_jobs=all_jobs, errors=errors, db=db, notifier=notifier,
            mode="boards", dry_run=dry_run, no_notify=no_notify, test_notify=test_notify,
            cfg=cfg,
        )

        new_cursor = end if end < n else 0
        if not dry_run:
            db.set_cursor(cursor_key, new_cursor)
            if export_dead_csv:
                db.export_dead_boards_csv(export_dead_csv)

        return new_cursor

    if run_until_wrap:
        for it in range(1, max_iterations + 1):
            cur = run_one_batch()
            log.info("[%d] cursor=%d", it, cur)
            if cur == 0:
                log.info("Full sweep complete (cursor wrapped to 0).")
                break
    else:
        run_one_batch()


def _process_boards_batch(
    batch: list[dict], db: Database, timeout: int, workers: int
) -> tuple[list[Job], list[str]]:
    all_jobs: list[Job] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [
            pool.submit(_process_one_board, b, db, timeout)
            for b in batch
        ]
        for fut in as_completed(futures):
            try:
                jobs, err = fut.result()
                if err:
                    errors.append(err)
                all_jobs.extend(jobs)
            except Exception as exc:
                errors.append(f"Board thread error: {type(exc).__name__}: {exc}")

    return all_jobs, errors


def _board_source_for(b: dict) -> Optional[object]:
    platform = b["platform"]
    company = b["company"]
    url = b["board_url"]
    if platform == "greenhouse":
        return GreenhouseSource(company, url)
    if platform == "lever":
        return LeverSource(company, url)
    if platform == "smartrecruiters":
        return SmartRecruitersSource(company, url)
    if platform == "workday":
        return WorkdaySource(company, url)
    return None


def _get_board_id(b: dict) -> str:
    platform = b["platform"]
    url = b["board_url"]
    if platform == "greenhouse":
        return gh_board_id(url)
    if platform == "lever":
        return lever_board_id(url)
    if platform == "smartrecruiters":
        return sr_board_id(url)
    if platform == "workday":
        return wd_board_id(url)
    return f"{platform}:"


def _process_one_board(b: dict, db: Database, timeout: int) -> tuple[list[Job], Optional[str]]:
    import requests

    platform = b["platform"]
    company = b["company"]
    url = b["board_url"]
    board_id = _get_board_id(b)

    if db.is_board_dead(board_id):
        return [], None

    source = _board_source_for(b)
    if source is None:
        return [], None

    sem = _BOARD_SEMAPHORES.get(platform)
    t0 = time.time()

    try:
        with sem:
            jobs = source.fetch(seen_keys=set(), timeout=timeout)

        elapsed = time.time() - t0

        # Bootstrap: if this is the first time we see this board, don't emit jobs
        is_first_run = not db.is_board_bootstrapped(board_id)
        if is_first_run:
            db.upsert_board(board_id=board_id, platform=platform, company=company, url=url, job_count=len(jobs))
            log.debug("Board bootstrapped: %s (%d jobs suppressed)", board_id, len(jobs))
            return [], None

        db.upsert_board(board_id=board_id, platform=platform, company=company, url=url, job_count=len(jobs))
        return jobs, None

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in (404, 410):
            db.upsert_board(board_id=board_id, platform=platform, company=company, url=url,
                            status="dead", fail_reason=f"HTTP {status}")
            return [], f"DEAD {board_id}: HTTP {status}"
        return [], f"{board_id}: HTTPError {status}: {exc}"

    except Exception as exc:
        return [], f"{board_id}: {type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# Shared result dispatcher
# ---------------------------------------------------------------------------

def _dispatch_results(
    *,
    all_jobs: list[Job],
    errors: list[str],
    db: Database,
    notifier: CompositeNotifier,
    mode: str,
    dry_run: bool,
    no_notify: bool,
    test_notify: bool,
    cfg: Config,
) -> None:
    # Filter by classification + location
    matched = [
        j for j in all_jobs
        if j.label in ("yes", "maybe") and (
            not cfg.filter.require_us_location or is_us_location(j.location)
        )
    ]

    # Age filter — drop listings older than MAX_JOB_AGE_DAYS
    before_age = len(matched)
    matched = [j for j in matched if not _is_too_old(j.posted)]
    dropped = before_age - len(matched)
    if dropped:
        log.info("Age filter: dropped %d stale job(s) older than %d days", dropped, MAX_JOB_AGE_DAYS)

    # Salary floor filter — drop jobs with explicitly stated sub-floor salaries
    before_salary = len(matched)
    matched = [j for j in matched if salary_passes_filter(getattr(j, "salary", ""))]
    salary_dropped = before_salary - len(matched)
    if salary_dropped:
        log.info("Salary filter: dropped %d job(s) below $%s/yr floor", salary_dropped, "90,000")

    # Company filter — hard-exclude staffing agencies / federal contractors,
    # and apply score bonus for confirmed H1B sponsors
    before_company = len(matched)
    filtered_matched = []
    for j in matched:
        adj, reason = company_score_adjustment(j.company)
        if adj == -999:
            log.debug("Company filter excluded: %s (%s)", j.company, reason)
            continue
        if adj > 0:
            j.score = min(100, j.score + adj)
            # Re-evaluate label after boost
            j.label = "yes" if j.score >= 70 else "maybe" if j.score >= 40 else "no"
        filtered_matched.append(j)
    excluded_count = before_company - len(filtered_matched)
    if excluded_count:
        log.info("Company filter: excluded %d job(s) (staffing/federal/mismatch)", excluded_count)
    matched = filtered_matched

    # Deduplication — keep only one entry per (company, title) pair
    before_dedup = len(matched)
    matched = _dedup_jobs(matched)
    dupes = before_dedup - len(matched)
    if dupes:
        log.info("Dedup: removed %d duplicate(s)", dupes)

    # Apply resume skill-match bonus + detect work type
    for j in matched:
        bonus = skill_bonus(j.title)
        j.score = min(100, j.score + bonus)
        if not j.work_type:
            j.work_type = detect_work_type(title=j.title, location=j.location)

    # Resume-vs-JD matching — fetch JD for jobs that don't have one yet,
    # score against master resume, apply score adjustment, and auto-feed ML
    resume_path = cfg.resume_path if hasattr(cfg, "resume_path") else "config/master_resume.txt"
    matched = batch_score_jobs(matched, resume_path=resume_path)
    for j in matched:
        if j.resume_match > 0:
            # +0 to +15 bonus scaled by how well resume matches JD
            bonus = int(j.resume_match * 0.15)
            j.score = min(100, j.score + bonus)
            j.label = "yes" if j.score >= 70 else "maybe" if j.score >= 40 else "no"
            # Auto-seed ML feedback for strong matches — bootstraps model
            # without waiting for manual --applied / --interested input
            if j.resume_match >= 75 and db.is_new_job(j.key):
                try:
                    db.record_feedback(j.key, "interested")
                    log.debug("Auto-feedback 'interested' for %s — %s (match %d%%)",
                              j.company, j.title, j.resume_match)
                except Exception:
                    pass

    # ML re-scoring: adjusts scores based on your applied/dismissed feedback
    # Skipped automatically if fewer than 10 feedback entries exist (cold start)
    matched = ml_rescore(matched, db=db)

    yes_jobs = sorted([j for j in matched if j.label == "yes"], key=lambda j: j.score, reverse=True)
    maybe_jobs = sorted([j for j in matched if j.label == "maybe"], key=lambda j: j.score, reverse=True)

    log.info("Matched: %d yes, %d maybe", len(yes_jobs), len(maybe_jobs))

    # Summarise source errors for the email footer
    source_errors = [e for e in errors if e]

    if test_notify:
        sample_yes = yes_jobs[:2]
        sample_maybe = maybe_jobs[:1]
        if not (sample_yes or sample_maybe):
            log.error("No matching jobs found for test notification.")
            sys.exit(1)
        if not no_notify:
            errs = notifier.notify(sample_yes, sample_maybe, subject_prefix=f"[TEST Job Radar]", mode=mode, source_errors=source_errors)
            for e in errs:
                log.error("Notifier error: %s", e)
        else:
            log.info("[TEST] Would notify: %d yes + %d maybe", len(sample_yes), len(sample_maybe))
        return

    # Determine which jobs are new (not yet in DB)
    new_yes = [j for j in yes_jobs if db.is_new_job(j.key)]
    new_maybe = [j for j in maybe_jobs if db.is_new_job(j.key)]

    log.info("New jobs: %d yes, %d maybe", len(new_yes), len(new_maybe))

    if new_yes or new_maybe:
        if no_notify:
            log.info("[no-notify] Would alert: %d yes + %d maybe", len(new_yes), len(new_maybe))
        else:
            errs = notifier.notify(new_yes, new_maybe, subject_prefix="[Job Radar]", mode=mode, source_errors=source_errors)
            for e in errs:
                log.error("Notifier error: %s", e)
    else:
        log.info("No new matching jobs.")

    # Persist all seen jobs to DB
    if not dry_run:
        for j in matched:
            db.mark_job_seen(
                key=j.key, source=j.source, company=j.company, title=j.title,
                location=j.location, url=j.url, posted=j.posted,
                score=j.score, label=j.label,
                work_type=getattr(j, "work_type", ""),
                salary=getattr(j, "salary", ""),
                resume_match=getattr(j, "resume_match", 0),
                description=getattr(j, "description", ""),
            )
        log.debug("Saved %d jobs to database.", len(matched))

        # Auto-expiry — clean up jobs not seen in 60 days to keep DB lean
        db.expire_old_jobs(days=60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run_health_check(cfg: Config, db: Database, notifier: CompositeNotifier) -> None:
    """Send a weekly health-check summary email."""
    from datetime import datetime, timezone
    stats = db.get_stats()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    fb = db.get_feedback_stats()
    ml = get_model_info()
    subject = f"[Job Radar] Weekly Health Check — {ts}"
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
          background:#f5f5f5; margin:0; padding:20px; color:#333; }}
  .container {{ max-width:600px; margin:0 auto; background:#fff;
                border-radius:8px; overflow:hidden;
                box-shadow:0 2px 8px rgba(0,0,0,.1); }}
  .header {{ background:#1a1a2e; color:#fff; padding:20px 28px; }}
  .header h1 {{ margin:0; font-size:20px; }}
  .header p  {{ margin:4px 0 0; font-size:13px; color:#aaa; }}
  .body {{ padding:24px 28px; }}
  .stat {{ display:flex; justify-content:space-between; padding:10px 0;
           border-bottom:1px solid #f0f0f0; font-size:14px; }}
  .stat-val {{ font-weight:700; color:#1d4ed8; }}
  .section-head {{ font-size:13px; font-weight:700; text-transform:uppercase;
                   letter-spacing:.05em; color:#555; margin-top:18px; margin-bottom:4px; }}
  .ok {{ color:#16a34a; font-weight:700; }}
  .footer {{ background:#f9f9f9; border-top:1px solid #eee;
             padding:14px 28px; font-size:12px; color:#888; }}
</style></head><body>
<div class="container">
  <div class="header">
    <h1>Job Radar ✅ Weekly Health Check</h1>
    <p>{ts}</p>
  </div>
  <div class="body">
    <p class="ok">Job Radar is running and monitoring 920+ companies for you.</p>
    <p class="section-head">📈 Activity</p>
    <div class="stat"><span>Jobs found (last 24 hrs)</span><span class="stat-val">{stats['new_24h']}</span></div>
    <div class="stat"><span>Jobs found (last 7 days)</span><span class="stat-val">{stats['new_7d']}</span></div>
    <div class="stat"><span>Total YES matches in DB</span><span class="stat-val">{stats['yes_count']}</span></div>
    <div class="stat"><span>Total MAYBE matches in DB</span><span class="stat-val">{stats['maybe_count']}</span></div>
    <div class="stat"><span>Total jobs tracked</span><span class="stat-val">{stats['total_jobs']}</span></div>
    <div class="stat"><span>Last job activity</span><span class="stat-val">{stats['last_activity'][:19]}</span></div>
    <div class="stat"><span>ATS boards tracked</span><span class="stat-val">{stats['boards']['total']} ({stats['boards']['active']} active)</span></div>
    <p class="section-head">🤖 Your Feedback (ML Training Data)</p>
    <div class="stat"><span>✅ Applied</span><span class="stat-val">{fb['applied']}</span></div>
    <div class="stat"><span>🔖 Interested</span><span class="stat-val">{fb['interested']}</span></div>
    <div class="stat"><span>❌ Dismissed</span><span class="stat-val">{fb['dismissed']}</span></div>
    <div class="stat"><span>Total feedback entries</span><span class="stat-val">{fb['total']}</span></div>
    <p class="section-head">🧠 ML Model Status</p>
    <div class="stat"><span>Model trained</span><span class="stat-val">{"✅ Yes" if ml["trained"] else "⏳ Not yet (need 10+ feedback)"}</span></div>
    <div class="stat"><span>Features learned</span><span class="stat-val">{ml["features"]}</span></div>
    <p style="font-size:12px;color:#888;margin-top:12px;">
      💡 <b>Tip:</b> Use <code>python -m src.main --applied &lt;url&gt;</code> after applying to a job.
      Once you have 10+ feedback entries the ML model trains automatically and starts boosting
      jobs similar to ones you liked — and penalising ones you dismissed.
    </p>
  </div>
  <div class="footer">Powered by Job Radar — targeting Data Analyst · Data Scientist · Data Engineer</div>
</div></body></html>"""

    text = (
        f"Job Radar Weekly Health Check — {ts}\n\n"
        f"✅ Job Radar is running and monitoring 920+ companies.\n\n"
        f"=== Activity ===\n"
        f"Jobs found last 24h : {stats['new_24h']}\n"
        f"Jobs found last 7d  : {stats['new_7d']}\n"
        f"Total YES in DB     : {stats['yes_count']}\n"
        f"Total MAYBE in DB   : {stats['maybe_count']}\n"
        f"Total jobs tracked  : {stats['total_jobs']}\n"
        f"Last activity       : {stats['last_activity'][:19]}\n"
        f"ATS boards tracked  : {stats['boards']['total']} ({stats['boards']['active']} active)\n\n"
        f"=== Feedback (ML Training Data) ===\n"
        f"Applied    : {fb['applied']}\n"
        f"Interested : {fb['interested']}\n"
        f"Dismissed  : {fb['dismissed']}\n"
        f"Total      : {fb['total']}\n"
    )

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    for n in notifier._notifiers:
        if hasattr(n, "smtp_host"):  # EmailNotifier
            if not n.is_configured():
                log.warning("Email not configured for health check.")
                return
            import smtplib, ssl as _ssl
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = n.user
            msg["To"] = ", ".join(n.recipients)
            msg.attach(MIMEText(text, "plain", "utf-8"))
            msg.attach(MIMEText(html, "html", "utf-8"))
            try:
                import certifi
                ctx = _ssl.create_default_context(cafile=certifi.where())
            except ImportError:
                ctx = _ssl.create_default_context()
            if n.smtp_port == 465:
                with smtplib.SMTP_SSL(n.smtp_host, n.smtp_port, context=ctx) as s:
                    s.login(n.user, n.password)
                    s.sendmail(n.user, n.recipients, msg.as_string())
            else:
                with smtplib.SMTP(n.smtp_host, n.smtp_port) as s:
                    s.ehlo(); s.starttls(context=ctx); s.ehlo()
                    s.login(n.user, n.password)
                    s.sendmail(n.user, n.recipients, msg.as_string())
            log.info("Health check email sent to %s", ", ".join(n.recipients))
            return
    log.warning("No email notifier configured — health check skipped.")


def run_record_feedback(db: Database, identifier: str, action: str) -> None:
    """Record user feedback for a job identified by URL or key."""
    # Identifier can be a full URL or a job key.  We look up by URL first.
    rows = db._conn.execute(
        "SELECT key, company, title, url FROM jobs WHERE url=? OR key=?",
        (identifier.strip(), identifier.strip()),
    ).fetchall()

    if not rows:
        # Try partial URL match (user might have trimmed query params)
        ident_clean = identifier.strip().split("?")[0].rstrip("/")
        rows = db._conn.execute(
            "SELECT key, company, title, url FROM jobs WHERE url LIKE ?",
            (f"%{ident_clean}%",),
        ).fetchall()

    if not rows:
        log.error(
            "Job not found in DB for identifier: %s\n"
            "Tip: copy the exact URL from the email 'View Job →' link.",
            identifier,
        )
        sys.exit(1)

    if len(rows) > 1:
        log.warning("Multiple jobs matched — using the first one.")

    job_key = rows[0]["key"]
    company = rows[0]["company"] or "?"
    title   = rows[0]["title"] or "?"

    db.record_feedback(job_key, action)
    emoji = {"applied": "✅", "dismissed": "❌", "interested": "🔖"}.get(action, "📝")
    log.info("%s Recorded '%s' for: [%s] %s", emoji, action, company, title)


def run_feedback_summary(db: Database) -> None:
    """Print all feedback entries to the terminal."""
    stats = db.get_feedback_stats()
    print(f"\n{'='*60}")
    print(f"  Job Radar — Feedback Summary")
    print(f"{'='*60}")
    print(f"  ✅  Applied    : {stats['applied']}")
    print(f"  🔖  Interested : {stats['interested']}")
    print(f"  ❌  Dismissed  : {stats['dismissed']}")
    print(f"  📊  Total      : {stats['total']}")
    print(f"{'='*60}\n")

    for action_label, action_key in [("✅ APPLIED", "applied"), ("🔖 INTERESTED", "interested"), ("❌ DISMISSED", "dismissed")]:
        jobs = db.get_feedback_jobs(action_key)
        if not jobs:
            continue
        print(f"{action_label} ({len(jobs)})")
        print("-" * 60)
        for j in jobs:
            company = j.get("company") or "?"
            title   = j.get("title") or "?"
            score   = j.get("score") or 0
            ts      = (j.get("created_at") or "")[:10]
            print(f"  [{ts}] {company} — {title}  (score {score})")
        print()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="job-radar",
        description="Job Radar — aggregate and alert on new engineering jobs.",
    )
    p.add_argument("--config", default="config.yaml", help="Path to YAML config file (default: config.yaml)")
    p.add_argument("--mode", default="main", choices=["main", "boards"], help="Run mode (default: main)")
    p.add_argument("--dry-run", action="store_true", help="Fetch jobs but do not save state or send notifications.")
    p.add_argument("--no-notify", action="store_true", help="Save state but skip all notifications.")
    p.add_argument("--test-notify", action="store_true", help="Send a sample notification without updating state.")
    p.add_argument("--health-check", action="store_true", help="Send a weekly health-check summary email and exit.")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging.")

    # Feedback (ML training signal)
    fg = p.add_argument_group("Feedback (ML training)")
    fg.add_argument("--applied", metavar="JOB_URL_OR_KEY", help="Mark a job as applied (copy URL from the email link).")
    fg.add_argument("--dismiss", metavar="JOB_URL_OR_KEY", help="Mark a job as not interesting.")
    fg.add_argument("--interested", metavar="JOB_URL_OR_KEY", help="Mark a job as interesting (bookmarked for later).")
    fg.add_argument("--feedback", action="store_true", help="Print a summary of all recorded feedback and exit.")
    fg.add_argument("--dashboard", action="store_true", help="Open the local feedback dashboard in your browser (http://localhost:5100).")

    # Boards options
    bg = p.add_argument_group("Boards mode options")
    bg.add_argument("--boards-csv", default="", help="Path to boards CSV file.")
    bg.add_argument("--boards-batch-size", type=int, default=0, help="Boards per run (0 = use config value).")
    bg.add_argument("--boards-timeout", type=int, default=0, help="HTTP timeout for boards (0 = use config value).")
    bg.add_argument("--boards-workers", type=int, default=0, help="Parallel board workers (0 = use config value).")
    bg.add_argument("--boards-run-until-wrap", action="store_true", help="Run batches until cursor wraps (full sweep).")
    bg.add_argument("--boards-max-iterations", type=int, default=2000, help="Safety cap for --boards-run-until-wrap.")
    bg.add_argument("--export-dead-csv", default="", help="Export dead boards to a CSV file.")

    return p


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    cfg = Config.load(args.config)

    db = Database(cfg.database.path)

    notifier = build_notifier(cfg)

    try:
        if args.health_check:
            run_health_check(cfg=cfg, db=db, notifier=notifier)
            return

        if args.dashboard:
            run_dashboard(db=db)
            return

        if args.feedback:
            run_feedback_summary(db=db)
            return

        if args.applied:
            run_record_feedback(db=db, identifier=args.applied, action="applied")
            return

        if args.dismiss:
            run_record_feedback(db=db, identifier=args.dismiss, action="dismissed")
            return

        if args.interested:
            run_record_feedback(db=db, identifier=args.interested, action="interested")
            return

        if args.mode == "main":
            run_main(
                cfg=cfg, db=db, notifier=notifier,
                dry_run=args.dry_run, no_notify=args.no_notify, test_notify=args.test_notify,
            )
        else:
            # Boards mode — resolve CSV and override config values if CLI flags given
            boards_csv = args.boards_csv or cfg.boards.csv
            try:
                boards_csv = _resolve_boards_csv(boards_csv)
            except FileNotFoundError as exc:
                log.error("%s", exc)
                sys.exit(1)

            batch_size = args.boards_batch_size or cfg.boards.batch_size
            timeout = args.boards_timeout or cfg.boards.timeout
            workers = args.boards_workers or cfg.boards.workers

            run_boards(
                cfg=cfg, db=db, notifier=notifier,
                boards_csv=boards_csv,
                batch_size=batch_size,
                timeout=timeout,
                workers=workers,
                dry_run=args.dry_run,
                no_notify=args.no_notify,
                test_notify=args.test_notify,
                run_until_wrap=args.boards_run_until_wrap,
                max_iterations=args.boards_max_iterations,
                export_dead_csv=args.export_dead_csv,
            )
    finally:
        db.close()


if __name__ == "__main__":
    main()
