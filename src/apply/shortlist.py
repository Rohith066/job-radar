"""Daily application queue — "what should I apply to first today?".

Reads the database read-only, scores eligible jobs with the Phase 2 pipeline,
and renders a short ranked list. Deliberately short: a list of 400 jobs is not
a decision aid. Jobs the user has already acted on are excluded, so the queue
drains as it is worked rather than repeating yesterday's suggestions.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from .fit import analyze_fit, FitResult
from .priority import application_priority, ApplicationPriority, APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW
from .queue import ApplicationQueue
from ..matching import ontology
from ..screening import analyze_title, analyze_location, analyze_experience, score_job

DEFAULT_LIMIT = 15
MAX_PER_COMPANY = 2          # mirrors scripts/build_shortlist.py's existing cap

_DATE_FORMATS = ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"]


def _parse_posted(posted: str):
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime((posted or "")[:26], fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


def _age_str(posted: str) -> str:
    dt = _parse_posted(posted)
    if not dt:
        return "date unknown"
    h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    if h < 1:
        return "just posted"
    if h < 24:
        return f"posted {int(h)}h ago"
    return f"posted {int(h / 24)}d ago"


@dataclass
class QueueEntry:
    job: dict
    screening: object
    fit: FitResult
    priority: ApplicationPriority

    @property
    def score(self) -> int:
        return self.priority.application_priority_score


def evaluate_job(row: dict, resume_skills: set[str]) -> Optional[QueueEntry]:
    """Run the full Phase 1 + Phase 2 assessment for one stored job row."""
    title = analyze_title(row.get("title") or "")
    location = analyze_location(row.get("location") or "", row.get("country_focus") or "")
    jd = row.get("description") or ""
    experience = analyze_experience(jd)
    screening = score_job(title=title, location=location, experience=experience,
                          posted_at=_parse_posted(row.get("posted") or ""))

    jd_canonicals = ontology.extract_canonical_skills(jd) if jd else {}
    matched = {c for c in jd_canonicals if c in resume_skills}

    fit = analyze_fit(
        jd_text=jd,
        matched_canonicals=matched,
        role_family=title.role_family,
        experience_min=experience.min_years,
        matcher_score=row.get("resume_match") or 0,
    )
    priority = application_priority(
        screening_score=screening.score,
        screening_priority=screening.priority,
        fit=fit,
        role_family=title.role_family,
        location_class=location.classification,
        ghost_level="",
    )
    return QueueEntry(job=row, screening=screening, fit=fit, priority=priority)


def build_queue(db, *, limit: int = DEFAULT_LIMIT, resume_text: str = "",
                include_acted: bool = False, max_per_company: int = MAX_PER_COMPANY,
                enforce_age: bool = True) -> list[QueueEntry]:
    """Rank eligible, un-acted-on jobs by application priority.

    Applies the same job-age policy Phase 1 alerting uses. Without it the queue
    ranked the whole stored history, so 7 of the 15 default slots were filled
    with postings Phase 1 would never have alerted on — the application
    interface and the alert stream were describing different populations.

    The age rule is not re-implemented here: `_is_too_old` and
    `MAX_JOB_AGE_DAYS` are imported from the orchestrator, so the queue tracks
    the configured policy automatically, including its treatment of undated
    postings. `enforce_age=False` is for shadow analysis only and must not be
    used for the actionable queue.
    """
    from ..resume_matcher import load_resume, _resume_path_from_env
    # Imported inside the function: src.main imports this package, so a
    # module-level import would be circular.
    from ..main import _is_too_old

    resume_text = resume_text or load_resume(_resume_path_from_env())
    resume_skills = set(ontology.extract_canonical_skills(resume_text)) if resume_text else set()

    q = ApplicationQueue(db)
    excluded = set() if include_acted else q.excluded_keys()

    rows = db._conn.execute(
        "SELECT key, company, title, location, url, posted, description, "
        "       resume_match, priority, opportunity_score, role_family, location_class "
        "FROM jobs WHERE priority IN ('APPLY_NOW','STRONG','REVIEW') OR priority = '' "
        "ORDER BY last_seen DESC LIMIT 4000"
    ).fetchall()

    entries: list[QueueEntry] = []
    for r in rows:
        row = dict(r)
        if row["key"] in excluded:
            continue
        # Same eligibility gate as Phase 1 alerting.
        if enforce_age and _is_too_old(row.get("posted") or ""):
            continue
        e = evaluate_job(row, resume_skills)
        if e and e.priority.is_actionable:
            entries.append(e)

    entries.sort(key=lambda e: (-e.score, -(e.job.get("resume_match") or 0)))

    if max_per_company:
        seen: dict[str, int] = {}
        capped = []
        for e in entries:
            c = (e.job.get("company") or "").lower()
            if seen.get(c, 0) >= max_per_company:
                continue
            seen[c] = seen.get(c, 0) + 1
            capped.append(e)
        entries = capped

    return entries[:limit]


def render_queue(entries: list[QueueEntry], *, show_url: bool = True) -> str:
    """Plain-text daily queue, short enough to act on."""
    if not entries:
        return ("\nTODAY'S APPLICATION QUEUE\n\n"
                "  Nothing actionable right now — either everything eligible has been\n"
                "  acted on, or no new jobs cleared screening.\n")

    out = ["", "TODAY'S APPLICATION QUEUE", ""]
    for i, e in enumerate(entries, 1):
        j, f, p = e.job, e.fit, e.priority
        out.append(f"{i:>2}. {p.application_priority_score} — {j['title']} — {j['company']}")
        out.append(f"    {p.priority}   {_age_str(j.get('posted') or '')}   {j.get('location') or '?'}")
        out.append(f"    screening {p.screening_score} · resume fit {p.resume_fit_score} "
                   f"· priority {p.application_priority_score}")
        good = list(f.matched_required_skills[:4]) + list(f.matched_preferred_skills[:2])
        if f.experience_fit == "ideal":
            good.insert(0, "experience in range")
        if good:
            out.append("    Why:")
            out.extend(f"      + {g}" for g in good[:5])
        gaps = list(f.missing_required_skills[:2]) + [f"{s} (preferred)" for s in f.missing_preferred_skills[:2]]
        if gaps:
            out.append("    Gaps:")
            out.extend(f"      ~ {g}" for g in gaps[:4])
        for w in p.warnings[:2]:
            if not w.startswith("Missing"):
                out.append(f"      ! {w}")
        if show_url and j.get("url"):
            out.append(f"    {j['url']}")
        out.append("")
    out.append(f"  {len(entries)} job(s). Mark one:  python3 -m src.apply applied <job_key>")
    out.append("")
    return "\n".join(out)
