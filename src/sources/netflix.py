"""Netflix careers source adapter."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_ENDPOINT = "https://jobs.netflix.com/api/search"
_HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0",
    "referer": "https://jobs.netflix.com/search",
}
_PAGE_SIZE = 100


def _key(job: dict) -> str:
    job_id = str(job.get("id") or "")
    return f"netflix:{job_id}" if job_id else f"netflix:url:{job.get('external_link', '')}"


def _normalize(job: dict) -> dict:
    title = job.get("text") or job.get("title") or "Unknown Title"

    loc_raw = job.get("location") or {}
    if isinstance(loc_raw, dict):
        loc = loc_raw.get("name") or "United States"
    elif isinstance(loc_raw, list) and loc_raw:
        first = loc_raw[0]
        loc = first if isinstance(first, str) else (first.get("name") if isinstance(first, dict) else "United States")
    else:
        loc = str(loc_raw) if loc_raw else "United States"

    posted = job.get("updated_at") or job.get("created_at") or ""
    job_id = str(job.get("id") or "")
    external = job.get("external_link") or ""
    url = external if external else (f"https://jobs.netflix.com/jobs/{job_id}" if job_id else "https://jobs.netflix.com/search")

    return {
        "key": _key(job),
        "title": str(title),
        "location": str(loc),
        "posted": str(posted),
        "url": url,
    }


class NetflixSource(BaseSource):
    name = "netflix"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        all_raw: list[dict] = []
        page = 0

        while True:
            params = {
                "q": "data",
                "location": "United States",
                "limit": _PAGE_SIZE,
                "skip": page * _PAGE_SIZE,
            }
            try:
                r = sess.get(_ENDPOINT, headers=_HEADERS, params=params, timeout=timeout)
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.warning("netflix: page %d failed — %s", page, exc)
                break

            # Response shape: {"count": N, "records": {"postings": [...]}}
            records = data.get("records") or {}
            jobs = (
                records.get("postings")
                or data.get("jobs")
                or data.get("results")
                or []
            )
            if not jobs:
                break

            all_raw.extend(jobs)
            if len(all_raw) >= self.max_jobs:
                all_raw = all_raw[: self.max_jobs]
                break

            total = data.get("count") or records.get("count") or 0
            if total == 0 or len(all_raw) >= total:
                break
            page += 1

        result: list[Job] = []
        for raw in all_raw:
            n = _normalize(raw)
            cr = classify(n["title"])
            result.append(
                Job(
                    key=n["key"],
                    source=self.name,
                    company="Netflix",
                    title=n["title"],
                    location=n["location"],
                    url=n["url"],
                    posted=n["posted"],
                    score=cr.score,
                    label=cr.label,
                )
            )

        log.info("netflix: fetched %d positions", len(result))
        return result
