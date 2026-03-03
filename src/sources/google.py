"""Google careers source adapter."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_ENDPOINT = "https://careers.google.com/api/v3/search/"
_HEADERS = {
    "accept": "application/json",
    "referer": "https://careers.google.com/jobs/results/",
    "user-agent": "Mozilla/5.0",
}
_BASE_PARAMS = {
    "q": "data analyst OR data scientist OR data engineer OR analytics engineer OR business intelligence",
    "location": "United States",
    "jlo": "en_US",
    "page": 1,
    "sort_by": "date",
}


def _key(job: dict) -> str:
    job_id = str(job.get("id") or job.get("job_id") or "")
    return f"google:{job_id}" if job_id else f"google:url:{job.get('apply_url','')}"


def _normalize(job: dict) -> dict:
    title = job.get("title") or "Unknown Title"
    locs = job.get("locations") or []
    if isinstance(locs, list) and locs:
        first = locs[0] if isinstance(locs[0], dict) else {}
        loc = first.get("display") or first.get("city") or "United States"
    else:
        loc = "United States"
    apply_url = job.get("apply_url") or job.get("url") or "https://careers.google.com/jobs/"
    posted = job.get("date") or ""
    return {"key": _key(job), "title": str(title), "location": str(loc), "posted": str(posted), "url": apply_url}


class GoogleSource(BaseSource):
    name = "google"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        all_raw: list[dict] = []
        page = 1

        while True:
            params = dict(_BASE_PARAMS)
            params["page"] = page
            try:
                r = sess.get(_ENDPOINT, params=params, headers=_HEADERS, timeout=timeout)
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.warning("google: page %d failed — %s", page, exc)
                break

            jobs = data.get("jobs") or []
            if not jobs:
                break

            all_raw.extend(jobs)
            if len(all_raw) >= self.max_jobs:
                all_raw = all_raw[:self.max_jobs]
                break

            if not data.get("next_page"):
                break
            page += 1

        result: list[Job] = []
        for raw in all_raw:
            n = _normalize(raw)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="Google",
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=cr.score, label=cr.label,
            ))

        log.info("google: fetched %d positions", len(result))
        return result
