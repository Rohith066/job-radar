"""Apple careers source adapter."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_ENDPOINT = "https://jobs.apple.com/api/role/search"
_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://jobs.apple.com",
    "referer": "https://jobs.apple.com/en-us/search",
    "user-agent": "Mozilla/5.0",
}
_PAYLOAD = {
    "query": "data",
    "filters": {
        "range": {
            "standardWeeklyHours": {"start": None, "end": None}
        },
        "location": ["country-USA"]
    },
    "page": 1,
    "locale": "en-us",
    "sort": "newest",
}


def _key(job: dict) -> str:
    job_id = str(job.get("positionId") or job.get("id") or "")
    return f"apple:{job_id}" if job_id else f"apple:url:{job.get('url','')}"


def _normalize(job: dict) -> dict:
    title = job.get("postingTitle") or job.get("title") or "Unknown Title"
    locs = job.get("locations") or []
    if isinstance(locs, list) and locs:
        first = locs[0] if isinstance(locs[0], dict) else {}
        loc = first.get("name") or "United States"
    else:
        loc = "United States"
    posted = job.get("postDateInGMT") or job.get("postDate") or ""
    job_id = str(job.get("positionId") or "")
    url = f"https://jobs.apple.com/en-us/details/{job_id}" if job_id else "https://jobs.apple.com/en-us/search"
    return {"key": _key(job), "title": str(title), "location": str(loc), "posted": str(posted), "url": url}


class AppleSource(BaseSource):
    name = "apple"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        all_raw: list[dict] = []
        page = 1

        while True:
            payload = dict(_PAYLOAD)
            payload["page"] = page
            try:
                r = sess.post(_ENDPOINT, headers=_HEADERS, json=payload, timeout=timeout)
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.warning("apple: page %d failed — %s", page, exc)
                break

            jobs = data.get("searchResults") or data.get("results") or []
            if not jobs:
                break

            all_raw.extend(jobs)
            if len(all_raw) >= self.max_jobs:
                all_raw = all_raw[:self.max_jobs]
                break

            total = data.get("totalRecords") or 0
            if len(all_raw) >= total:
                break
            page += 1

        result: list[Job] = []
        for raw in all_raw:
            n = _normalize(raw)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="Apple",
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=cr.score, label=cr.label,
            ))

        log.info("apple: fetched %d positions", len(result))
        return result
