"""Meta (Facebook) careers source adapter."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_ENDPOINT = "https://www.metacareers.com/graphql"
_HEADERS = {
    "accept": "application/json",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://www.metacareers.com",
    "referer": "https://www.metacareers.com/jobs",
    "user-agent": "Mozilla/5.0",
}


def _key(job: dict) -> str:
    job_id = str(job.get("id") or "")
    return f"meta:{job_id}" if job_id else f"meta:url:{job.get('url','')}"


def _normalize(job: dict) -> dict:
    title = job.get("title") or "Unknown Title"
    locations = job.get("locations") or []
    if isinstance(locations, list) and locations:
        loc = locations[0] if isinstance(locations[0], str) else str(locations[0])
    else:
        loc = "United States"
    posted = job.get("post_date") or job.get("updated_time") or ""
    job_id = str(job.get("id") or "")
    url = f"https://www.metacareers.com/jobs/{job_id}" if job_id else "https://www.metacareers.com/jobs"
    return {"key": _key(job), "title": str(title), "location": str(loc), "posted": str(posted), "url": url}


class MetaSource(BaseSource):
    name = "meta"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")

        # Meta uses a GraphQL-over-form-POST endpoint
        data = (
            "variables=%7B%22search_input%22%3A%7B%22q%22%3A%22data%22%2C"
            "%22divisions%22%3A%5B%5D%2C%22offices%22%3A%5B%5D%2C"
            "%22roles%22%3A%5B%5D%2C%22leadership_levels%22%3A%5B%5D%2C"
            "%22results_per_page%22%3A25%2C%22page%22%3A0%2C"
            "%22sort_by_new%22%3Atrue%2C%22is_leadership%22%3Afalse%2C"
            "%22location%22%3A%22united+states%22%7D%7D"
            "&doc_id=7613920925373426"
        )

        try:
            r = sess.post(_ENDPOINT, headers=_HEADERS, data=data, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            jobs = (
                ((payload.get("data") or {}).get("job_search") or {}).get("jobs") or []
            )
        except Exception as exc:
            log.warning("meta: fetch failed — %s", exc)
            return []

        if self.max_jobs and len(jobs) > self.max_jobs:
            jobs = jobs[:self.max_jobs]

        result: list[Job] = []
        for raw in jobs:
            n = _normalize(raw)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="Meta",
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=cr.score, label=cr.label,
            ))

        log.info("meta: fetched %d positions", len(result))
        return result
