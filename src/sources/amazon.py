"""Amazon jobs source adapter."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_ENDPOINT = "https://www.amazon.jobs/en/search.json"
_BASE_PARAMS: dict = {
    "category[]": [
        "machine-learning-science",
        "business-intelligence",
        "data-science",
        "software-development",   # data engineers at Amazon often post here
    ],
    "schedule_type_id[]": ["Full-Time"],
    "normalized_country_code[]": ["USA"],
    "radius": "100000km",
    "offset": 0,
    "result_limit": 50,
    "sort": "recent",
    "latitude": 38.89036,
    "longitude": -77.03196,
    "loc_query": "united states",
    "base_query": "",
}


def _key(job: dict) -> str:
    job_id = (
        job.get("id")
        or job.get("job_id")
        or job.get("jobId")
        or job.get("id_icims")
        or job.get("icims_id")
        or job.get("requisition_id")
        or ""
    )
    job_id = str(job_id)
    if job_id:
        return f"amazon:{job_id}"
    url = job.get("url") or job.get("job_path") or ""
    return f"amazon:url:{url}"


def _normalize(job: dict) -> dict:
    title = job.get("title") or job.get("job_title") or job.get("name") or "Unknown Title"
    loc = (
        job.get("location")
        or job.get("normalized_location")
        or job.get("city")
        or job.get("primary_location")
        or "Unknown Location"
    )
    posted = job.get("posted_date") or job.get("postedDate") or job.get("posted") or ""
    url = job.get("url") or job.get("job_path") or job.get("jobPath") or ""
    if isinstance(url, str) and url.startswith("/"):
        url = "https://www.amazon.jobs" + url
    if not url:
        url = "https://www.amazon.jobs/en/search"
    return {"key": _key(job), "title": str(title), "location": str(loc), "posted": str(posted), "url": str(url)}


class AmazonSource(BaseSource):
    name = "amazon"

    def __init__(self, max_jobs: int = 300) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        headers = {"accept": "application/json", "user-agent": "Mozilla/5.0"}

        all_raw: list[dict] = []
        offset = 0
        safety_cap = 5000

        while True:
            params = dict(_BASE_PARAMS)
            params["offset"] = offset

            r = sess.get(_ENDPOINT, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            data = r.json()

            jobs = data.get("jobs") or data.get("results") or data.get("job_results") or []
            if not jobs:
                break

            all_raw.extend(jobs)
            if len(all_raw) >= self.max_jobs:
                all_raw = all_raw[:self.max_jobs]
                break

            if offset > 0:
                page_keys = {_key(j) for j in jobs}
                if page_keys and page_keys.issubset(seen_keys):
                    break

            offset += len(jobs)
            if offset >= safety_cap:
                break

        result: list[Job] = []
        for raw in all_raw:
            n = _normalize(raw)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="Amazon",
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=cr.score, label=cr.label,
            ))

        log.info("amazon: fetched %d positions", len(result))
        return result
