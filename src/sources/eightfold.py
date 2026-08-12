"""Eightfold ATS source adapter — covers Microsoft and NVIDIA."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source definitions
# ---------------------------------------------------------------------------
_SOURCES: dict[str, dict[str, Any]] = {
    "microsoft": {
        "company": "Microsoft",
        "endpoint": "https://apply.careers.microsoft.com/api/pcsx/search",
        "base_url": "https://apply.careers.microsoft.com",
        "default_url": "https://apply.careers.microsoft.com/careers",
        "params": {
            "domain": "microsoft.com",
            "query": "",
            "location": "United States, Multiple Locations, Multiple Locations",
            "start": 0,
            "sort_by": "timestamp",
            "filter_include_remote": 1,
            "filter_seniority": ["Entry", "Mid-Level"],
        },
    },
    "nvidia": {
        "company": "NVIDIA",
        "endpoint": "https://nvidia.eightfold.ai/api/pcsx/search",
        "base_url": "https://nvidia.eightfold.ai",
        "default_url": "https://nvidia.eightfold.ai/careers",
        "params": {
            "domain": "nvidia.com",
            "query": "",
            "location": "united states",
            "start": 0,
            "sort_by": "timestamp",
            "filter_include_remote": 1,
            "filter_job_category": "engineering",
            "filter_job_type": "regular employee",
            "filter_time_type": "full time",
            "filter_hiring_title": [
                "Data Analyst",
                "Data Scientist",
                "Data Engineer",
                "Analytics Engineer",
                "Business Intelligence",
                "Machine Learning Engineer",
                "Applied Scientist",
                "Artificial Intelligence",
                "machine learning",
                "data",
            ],
        },
    },
}


def _key(source: str, pos: dict) -> str:
    job_id = str(pos.get("id", ""))
    if job_id:
        return f"{source}:{job_id}"
    url = pos.get("applyUrl") or pos.get("positionUrl") or ""
    return f"{source}:url:{url}"


def _normalize(source: str, pos: dict) -> dict:
    cfg = _SOURCES[source]
    title = pos.get("name") or pos.get("title") or "Unknown Title"

    if isinstance(pos.get("standardizedLocations"), list) and pos["standardizedLocations"]:
        loc = pos["standardizedLocations"][0]
    elif isinstance(pos.get("locations"), list) and pos["locations"]:
        loc = pos["locations"][0]
    else:
        loc = "Unknown Location"

    posted_ts = pos.get("postedTs")
    posted = ""
    if isinstance(posted_ts, (int, float)):
        posted = datetime.fromtimestamp(posted_ts, tz=timezone.utc).strftime("%Y-%m-%d")

    url = pos.get("positionUrl") or pos.get("applyUrl") or cfg["default_url"]
    if isinstance(url, str) and url.startswith("/"):
        url = cfg["base_url"] + url

    return {
        "key": _key(source, pos),
        "company": cfg["company"],
        "title": str(title),
        "location": str(loc),
        "posted": posted,
        "url": str(url),
    }


class EightfoldSource(BaseSource):
    """Single adapter for any Eightfold-based career site."""

    def __init__(self, source_name: str, max_jobs: int = 300) -> None:
        if source_name not in _SOURCES:
            raise ValueError(f"Unknown Eightfold source: {source_name}")
        self.name = source_name
        self.max_jobs = max_jobs
        self._cfg = _SOURCES[source_name]

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        params0 = self._cfg["params"]
        endpoint = self._cfg["endpoint"]
        headers = {"accept": "application/json", "user-agent": "Mozilla/5.0"}

        all_raw: list[dict] = []
        start = 0
        safety_cap = 5000

        while True:
            params = dict(params0)
            params["start"] = start

            r = sess.get(endpoint, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            data = r.json()

            positions = (data.get("data", {}) or {}).get("positions", []) or []
            if not positions:
                break

            all_raw.extend(positions)
            if len(all_raw) >= self.max_jobs:
                all_raw = all_raw[:self.max_jobs]
                break

            # Early exit if entire page is already seen
            if start > 0:
                page_keys = {_key(self.name, p) for p in positions}
                if page_keys and page_keys.issubset(seen_keys):
                    log.debug("%s: page fully seen at offset %d — stopping", self.name, start)
                    break

            start += len(positions)
            if start >= safety_cap:
                break

        jobs: list[Job] = []
        for pos in all_raw:
            n = _normalize(self.name, pos)
            result = classify(n["title"])
            jobs.append(Job(
                key=n["key"], source=self.name, company=n["company"],
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=result.score, label=result.label,
            ))

        log.info("%s: fetched %d positions", self.name, len(jobs))
        return jobs
