"""Stripe careers source adapter (via Greenhouse board API)."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

# Stripe's public Greenhouse job board API (no auth required)
_ENDPOINT = "https://boards-api.greenhouse.io/v1/boards/stripe/jobs"
_HEADERS = {
    "accept": "application/json",
    "user-agent": "Mozilla/5.0",
    "referer": "https://stripe.com/jobs",
}


def _key(job: dict) -> str:
    job_id = str(job.get("id") or "")
    return f"stripe:{job_id}" if job_id else f"stripe:url:{job.get('absolute_url', '')}"


def _normalize(job: dict) -> dict:
    title = job.get("title") or "Unknown Title"

    loc_raw = job.get("location") or {}
    if isinstance(loc_raw, dict):
        loc = loc_raw.get("name") or "United States"
    else:
        loc = str(loc_raw) if loc_raw else "United States"

    posted = job.get("updated_at") or ""
    url = job.get("absolute_url") or "https://stripe.com/jobs"

    return {
        "key": _key(job),
        "title": str(title),
        "location": str(loc),
        "posted": str(posted),
        "url": url,
    }


class StripeSource(BaseSource):
    name = "stripe"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        try:
            r = sess.get(_ENDPOINT, headers=_HEADERS, timeout=timeout)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("stripe: fetch failed — %s", exc)
            return []

        all_raw: list[dict] = data.get("jobs") or []
        if len(all_raw) > self.max_jobs:
            all_raw = all_raw[: self.max_jobs]

        result: list[Job] = []
        for raw in all_raw:
            n = _normalize(raw)
            cr = classify(n["title"])
            result.append(
                Job(
                    key=n["key"],
                    source=self.name,
                    company="Stripe",
                    title=n["title"],
                    location=n["location"],
                    url=n["url"],
                    posted=n["posted"],
                    score=cr.score,
                    label=cr.label,
                )
            )

        log.info("stripe: fetched %d positions", len(result))
        return result
