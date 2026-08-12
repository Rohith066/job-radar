"""Remotive source adapter — free public API, remote jobs.

API: https://remotive.com/api/remote-jobs?search=<query>
No API key required. Returns full JD descriptions + salary, which feed
directly into resume matching. All jobs are remote.
"""
from __future__ import annotations

import logging
import re

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_ENDPOINT = "https://remotive.com/api/remote-jobs"

# Search terms covering both tracks (DE + AI)
_SEARCHES = [
    "data engineer",
    "analytics engineer",
    "ai engineer",
    "machine learning engineer",
    "data analyst",
]

# Locations Remotive uses that are US-compatible for a US-based candidate
_US_OK = ("usa", "united states", "u.s", "worldwide", "anywhere", "north america", "americas")


def _strip_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _location_for(required: str) -> str:
    """Map Remotive's candidate_required_location to a location string our
    US filter understands. US-compatible regions → 'Remote, USA'; otherwise
    keep the region so the non-US filter drops it."""
    r = (required or "").lower()
    if not r or any(tok in r for tok in _US_OK):
        return "Remote, USA"
    return f"Remote ({required})"   # e.g. 'Remote (Europe)' → dropped downstream


class RemotiveSource(BaseSource):
    name = "remotive"

    def __init__(self, max_jobs: int = 150) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("remotive")
        headers = {"accept": "application/json", "user-agent": "job-radar/1.0"}

        seen_ids: set[str] = set()
        result: list[Job] = []

        for query in _SEARCHES:
            if len(result) >= self.max_jobs:
                break
            try:
                r = sess.get(
                    _ENDPOINT,
                    params={"search": query, "limit": 50},
                    headers=headers, timeout=timeout,
                )
                r.raise_for_status()
                jobs = r.json().get("jobs", [])
            except Exception as exc:
                log.debug("remotive query %r failed: %s", query, exc)
                continue

            for raw in jobs:
                jid = str(raw.get("id", ""))
                if not jid or jid in seen_ids:
                    continue
                seen_ids.add(jid)

                import html as _html
                title = _html.unescape(str(raw.get("title", "Unknown Title")).strip())
                cr = classify(title)
                if cr.label == "no":
                    continue

                result.append(Job(
                    key=f"remotive:{jid}",
                    source=self.name,
                    company=str(raw.get("company_name", "")).strip(),
                    title=title,
                    location=_location_for(raw.get("candidate_required_location", "")),
                    url=str(raw.get("url", "")),
                    posted=str(raw.get("publication_date", "")),
                    score=cr.score, label=cr.label,
                    salary=str(raw.get("salary", "") or "").strip(),
                    work_type="Remote",
                    description=_strip_html(raw.get("description", "")),
                ))

        log.info("remotive: fetched %d positions", len(result))
        return result[: self.max_jobs]
