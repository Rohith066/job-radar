"""Greenhouse ATS board source adapter."""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_API_BASE = "https://boards-api.greenhouse.io/v1/boards"


def _slug(board_url: str) -> str:
    parts = [p for p in (urlparse(board_url or "").path or "").split("/") if p]
    return parts[0] if parts else ""


def _board_id(board_url: str) -> str:
    slug = _slug(board_url)
    return f"greenhouse:{slug}" if slug else "greenhouse:"


def _key(company_slug: str, job: dict) -> str:
    job_id = str(job.get("id") or "")
    if job_id:
        return f"greenhouse:{company_slug}:{job_id}"
    return f"greenhouse:{company_slug}:url:{job.get('absolute_url', '')}"


class GreenhouseSource(BaseSource):
    """Fetches jobs from a single Greenhouse board."""

    def __init__(self, company: str, board_url: str) -> None:
        self.name = f"greenhouse:{_slug(board_url)}"
        self.company = company
        self.board_url = board_url
        self._slug = _slug(board_url)
        self.board_id = _board_id(board_url)

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        if not self._slug:
            return []

        sess = get_session("greenhouse")
        url = f"{_API_BASE}/{self._slug}/jobs"
        r = sess.get(url, params={"content": "true"}, timeout=timeout)
        r.raise_for_status()

        raw_jobs = r.json().get("jobs") or []
        if not isinstance(raw_jobs, list):
            return []

        result: list[Job] = []
        for raw in raw_jobs:
            job_id = str(raw.get("id") or "")
            key = f"greenhouse:{self._slug}:{job_id}" if job_id else f"greenhouse:{self._slug}:url:{raw.get('absolute_url','')}"
            title = raw.get("title") or "Unknown Title"
            loc_obj = raw.get("location") or {}
            loc = str(loc_obj.get("name", "Unknown Location")) if isinstance(loc_obj, dict) else "Unknown Location"
            posted = raw.get("updated_at") or raw.get("created_at") or ""
            url_job = raw.get("absolute_url") or raw.get("url") or self.board_url
            # Greenhouse returns HTML content when fetched with ?content=true
            html = raw.get("content") or ""
            description = re.sub(r"<[^>]+>", " ", html).strip() if html else ""
            cr = classify(title)
            result.append(Job(
                key=key, source="greenhouse", company=self.company,
                title=title, location=loc, url=url_job,
                posted=posted, score=cr.score, label=cr.label,
                description=description,
            ))

        log.debug("greenhouse:%s: fetched %d jobs", self._slug, len(result))
        return result
