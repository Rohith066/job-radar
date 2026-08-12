"""Lever ATS board source adapter."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_API_BASE = "https://jobs.lever.co/v0/postings"


def _slug(board_url: str) -> str:
    parts = [p for p in (urlparse(board_url or "").path or "").split("/") if p]
    return parts[0] if parts else ""


def _board_id(board_url: str) -> str:
    slug = _slug(board_url)
    return f"lever:{slug}" if slug else "lever:"


class LeverSource(BaseSource):
    """Fetches jobs from a single Lever board."""

    def __init__(self, company: str, board_url: str) -> None:
        self._slug_val = _slug(board_url)
        self.name = f"lever:{self._slug_val}"
        self.company = company
        self.board_url = board_url
        self.board_id = _board_id(board_url)

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        if not self._slug_val:
            return []

        sess = get_session("lever")
        url = f"{_API_BASE}/{self._slug_val}"
        r = sess.get(url, params={"mode": "json"}, timeout=timeout)
        r.raise_for_status()

        raw_jobs = r.json()
        if not isinstance(raw_jobs, list):
            return []

        result: list[Job] = []
        for raw in raw_jobs:
            job_id = str(raw.get("id") or "")
            key = (
                f"lever:{self._slug_val}:{job_id}" if job_id
                else f"lever:{self._slug_val}:url:{raw.get('hostedUrl','')}"
            )
            title = raw.get("text") or raw.get("title") or "Unknown Title"
            cats = raw.get("categories") or {}
            loc = str(cats.get("location", "Unknown Location")) if isinstance(cats, dict) else "Unknown Location"

            # Lever returns createdAt as Unix ms timestamp
            posted_raw = raw.get("createdAt") or ""
            posted = ""
            if isinstance(posted_raw, (int, float)) and posted_raw:
                posted = datetime.fromtimestamp(float(posted_raw) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            elif isinstance(posted_raw, str):
                posted = posted_raw

            url_job = raw.get("hostedUrl") or raw.get("applyUrl") or self.board_url
            # Lever returns plain-text description in 'descriptionPlain' or HTML in 'description'
            desc_plain = raw.get("descriptionPlain") or ""
            desc_html  = raw.get("description") or ""
            extra_plain = raw.get("additionalPlain") or ""
            if desc_plain:
                description = f"{desc_plain}\n{extra_plain}".strip()
            elif desc_html:
                description = re.sub(r"<[^>]+>", " ", desc_html).strip()
            else:
                description = ""
            cr = classify(title)
            result.append(Job(
                key=key, source="lever", company=self.company,
                title=title, location=loc, url=url_job,
                posted=posted, score=cr.score, label=cr.label,
                description=description,
            ))

        log.debug("lever:%s: fetched %d jobs", self._slug_val, len(result))
        return result
