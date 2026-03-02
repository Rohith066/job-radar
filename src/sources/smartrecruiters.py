"""SmartRecruiters ATS board source adapter."""
from __future__ import annotations

import logging
from urllib.parse import urlparse

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_API_BASE = "https://api.smartrecruiters.com/v1/companies"


def _company_slug(board_url: str) -> str:
    parts = [p for p in (urlparse(board_url or "").path or "").split("/") if p]
    return parts[0].strip() if parts else ""


def _board_id(board_url: str) -> str:
    slug = _company_slug(board_url).lower()
    return f"smartrecruiters:{slug}" if slug else "smartrecruiters:"


class SmartRecruitersSource(BaseSource):
    """Fetches jobs from a single SmartRecruiters board."""

    def __init__(self, company: str, board_url: str) -> None:
        slug = _company_slug(board_url)
        self.name = f"smartrecruiters:{slug}"
        self.company = company
        self.board_url = board_url
        self._slug = slug
        self.board_id = _board_id(board_url)

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        if not self._slug:
            return []

        sess = get_session("smartrecruiters")
        headers = {"accept": "application/json", "user-agent": "Mozilla/5.0"}

        all_raw: list[dict] = []
        offset = 0
        limit = 100
        safety_cap = 5000

        while True:
            url = f"{_API_BASE}/{self._slug}/postings"
            r = sess.get(url, params={"offset": offset, "limit": limit}, headers=headers, timeout=timeout)
            r.raise_for_status()

            data = r.json() if r.content else {}
            posts = data.get("content") or data.get("postings") or []
            if not isinstance(posts, list) or not posts:
                break

            all_raw.extend(posts)
            if len(all_raw) >= 500:
                break
            offset += len(posts)
            if offset >= safety_cap:
                break

        result: list[Job] = []
        for raw in all_raw:
            pid = str(raw.get("id") or raw.get("ref") or "")
            key = (
                f"smartrecruiters:{self._slug}:{pid}" if pid
                else f"smartrecruiters:{self._slug}:url:{raw.get('referrer','')}"
            )
            title = raw.get("name") or raw.get("jobTitle") or "Unknown Title"
            loc_obj = raw.get("location") or {}
            if isinstance(loc_obj, dict):
                loc = make_location([loc_obj.get("city"), loc_obj.get("region") or loc_obj.get("state"), loc_obj.get("country")])
            else:
                loc = str(loc_obj) if loc_obj else "Unknown Location"
            posted = raw.get("releasedDate") or raw.get("publicationDate") or raw.get("createdOn") or ""
            url_job = raw.get("referrer") or raw.get("applyUrl") or raw.get("url") or self.board_url
            cr = classify(title)
            result.append(Job(
                key=key, source="smartrecruiters", company=self.company,
                title=title, location=loc, url=url_job,
                posted=posted, score=cr.score, label=cr.label,
            ))

        log.debug("smartrecruiters:%s: fetched %d jobs", self._slug, len(result))
        return result
