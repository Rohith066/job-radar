"""The Muse source adapter — free public API, US-friendly jobs.

API: https://www.themuse.com/api/public/jobs?category=<cat>&level=<lvl>&page=<n>
No API key required. Returns full JD (contents) + locations + levels.
Filters to data/engineering categories at entry/mid levels.
"""
from __future__ import annotations

import logging
import re

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_ENDPOINT = "https://www.themuse.com/api/public/jobs"

_CATEGORIES = [
    "Data and Analytics",
    "Data Science",
    "Engineering",
    "Software Engineering",
]
# Only junior-friendly levels (matches Rohith's ~3 yrs experience)
_LEVELS = ("Entry Level", "Mid Level", "Internship")
_OK_LEVELS = {"entry level", "mid level"}


def _strip_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z#0-9]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


class TheMuseSource(BaseSource):
    name = "themuse"

    def __init__(self, max_jobs: int = 150) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("themuse")
        headers = {"accept": "application/json", "user-agent": "job-radar/1.0"}

        seen_ids: set[str] = set()
        result: list[Job] = []

        for category in _CATEGORIES:
            if len(result) >= self.max_jobs:
                break
            for page in range(1, 4):   # 3 pages per category (~60 jobs)
                if len(result) >= self.max_jobs:
                    break
                try:
                    r = sess.get(
                        _ENDPOINT,
                        params={"category": category, "level": list(_LEVELS),
                                "page": page, "location": "United States"},
                        headers=headers, timeout=timeout,
                    )
                    r.raise_for_status()
                    results = r.json().get("results", [])
                except Exception as exc:
                    log.debug("themuse %s p%d failed: %s", category, page, exc)
                    break

                if not results:
                    break

                for raw in results:
                    jid = str(raw.get("id", ""))
                    if not jid or jid in seen_ids:
                        continue
                    seen_ids.add(jid)

                    # Level gate — skip senior even if category matched
                    levels = {lvl.get("short_name", lvl.get("name", "")).lower()
                              for lvl in (raw.get("levels") or [])}
                    level_names = {lvl.get("name", "").lower() for lvl in (raw.get("levels") or [])}
                    if level_names and not (level_names & _OK_LEVELS):
                        continue

                    import html as _html
                    title = _html.unescape(str(raw.get("name", "Unknown Title")).strip())
                    cr = classify(title)
                    if cr.label == "no":
                        continue

                    locs = [l.get("name", "") for l in (raw.get("locations") or [])]
                    location = "; ".join([l for l in locs if l]) or "United States"
                    company = (raw.get("company") or {}).get("name", "")
                    url = (raw.get("refs") or {}).get("landing_page", "")

                    result.append(Job(
                        key=f"themuse:{jid}",
                        source=self.name,
                        company=str(company).strip(),
                        title=title,
                        location=location,
                        url=str(url),
                        posted=str(raw.get("publication_date", "")),
                        score=cr.score, label=cr.label,
                        description=_strip_html(raw.get("contents", "")),
                    ))

        log.info("themuse: fetched %d positions", len(result))
        return result[: self.max_jobs]
