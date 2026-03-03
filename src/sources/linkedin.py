"""LinkedIn Jobs source adapter using the public guest search API.

⚠️  IMPORTANT LIMITATIONS:
    - LinkedIn aggressively rate-limits automated requests.
    - GitHub Actions cloud IPs are frequently blocked (429 / CAPTCHA).
    - This source works best when run locally on your own machine.
    - If you see consistent 429/403 errors in GitHub Actions, disable this
      source in config.yaml (linkedin: enabled: false) and run it manually.
    - Max 1 page (25 jobs) by default to stay under rate limits.
"""
from __future__ import annotations

import logging
import re
import time

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.linkedin.com/jobs/search/",
}

# LinkedIn geoId for "United States"
_GEO_US = "103644278"

# Search queries to run — LinkedIn searches one keyword set at a time
_QUERIES = [
    "data analyst",
    "data scientist",
    "data engineer",
    "analytics engineer",
    "business intelligence analyst",
    "machine learning engineer",
]

# Delay between pages to avoid rate limits (seconds)
_PAGE_DELAY = 3.0


def _parse_cards(html: str) -> list[dict]:
    """Parse LinkedIn job card HTML and return list of raw job dicts."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.error("linkedin: beautifulsoup4 not installed. Run: pip install beautifulsoup4")
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("div", class_="base-card")
    results = []

    for card in cards:
        try:
            # Extract job ID from data-entity-urn="urn:li:jobPosting:1234567890"
            urn = card.get("data-entity-urn", "")
            job_id = ""
            m = re.search(r"jobPosting:(\d+)", urn)
            if m:
                job_id = m.group(1)

            title_el = card.find(class_="base-search-card__title")
            company_el = card.find(class_="base-search-card__subtitle")
            location_el = card.find(class_="job-search-card__location")
            date_el = card.find("time")

            title = title_el.get_text(strip=True) if title_el else ""
            company = company_el.get_text(strip=True) if company_el else "Unknown"
            location = location_el.get_text(strip=True) if location_el else "United States"
            posted = date_el.get("datetime", "") if date_el else ""

            if not title or not job_id:
                continue

            results.append({
                "job_id": job_id,
                "title": title,
                "company": company,
                "location": location,
                "posted": posted,
                "url": f"https://www.linkedin.com/jobs/view/{job_id}",
            })
        except Exception:
            continue

    return results


class LinkedInSource(BaseSource):
    name = "linkedin"

    def __init__(self, max_jobs: int = 100) -> None:
        # Default low cap — LinkedIn blocks aggressively
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        all_raw: list[dict] = []
        seen_ids: set[str] = set()

        for query in _QUERIES:
            if len(all_raw) >= self.max_jobs:
                break

            params = {
                "keywords": query,
                "location": "United States",
                "geoId": _GEO_US,
                "f_TPR": "r86400",   # posted in last 24 hours
                "start": 0,
                "count": 25,
            }

            try:
                r = sess.get(_SEARCH_URL, headers=_HEADERS, params=params, timeout=timeout)

                if r.status_code == 429:
                    log.warning("linkedin: rate limited (429) — skipping remaining queries")
                    break
                if r.status_code in (403, 999):
                    log.warning("linkedin: blocked (HTTP %d) — LinkedIn may be blocking cloud IPs", r.status_code)
                    break

                r.raise_for_status()
                cards = _parse_cards(r.text)

                for card in cards:
                    jid = card["job_id"]
                    if jid not in seen_ids:
                        seen_ids.add(jid)
                        all_raw.append(card)

                log.debug("linkedin: query=%r fetched %d cards", query, len(cards))

            except Exception as exc:
                log.warning("linkedin: query=%r failed — %s", query, exc)

            # Polite delay between queries
            time.sleep(_PAGE_DELAY)

        if len(all_raw) > self.max_jobs:
            all_raw = all_raw[: self.max_jobs]

        result: list[Job] = []
        for raw in all_raw:
            key = f"linkedin:{raw['job_id']}"
            cr = classify(raw["title"])
            result.append(
                Job(
                    key=key,
                    source=self.name,
                    company=raw["company"],
                    title=raw["title"],
                    location=raw["location"],
                    url=raw["url"],
                    posted=raw["posted"],
                    score=cr.score,
                    label=cr.label,
                )
            )

        log.info("linkedin: fetched %d positions", len(result))
        return result
