"""IBM Elasticsearch-based jobs source adapter."""
from __future__ import annotations

import logging
from typing import Any

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_ENDPOINT = "https://www-api.ibm.com/search/api/v2"
_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://www.ibm.com",
    "referer": "https://www.ibm.com/",
    "user-agent": "Mozilla/5.0",
}
_PAYLOAD: dict[str, Any] = {
    "appId": "careers",
    "scopes": ["careers2"],
    "query": {"bool": {"must": []}},
    "post_filter": {
        "bool": {
            "must": [
                {
                    "bool": {
                        "should": [
                            {"term": {"field_keyword_08": "Data & Analytics"}},
                            {"term": {"field_keyword_08": "Consulting"}},
                        ]
                    }
                },
                {"term": {"field_keyword_18": "Entry Level"}},
                {"term": {"field_keyword_05": "United States"}},
            ]
        }
    },
    "size": 30,
    "sort": [{"dcdate": "desc"}, {"_score": "desc"}],
    "lang": "zz",
    "localeSelector": {},
    "sm": {"query": "", "lang": "zz"},
    "_source": [
        "_id", "title", "url", "field_keyword_17", "field_keyword_08", "field_keyword_18",
    ],
}


def _key(hit: dict) -> str:
    _id = str(hit.get("_id") or hit.get("id") or "")
    if _id:
        return f"ibm:{_id}"
    url = (hit.get("_source") or {}).get("url") or hit.get("url") or ""
    return f"ibm:url:{url}"


def _normalize(hit: dict) -> dict:
    src = hit.get("_source") if isinstance(hit.get("_source"), dict) else hit
    title = src.get("title") or "Unknown Title"
    url = src.get("url") or ""
    if url and isinstance(url, str) and url.startswith("/"):
        url = "https://www.ibm.com" + url
    if not url:
        url = "https://www.ibm.com/careers/search"
    posted = src.get("dcdate") or ""
    loc_raw = src.get("field_keyword_17")
    if isinstance(loc_raw, list) and loc_raw:
        loc = str(loc_raw[0])
    elif isinstance(loc_raw, str) and loc_raw:
        loc = loc_raw
    else:
        loc = "United States"
    return {"key": _key(hit), "title": str(title), "location": loc, "posted": str(posted), "url": str(url)}


class IBMSource(BaseSource):
    name = "ibm"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        payload = dict(_PAYLOAD)
        r = sess.post(_ENDPOINT, headers=_HEADERS, json=payload, timeout=timeout)

        # IBM occasionally rejects the `aggs` field — retry without it
        if r.status_code == 400 and "aggs" in (r.text or ""):
            payload.pop("aggs", None)
            r = sess.post(_ENDPOINT, headers=_HEADERS, json=payload, timeout=timeout)

        r.raise_for_status()
        data = r.json()

        hits = data.get("results") if isinstance(data.get("results"), list) else (
            (data.get("hits") or {}).get("hits") or []
        )
        if self.max_jobs and len(hits) > self.max_jobs:
            hits = hits[:self.max_jobs]

        result: list[Job] = []
        for hit in hits:
            n = _normalize(hit)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="IBM",
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=cr.score, label=cr.label,
            ))

        log.info("ibm: fetched %d positions", len(result))
        return result
