"""Goldman Sachs GraphQL source adapter."""
from __future__ import annotations

import logging
from typing import Any

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_ENDPOINT = "https://api-higher.gs.com/gateway/api/v1/graphql"
_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://higher.gs.com",
    "referer": "https://higher.gs.com/",
    "user-agent": "Mozilla/5.0",
}
_QUERY = """
query GetRoles($searchQueryInput: RoleSearchQueryInput!) {
  roleSearch(searchQueryInput: $searchQueryInput) {
    totalCount
    items {
      roleId corporateTitle jobTitle jobFunction
      locations { primary state country city }
      externalSource { sourceId }
    }
  }
}"""

_PAYLOAD: dict[str, Any] = {
    "operationName": "GetRoles",
    "variables": {
        "searchQueryInput": {
            "page": {"pageSize": 20, "pageNumber": 0},
            "sort": {"sortStrategy": "POSTED_DATE", "sortOrder": "DESC"},
            "filters": [
                {
                    "filterCategoryType": "LOCATION",
                    "filters": [
                        {
                            "filter": "United States",
                            "subFilters": [
                                {"filter": "New York", "subFilters": [{"filter": "New York", "subFilters": []}]},
                                {"filter": "California", "subFilters": [{"filter": "San Francisco", "subFilters": []}]},
                                {"filter": "Massachusetts", "subFilters": [{"filter": "Boston", "subFilters": []}]},
                                {"filter": "Texas", "subFilters": [{"filter": "Dallas", "subFilters": []}]},
                                {"filter": "Utah", "subFilters": [{"filter": "Salt Lake City", "subFilters": []}]},
                            ],
                        }
                    ],
                },
            ],
            "experiences": ["EARLY_CAREER", "PROFESSIONAL"],
            "searchTerm": "data",
        }
    },
    "query": _QUERY,
}


def _key(item: dict) -> str:
    role_id = str(item.get("roleId", ""))
    if role_id:
        return f"goldman_sachs:{role_id}"
    source_id = (item.get("externalSource") or {}).get("sourceId", "")
    return f"goldman_sachs:url:{source_id}"


def _normalize(item: dict) -> dict:
    title = item.get("jobTitle") or item.get("corporateTitle") or "Unknown Title"
    locs = item.get("locations") or []
    loc = "Unknown Location"
    if isinstance(locs, list) and locs:
        first = locs[0] if isinstance(locs[0], dict) else {}
        loc = first.get("primary") or make_location([first.get("city"), first.get("state"), first.get("country")])
    role_id = str(item.get("roleId", ""))
    url = f"https://higher.gs.com/roles/{role_id}" if role_id else "https://higher.gs.com/results"
    return {"key": _key(item), "title": str(title), "location": str(loc), "url": url}


class GoldmanSachsSource(BaseSource):
    name = "goldman_sachs"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        r = sess.post(_ENDPOINT, headers=_HEADERS, json=_PAYLOAD, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        items = (
            ((data.get("data") or {}).get("roleSearch") or {}).get("items") or []
        )
        if self.max_jobs and len(items) > self.max_jobs:
            items = items[:self.max_jobs]

        result: list[Job] = []
        for item in items:
            n = _normalize(item)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="Goldman Sachs",
                title=n["title"], location=n["location"], url=n["url"],
                posted="", score=cr.score, label=cr.label,
            ))

        log.info("goldman_sachs: fetched %d roles", len(result))
        return result
