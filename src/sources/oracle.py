"""Oracle careers source adapter."""
from __future__ import annotations

import logging

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_URL = (
    "https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/"
    "recruitingCEJobRequisitions"
    "?onlyData=true"
    "&expand=requisitionList.workLocation,requisitionList.otherWorkLocations,"
    "requisitionList.secondaryLocations,flexFieldsFacet.values,"
    "requisitionList.requisitionFlexFields"
    "&finder=findReqs;siteNumber=CX_45001,"
    "facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3B"
    "ORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,"
    "limit=14,lastSelectedFacet=AttributeChar13,"
    "locationId=300000000149325,"
    "selectedCategoriesFacet=300000001559315%3B300000001917356,"
    "selectedFlexFieldsFacets=%22AttributeChar6%7C0%20to%202%2B%20years%22,"
    "selectedLocationsFacet=300000000149325,"
    "selectedPostingDatesFacet=7,sortBy=POSTING_DATES_DESC"
)
_HEADERS = {
    "accept": "application/json",
    "origin": "https://careers.oracle.com",
    "referer": "https://careers.oracle.com/",
    "user-agent": "Mozilla/5.0",
}


def _key(req: dict) -> str:
    rid = (
        req.get("requisitionId")
        or req.get("RequisitionId")
        or req.get("id")
        or req.get("Id")
        or ""
    )
    rid = str(rid)
    if rid:
        return f"oracle:{rid}"
    url = req.get("ExternalApplyLink") or req.get("applyUrl") or req.get("externalApplyUrl") or ""
    return f"oracle:url:{url}"


def _normalize(req: dict) -> dict:
    title = (
        req.get("Title")
        or req.get("title")
        or req.get("requisitionTitle")
        or req.get("requisitionName")
        or "Unknown Title"
    )
    loc_parts: list = []
    wl = req.get("workLocation")
    if isinstance(wl, dict):
        loc_parts.extend([wl.get("city"), wl.get("state"), wl.get("country")])
    loc = make_location(loc_parts) if loc_parts else "United States"
    posted = req.get("PostedDate") or req.get("postedDate") or req.get("postingDate") or ""
    url = (
        req.get("ExternalApplyLink")
        or req.get("externalApplyUrl")
        or req.get("applyUrl")
        or "https://careers.oracle.com/jobs/#en/sites/jobsearch"
    )
    return {"key": _key(req), "title": str(title), "location": str(loc), "posted": str(posted), "url": str(url)}


class OracleSource(BaseSource):
    name = "oracle"

    def __init__(self, max_jobs: int = 200) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("main")
        r = sess.get(_URL, headers=_HEADERS, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        items = data.get("items")
        reqs = items if isinstance(items, list) else (data.get("requisitionList") or [])
        if self.max_jobs and len(reqs) > self.max_jobs:
            reqs = reqs[:self.max_jobs]

        result: list[Job] = []
        for req in reqs:
            n = _normalize(req)
            cr = classify(n["title"])
            result.append(Job(
                key=n["key"], source=self.name, company="Oracle",
                title=n["title"], location=n["location"], url=n["url"],
                posted=n["posted"], score=cr.score, label=cr.label,
            ))

        log.info("oracle: fetched %d requisitions", len(result))
        return result
