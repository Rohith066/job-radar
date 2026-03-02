"""Workday CXS board source adapter with URL normalization."""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job, make_location

log = logging.getLogger(__name__)

_LOCALE_RE = re.compile(r"^[a-z]{2}[-_][a-zA-Z]{2}$")
_HEADERS = {"accept": "application/json", "content-type": "application/json", "user-agent": "Mozilla/5.0"}
_WML_APP_ERROR = "<wml:Application_Error"


def _canon_locale(seg: str) -> str:
    s = (seg or "").strip().replace("_", "-")
    parts = s.split("-")
    if len(parts) == 2:
        return f"{parts[0].lower()}-{parts[1].upper()}"
    return s


def _parse(board_url: str) -> tuple[str, str, str]:
    """Return (origin, tenant, site) from a Workday board URL."""
    u = urlparse(board_url)
    host = u.netloc
    tenant = host.split(".")[0]
    origin = f"{u.scheme}://{host}"
    segs = [s for s in (u.path or "").split("/") if s]
    if not segs:
        raise ValueError(f"Workday board_url has no path: {board_url}")
    site = segs[1] if (len(segs) >= 2 and _LOCALE_RE.match(segs[0])) else segs[0]
    return origin, tenant, site


def _board_id(board_url: str) -> str:
    try:
        _, tenant, site = _parse(board_url)
        return f"workday:{tenant}:{site}"
    except ValueError:
        return "workday:"


def _locale(board_url: str) -> str:
    segs = [s for s in (urlparse(board_url).path or "").split("/") if s]
    if segs and _LOCALE_RE.match(segs[0]):
        return _canon_locale(segs[0]) or "en-US"
    return "en-US"


def _normalize_url(board_url: str, ext: str) -> str:
    """Normalize Workday external job paths into full clickable URLs."""
    ext = (ext or "").strip()
    if not ext:
        return ""

    locale = _locale(board_url)
    try:
        _, _, site = _parse(board_url)
    except ValueError:
        return ext

    bu = urlparse(board_url)
    base_host = bu.netloc

    if ext.startswith("http"):
        eu = urlparse(ext)
        host = eu.netloc or base_host
        path = eu.path or ""
    else:
        host = base_host
        path = ext

    if not host or not path.startswith("/"):
        return ext

    segs = [s for s in path.split("/") if s]

    if len(segs) >= 2 and _LOCALE_RE.match(segs[0]) and site and segs[1] == site:
        # Already in /locale/site/... form — canonicalize locale
        canon0 = _canon_locale(segs[0])
        if canon0 != segs[0]:
            segs[0] = canon0
        new_path = "/" + "/".join(segs)
    elif len(segs) >= 2 and _LOCALE_RE.match(segs[0]) and site and segs[1] in {"job", "jobs"}:
        # /locale/job/... — insert site
        rest = "/".join(segs[1:])
        new_path = f"/{_canon_locale(segs[0]) or segs[0]}/{site}/{rest}"
    elif len(segs) >= 1 and site and segs[0] == site:
        # /site/... — insert locale
        new_path = f"/{locale}{path}"
    elif len(segs) >= 1 and segs[0] in {"job", "jobs"} and site:
        # /job/... — insert locale + site
        new_path = f"/{locale}/{site}{path}"
    else:
        new_path = path

    return f"https://{host}{new_path}"


class WorkdaySource(BaseSource):
    """Fetches jobs from a Workday CXS board endpoint."""

    def __init__(self, company: str, board_url: str) -> None:
        self.company = company
        self.board_url = board_url
        self.board_id = _board_id(board_url)
        try:
            _, self._tenant, self._site = _parse(board_url)
        except ValueError:
            self._tenant = ""
            self._site = ""
        self.name = f"workday:{self._tenant}:{self._site}"

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        if not self._tenant or not self._site:
            return []

        u = urlparse(self.board_url)
        origin = f"{u.scheme}://{u.netloc}"
        approot = f"{origin}/wday/cxs/{self._tenant}/{self._site}/approot"
        jobs_url = f"{origin}/wday/cxs/{self._tenant}/{self._site}/jobs"

        sess = get_session("workday")

        # Bootstrap call
        boot = sess.get(approot, timeout=timeout)
        boot.raise_for_status()

        all_raw: list[dict] = []
        offset = 0
        limit = 20
        safety_cap = 5000

        while True:
            payload = {"limit": limit, "offset": offset, "searchText": "", "appliedFacets": {}}
            resp = sess.post(jobs_url, headers=_HEADERS, json=payload, timeout=timeout)
            resp.raise_for_status()

            if _WML_APP_ERROR in (resp.text or ""):
                raise RuntimeError(f"Workday application error: {resp.text[:200]}")

            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                raise RuntimeError(f"Workday non-JSON response (ct={ct}): {resp.text[:200]}")

            data = resp.json() if resp.content else {}
            posts = data.get("jobPostings") or data.get("items") or []
            if not isinstance(posts, list) or not posts:
                break

            all_raw.extend(posts)
            if len(all_raw) >= 500:
                break
            offset += len(posts)
            if offset >= safety_cap:
                break

        result: list[Job] = []
        for post in all_raw:
            title = post.get("title") or post.get("jobTitle") or "Unknown Title"
            raw_loc = post.get("locationsText") or post.get("location") or "Unknown Location"
            loc = make_location([str(x) for x in raw_loc]) if isinstance(raw_loc, list) else str(raw_loc)
            posted = post.get("postedOn") or post.get("postedDate") or post.get("timePosted") or ""

            ext = post.get("externalPath") or post.get("externalUrl") or ""
            if ext:
                if not ext.startswith("/"):
                    ext = "/" + ext
                url_job = _normalize_url(self.board_url, ext)
            else:
                url_job = self.board_url

            pid = str(post.get("jobPostingId") or post.get("id") or "")
            key = (
                f"workday:{self._tenant}:{self._site}:{pid}" if pid
                else f"workday:{self._tenant}:{self._site}:url:{url_job}"
            )

            cr = classify(title)
            result.append(Job(
                key=key, source="workday", company=self.company,
                title=title, location=loc, url=url_job,
                posted=str(posted), score=cr.score, label=cr.label,
            ))

        log.debug("workday:%s:%s: fetched %d jobs", self._tenant, self._site, len(result))
        return result
