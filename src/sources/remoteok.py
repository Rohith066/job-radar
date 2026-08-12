"""RemoteOK source adapter — free public API, remote tech jobs.

API: https://remoteok.com/api
No API key. First array element is legal/metadata and is skipped.
Returns description + salary_min/max. All jobs are remote.
"""
from __future__ import annotations

import logging
import re

from ..classifier import classify
from ..utils.http import get_session
from .base import BaseSource, Job

log = logging.getLogger(__name__)

_ENDPOINT = "https://remoteok.com/api"


def _strip_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean(s: str) -> str:
    """Strip emoji / mojibake and unescape HTML entities (&amp; → &)."""
    if not s:
        return ""
    import html
    s = html.unescape(s)
    # Keep printable ASCII + common punctuation; drop the rest
    s = "".join(ch for ch in s if ch.isascii() and (ch.isprintable() or ch == " "))
    return re.sub(r"\s+", " ", s).strip()


def _salary(raw: dict) -> str:
    lo, hi = raw.get("salary_min"), raw.get("salary_max")
    try:
        lo = int(lo) if lo else 0
        hi = int(hi) if hi else 0
    except (ValueError, TypeError):
        return ""
    if lo and hi:
        return f"${lo:,} - ${hi:,}"
    if lo:
        return f"${lo:,}+"
    return ""


class RemoteOKSource(BaseSource):
    name = "remoteok"

    def __init__(self, max_jobs: int = 150) -> None:
        self.max_jobs = max_jobs

    def fetch(self, seen_keys: set[str], timeout: int = 30) -> list[Job]:
        sess = get_session("remoteok")
        # RemoteOK requires a real-looking User-Agent or returns 403
        headers = {
            "accept": "application/json",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        }
        try:
            r = sess.get(_ENDPOINT, headers=headers, timeout=timeout)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("remoteok fetch failed: %s", exc)
            return []

        result: list[Job] = []
        for raw in data:
            # Skip the leading legal/metadata object
            if not isinstance(raw, dict) or "position" not in raw:
                continue

            title = _clean(str(raw.get("position", ""))) or "Unknown Title"
            cr = classify(title)
            if cr.label == "no":
                continue

            jid = str(raw.get("id", ""))
            url = str(raw.get("url") or raw.get("apply_url") or "")
            if not jid and not url:
                continue

            result.append(Job(
                key=f"remoteok:{jid or url}",
                source=self.name,
                company=_clean(str(raw.get("company", ""))),
                title=title,
                location="Remote, USA",   # RemoteOK is global-remote; US filter keeps it
                url=url,
                posted=str(raw.get("date", "")),
                score=cr.score, label=cr.label,
                salary=_salary(raw),
                work_type="Remote",
                description=_strip_html(raw.get("description", "")),
            ))
            if len(result) >= self.max_jobs:
                break

        log.info("remoteok: fetched %d positions", len(result))
        return result
