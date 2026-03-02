"""Shared HTTP session factory with retry logic and per-platform pooling."""
from __future__ import annotations

import threading
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_thread_local = threading.local()

_RETRY_CONFIG = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    backoff_factor=0.4,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
    respect_retry_after_header=True,
)


def make_session(timeout: Optional[int] = None) -> requests.Session:
    """Create a new requests.Session with retry adapter mounted."""
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=_RETRY_CONFIG)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"user-agent": "Mozilla/5.0", "accept": "application/json,*/*"})
    return s


def get_session(bucket: str = "default") -> requests.Session:
    """Return a thread-local session for the given bucket (platform name).

    One session per thread per bucket ensures connection pooling is preserved
    within a thread while isolating different ATS platforms.
    """
    sessions: dict = getattr(_thread_local, "sessions", None)
    if sessions is None:
        sessions = {}
        _thread_local.sessions = sessions
    if bucket not in sessions:
        sessions[bucket] = make_session()
    return sessions[bucket]
