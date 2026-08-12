"""Optional local sentence-transformer backend for semantic similarity.

Design constraints driving this module:

* **Optional.** ``sentence-transformers`` pulls in torch (~2 GB). job-radar runs
  hourly on GitHub Actions where that cost is not worth paying, so the import is
  guarded and every caller degrades gracefully. Install via
  ``pip install -r requirements-semantic.txt`` for full hybrid matching.
* **No paid APIs.** Everything runs locally on CPU.
* **Cached.** The model loads once per process; embeddings are memoised by text
  hash and persisted to disk so re-scoring unchanged resumes/JDs is free.
* **No FAISS.** We compare ~50 JD skill phrases against ~150 resume sentences —
  a dense numpy matmul is faster than building an index. FAISS would be
  unjustified weight here (it *is* the right call in CrisisLens, which searches
  thousands of hazards).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Optional, Sequence

from .config import EMBED_MODEL

log = logging.getLogger(__name__)

_CACHE_DIR = Path(os.environ.get("MATCH_CACHE_DIR", "state/embeddings"))
_model = None
_model_lock = threading.Lock()
_available: Optional[bool] = None
_mem_cache: dict[str, "object"] = {}


def is_available() -> bool:
    """True when sentence-transformers + numpy can be imported."""
    global _available
    if _available is not None:
        return _available
    try:
        import numpy  # noqa: F401
        import sentence_transformers  # noqa: F401
        _available = True
    except ImportError:
        log.debug("sentence-transformers unavailable — semantic layer disabled")
        _available = False
    return _available


def _get_model():
    """Load the model once per process (thread-safe)."""
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:
                from sentence_transformers import SentenceTransformer
                log.info("Loading embedding model: %s", EMBED_MODEL)
                _model = SentenceTransformer(EMBED_MODEL)
    return _model


def _key(text: str) -> str:
    return hashlib.sha256(f"{EMBED_MODEL}|{text}".encode("utf-8")).hexdigest()[:32]


def _disk_path(k: str) -> Path:
    return _CACHE_DIR / f"{k}.json"


def embed(texts: Sequence[str], use_cache: bool = True):
    """Embed texts into L2-normalised vectors.

    Returns a numpy array of shape (len(texts), dim), or ``None`` when the
    semantic backend is unavailable. Normalised so dot product == cosine.
    """
    if not is_available() or not texts:
        return None
    import numpy as np

    vectors: list = [None] * len(texts)
    todo: list[int] = []

    for i, t in enumerate(texts):
        if not use_cache:
            todo.append(i)
            continue
        k = _key(t)
        if k in _mem_cache:
            vectors[i] = _mem_cache[k]
            continue
        p = _disk_path(k)
        if p.exists():
            try:
                v = np.asarray(json.loads(p.read_text()), dtype="float32")
                _mem_cache[k] = v
                vectors[i] = v
                continue
            except Exception:
                pass  # corrupt cache entry — recompute
        todo.append(i)

    if todo:
        fresh = _get_model().encode(
            [texts[i] for i in todo],
            normalize_embeddings=True,
            show_progress_bar=False,
        ).astype("float32")
        if use_cache:
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        for slot, vec in zip(todo, fresh):
            vectors[slot] = vec
            if use_cache:
                k = _key(texts[slot])
                _mem_cache[k] = vec
                try:
                    _disk_path(k).write_text(json.dumps(vec.tolist()))
                except Exception as e:
                    log.debug("embedding cache write failed: %s", e)

    return np.vstack(vectors)


def cosine_matrix(a_texts: Sequence[str], b_texts: Sequence[str]):
    """Pairwise cosine similarity matrix, shape (len(a), len(b)).

    Returns ``None`` if the semantic backend is unavailable.
    """
    if not is_available() or not a_texts or not b_texts:
        return None
    A = embed(a_texts)
    B = embed(b_texts)
    if A is None or B is None:
        return None
    return A @ B.T  # both L2-normalised, so this is cosine


def similarity(a: str, b: str) -> Optional[float]:
    """Cosine similarity between two strings, or None if unavailable."""
    m = cosine_matrix([a], [b])
    return float(m[0][0]) if m is not None else None


def clear_cache() -> int:
    """Delete persisted embeddings. Returns the number of files removed."""
    _mem_cache.clear()
    if not _CACHE_DIR.exists():
        return 0
    n = 0
    for p in _CACHE_DIR.glob("*.json"):
        try:
            p.unlink()
            n += 1
        except OSError:
            pass
    return n
