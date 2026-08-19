"""One-time sampler: live jobs.db -> frozen benchmark corpus.

Why freeze at all
-----------------
The production database is pruned on a 30-day retention policy, so a benchmark
that samples it directly changes composition every month. That makes matcher
comparisons meaningless: an F1 moving 0.79 -> 0.87 could be a better algorithm
or simply a different set of underlying job descriptions.

Freezing decouples the two. Production pruning can run freely; the benchmark
keeps measuring the same jobs.

Re-running this script is a deliberate act that produces a NEW corpus version.
It is not part of any automated flow.

Usage:
    python -m bench.corpus.build_corpus                  # default 200 jobs
    python -m bench.corpus.build_corpus --size 300 --seed 42
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import random
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

CORPUS_DIR = Path(__file__).parent
CORPUS_PATH = CORPUS_DIR / "jobs.jsonl"
MANIFEST_PATH = CORPUS_DIR / "manifest.json"

# Stratify so no single role family dominates. Weights reflect the shape of the
# real market (see the 90-day analysis) without letting Data Analyst — the
# largest family — crowd out the engineering roles the matcher is tuned for.
STRATA: dict[str, tuple[str, ...]] = {
    "data_engineer": ("data engineer", "data engineering"),
    "analytics_engineer": ("analytics engineer",),
    "platform_infra": ("data platform", "data infrastructure", "etl engineer",
                       "data warehouse"),
    "ml_engineer": ("ml engineer", "machine learning engineer", "mlops"),
    "ai_llm": ("ai engineer", "llm engineer", "nlp engineer", "applied ai",
               "generative ai", "genai"),
    "data_scientist": ("data scientist", "applied scientist"),
    "data_analyst": ("data analyst", "business intelligence", "bi analyst",
                     "bi engineer"),
}
TARGET_MIX = {
    "data_engineer": 0.26,
    "analytics_engineer": 0.16,
    "platform_infra": 0.10,
    "ml_engineer": 0.14,
    "ai_llm": 0.12,
    "data_scientist": 0.12,
    "data_analyst": 0.10,
}

MIN_DESC = 800      # substantive enough to carry real requirements
MAX_DESC = 20_000   # guard against boilerplate-heavy monsters


def clean(text: str) -> str:
    """Strip HTML and collapse whitespace, matching the pipeline's treatment."""
    t = html.unescape(html.unescape(text or ""))
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def stratum_of(title: str) -> str | None:
    t = (title or "").lower()
    for name, keys in STRATA.items():
        if any(k in t for k in keys):
            return name
    return None


def build(db_path: str, size: int, seed: int) -> dict:
    rng = random.Random(seed)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT key, company, title, location, url, posted, source, description "
        "FROM jobs WHERE description != '' ORDER BY key"   # deterministic order
    ).fetchall()
    con.close()

    # Bucket eligible jobs by stratum, deduplicating on (company, title)
    buckets: dict[str, list[dict]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    for r in rows:
        desc = clean(r["description"])
        if not (MIN_DESC <= len(desc) <= MAX_DESC):
            continue
        stratum = stratum_of(r["title"])
        if stratum is None:
            continue
        dedup_key = (r["company"].strip().lower(), r["title"].strip().lower())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        buckets[stratum].append({
            "id": hashlib.sha256(r["key"].encode()).hexdigest()[:16],
            "stratum": stratum,
            "company": r["company"],
            "title": r["title"],
            "location": r["location"],
            "source": r["source"],
            "posted": r["posted"],
            "description": desc,
        })

    # Sample per stratum against the target mix, redistributing any shortfall
    selected: list[dict] = []
    shortfall = 0
    for stratum, share in TARGET_MIX.items():
        want = round(size * share)
        pool = buckets.get(stratum, [])
        rng.shuffle(pool)
        take = pool[:want]
        selected.extend(take)
        if len(take) < want:
            shortfall += want - len(take)

    if shortfall:
        leftovers = [j for s, pool in buckets.items() for j in pool
                     if j not in selected]
        rng.shuffle(leftovers)
        selected.extend(leftovers[:shortfall])

    selected.sort(key=lambda j: j["id"])   # stable on disk

    CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    with CORPUS_PATH.open("w", encoding="utf-8") as f:
        for j in selected:
            f.write(json.dumps(j, ensure_ascii=False) + "\n")

    corpus_hash = hashlib.sha256(CORPUS_PATH.read_bytes()).hexdigest()[:16]
    manifest = {
        "version": datetime.now(timezone.utc).strftime("%Y%m%d"),
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "n_jobs": len(selected),
        "seed": seed,
        "corpus_sha256_16": corpus_hash,
        "eligible_pool": sum(len(v) for v in buckets.values()),
        "mix": dict(Counter(j["stratum"] for j in selected)),
        "min_desc_chars": MIN_DESC,
        "max_desc_chars": MAX_DESC,
        "note": "Frozen benchmark corpus. Do not regenerate casually — "
                "regenerating invalidates comparisons against prior results.",
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def load_corpus() -> list[dict]:
    """Read the frozen corpus. Raises if it has not been built."""
    if not CORPUS_PATH.exists():
        raise FileNotFoundError(
            f"No frozen corpus at {CORPUS_PATH}. "
            "Build it with: python -m bench.corpus.build_corpus"
        )
    with CORPUS_PATH.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def verify() -> bool:
    """Check the corpus file still matches the hash recorded in the manifest."""
    if not (CORPUS_PATH.exists() and MANIFEST_PATH.exists()):
        return False
    manifest = json.loads(MANIFEST_PATH.read_text())
    actual = hashlib.sha256(CORPUS_PATH.read_bytes()).hexdigest()[:16]
    return actual == manifest.get("corpus_sha256_16")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="state/jobs.db")
    ap.add_argument("--size", type=int, default=200)
    ap.add_argument("--seed", type=int, default=1337)
    args = ap.parse_args()

    if CORPUS_PATH.exists():
        print(f"WARNING: {CORPUS_PATH} exists. Regenerating invalidates "
              f"comparisons against previously reported results.")
        if input("Type 'regenerate' to proceed: ").strip() != "regenerate":
            raise SystemExit("Aborted.")

    m = build(args.db, args.size, args.seed)
    print(json.dumps(m, indent=2))
    print(f"\nWrote {m['n_jobs']} jobs to {CORPUS_PATH}")
