"""Shadow-mode comparison: old classification vs new Phase 1 screening.

Runs the new screening over jobs already in state and reports how their verdicts
would change, without touching production state. The database is opened in
SQLite read-only mode, so the script physically cannot write to it.

The "old" verdict is the `label` column already stored on each row. That is the
production outcome the previous pipeline actually produced for that job —
including the downstream score bonuses that could lift a capped senior title
back over the alert threshold — which makes it a truer baseline than re-running
a title classifier in isolation.

Usage:
    python3 -m scripts.shadow_compare
    python3 -m scripts.shadow_compare --db state/jobs.db --limit 5000
    python3 -m scripts.shadow_compare --corpus bench/corpus/jobs.jsonl
    python3 -m scripts.shadow_compare --show-disagreements 40
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.screening import analyze_title, analyze_location, analyze_experience, score_job
from src.screening.scoring import APPLY_NOW, STRONG, REVIEW, LOW, REJECT

OLD_LABELS = ("yes", "maybe", "no")
NEW_PRIORITIES = (APPLY_NOW, STRONG, REVIEW, LOW, REJECT)

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d",
]


def _parse_posted(posted: str):
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime((posted or "")[:26], fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


def load_from_db(path: str, limit: int) -> list[dict]:
    """Open the production database READ-ONLY. Never writes."""
    if not os.path.exists(path):
        raise SystemExit(f"No database at {path}")
    uri = f"file:{os.path.abspath(path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT title, location, description, posted, label, score "
        "FROM jobs ORDER BY last_seen DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_from_corpus(path: str) -> list[dict]:
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            out.append({
                "title": r.get("title", ""), "location": r.get("location", ""),
                "description": r.get("description", ""), "posted": r.get("posted", ""),
                "label": "", "score": 0,
            })
    return out


def screen(row: dict, country_focus: str = ""):
    title = analyze_title(row.get("title") or "")
    location = analyze_location(row.get("location") or "", country_focus)
    experience = analyze_experience(row.get("description") or "")
    return title, location, score_job(
        title=title, location=location, experience=experience,
        posted_at=_parse_posted(row.get("posted") or ""),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="state/jobs.db")
    ap.add_argument("--corpus", default="")
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--country-focus", default="",
                    help="Board country_focus to assume (default: none)")
    ap.add_argument("--show-disagreements", type=int, default=25)
    args = ap.parse_args()

    if args.corpus:
        rows = load_from_corpus(args.corpus)
        source = f"corpus {args.corpus}"
    else:
        rows = load_from_db(args.db, args.limit)
        source = f"{args.db} (read-only)"

    matrix: dict[tuple[str, str], int] = Counter()
    new_counts: Counter = Counter()
    old_counts: Counter = Counter()
    examples: dict[tuple[str, str], list] = defaultdict(list)
    reason_counts: Counter = Counter()

    for r in rows:
        title, location, opp = screen(r, args.country_focus)
        old = (r.get("label") or "?").lower()
        new = opp.priority
        matrix[(old, new)] += 1
        old_counts[old] += 1
        new_counts[new] += 1
        for c in opp.reason_codes:
            reason_counts[c] += 1
        if len(examples[(old, new)]) < args.show_disagreements:
            examples[(old, new)].append((r.get("title", ""), r.get("location", ""), opp.score))

    total = len(rows)
    print(f"\nShadow comparison — {total} jobs from {source}\n")

    print("OLD label distribution")
    for lbl in OLD_LABELS:
        if old_counts.get(lbl):
            print(f"  {lbl:6} {old_counts[lbl]:6d}  ({old_counts[lbl]/total*100:5.1f}%)")
    print()

    print("NEW priority distribution")
    for p in NEW_PRIORITIES:
        print(f"  {p:10} {new_counts.get(p, 0):6d}  ({new_counts.get(p, 0)/total*100:5.1f}%)")
    print()

    print("Transitions")
    header = "  old \\ new  " + "".join(f"{p:>11}" for p in NEW_PRIORITIES)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for old in OLD_LABELS:
        if not old_counts.get(old):
            continue
        cells = "".join(f"{matrix.get((old, p), 0):>11d}" for p in NEW_PRIORITIES)
        print(f"  {old:10} {cells}")
    print()

    alerted_before = old_counts.get("yes", 0) + old_counts.get("maybe", 0)
    alerts_now = sum(new_counts.get(p, 0) for p in (APPLY_NOW, STRONG, REVIEW))
    print(f"Alerting before : {alerted_before:6d}  ({alerted_before/total*100:5.1f}%)")
    print(f"Alerting now    : {alerts_now:6d}  ({alerts_now/total*100:5.1f}%)")
    print(f"Top band now    : {new_counts.get(APPLY_NOW, 0):6d} APPLY_NOW")
    print()

    print("Most common reason codes")
    for code, n in reason_counts.most_common(15):
        print(f"  {code:26} {n:6d}")
    print()

    print("Notable disagreements")
    for old, new in (("yes", REJECT), ("maybe", REJECT), ("no", APPLY_NOW), ("no", STRONG)):
        rows_ex = examples.get((old, new)) or []
        if not rows_ex:
            continue
        print(f"\n  {old.upper()} -> {new}   ({matrix.get((old, new), 0)} jobs)")
        for title, loc, score in rows_ex[:12]:
            print(f"    {score:3d}  {title[:62]:64} | {loc[:28]}")
    print()


if __name__ == "__main__":
    main()
