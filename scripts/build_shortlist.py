#!/usr/bin/env python3
"""Generate a freshness-first "apply now" shortlist from the current radar inventory.

Standalone operational script: it reads state/jobs.db read-only and writes CSV +
Markdown into output/. It does not touch discovery, matching, scoring, the
feedback system, or the database schema.

Usage:
    python scripts/build_shortlist.py
    python scripts/build_shortlist.py --limit 40 --windows 7,14,30
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.company_filter import company_score_adjustment  # noqa: E402

# Sanity floor for a parsed posting date; anything older is treated as unusable.
DATE_FLOOR = date(2020, 1, 1)

# Conservative staffing/recruiting heuristic. This only ever sets a FLAG for human
# review — it never removes a row, and HARD_EXCLUDE_COMPANIES is never modified.
STAFFING_HINTS = [
    "staffing", "recruit", "talent", "consulting", "consultancy", "solutions inc",
    "technologies inc", "systems inc", "resourc", "placement", "hire", "hiring",
    "search group", "search partners", "headhunt", "manpower", "workforce",
    "it services", "infotech", "softtech", "technosoft", "global services",
]
KNOWN_STAFFING = ["aspiringit", "alois", "aimhire", "haystack"]

_ISO_DATE = re.compile(r"^(\d{4}-\d{2}-\d{2})$")
_ISO_DATETIME = re.compile(r"^(\d{4}-\d{2}-\d{2})T")
_REL_TODAY = re.compile(r"(?i)^posted\s+today")
_REL_YESTERDAY = re.compile(r"(?i)^posted\s+yesterday")
_REL_DAYS = re.compile(r"(?i)^posted\s+(\d+)\+?\s+day")
_LONG_MONTH = re.compile(r"(?i)^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$")


def staffing_suspect(company: str) -> bool:
    c = (company or "").lower().strip()
    if any(k in c for k in KNOWN_STAFFING):
        return True
    return any(h in c for h in STAFFING_HINTS)


def parse_posted(posted: str, first_seen_date: date, today: date) -> tuple[date | None, str]:
    """Parse the free-text `posted` column into a date.

    Relative strings ("Posted Today") are anchored to `first_seen_date`, the time
    the job was scraped — never to today. Anchoring them to today would re-date
    every stale relative posting as brand new.

    Returns (date|None, how). None means unusable, so the caller falls back to
    first_seen.
    """
    s = (posted or "").strip()
    if not s:
        return None, "empty"

    parsed: date | None = None
    how = "unrecognized"

    m = _ISO_DATE.match(s) or _ISO_DATETIME.match(s)
    if m:
        try:
            parsed, how = date.fromisoformat(m.group(1)), "iso"
        except ValueError:
            return None, "bad_iso"
    elif _REL_TODAY.match(s):
        parsed, how = first_seen_date, "rel_today"
    elif _REL_YESTERDAY.match(s):
        parsed, how = first_seen_date - timedelta(days=1), "rel_yesterday"
    elif (m := _REL_DAYS.match(s)) is not None:
        parsed, how = first_seen_date - timedelta(days=int(m.group(1))), "rel_days"
    elif (m := _LONG_MONTH.match(re.sub(r"\s+", " ", s))) is not None:
        stamp = f"{m.group(1)} {m.group(2)} {m.group(3)}"
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                parsed, how = datetime.strptime(stamp, fmt).date(), "long_month"
                break
            except ValueError:
                continue
        else:
            return None, "bad_long_month"

    if parsed is None:
        return None, how
    # A posting date in the future, or before the floor, is a source data error.
    # Note we deliberately do NOT reject posted > first_seen: many rows carry a
    # bulk-bootstrap first_seen while `posted` reflects a genuine recent refresh.
    if parsed > today:
        return None, "future"
    if parsed < DATE_FLOOR:
        return None, "too_old"
    return parsed, how


def load_candidates(conn: sqlite3.Connection, today: date) -> tuple[list[dict], dict]:
    """Return candidates passing the quality filters, with effective dates attached."""
    rows = conn.execute(
        """
        SELECT key, company, title, location, url, posted, score,
               COALESCE(resume_match, 0) AS resume_match,
               first_seen, LENGTH(description) AS desc_len
        FROM   jobs
        WHERE  label = 'yes'
          AND  key NOT IN (SELECT job_key FROM feedback)
          AND  LENGTH(description) > 200
        """
    ).fetchall()

    stats = {"sql_passed": len(rows), "hard_excluded": 0, "how": Counter()}
    out: list[dict] = []
    for r in rows:
        adj, _reason = company_score_adjustment(r["company"])
        if adj == -999:
            stats["hard_excluded"] += 1
            continue

        seen_date = date.fromisoformat(r["first_seen"][:10])
        posted_date, how = parse_posted(r["posted"], seen_date, today)
        stats["how"][how] += 1

        effective = posted_date or seen_date
        source = "posted" if posted_date else "first_seen"

        d = dict(r)
        d["effective_date"] = effective
        d["effective_source"] = source
        d["age_days"] = (today - effective).days
        out.append(d)

    stats["after_exclusions"] = len(out)
    return out, stats


def cap_per_company(rows: list[dict], cap: int = 2) -> list[dict]:
    """Keep at most `cap` rows per company, preserving the incoming rank order."""
    seen: Counter = Counter()
    kept = []
    for r in rows:
        ck = (r["company"] or "").lower().strip()
        if seen[ck] >= cap:
            continue
        seen[ck] += 1
        kept.append(r)
    return kept


def rank(rows: list[dict]) -> list[dict]:
    """Freshness first; matcher quality only breaks ties within the same date."""
    return sorted(
        rows,
        key=lambda r: (-r["effective_date"].toordinal(), -r["resume_match"], -r["score"], r["key"]),
    )


def select(candidates: list[dict], windows: list[int], limit: int) -> tuple[list[dict], int, dict]:
    """Widen the freshness window only until `limit` rows survive the company cap."""
    per_window = {}
    for w in windows:
        pool = [r for r in candidates if r["age_days"] <= w]
        per_window[w] = {"in_window": len(pool), "after_cap": len(cap_per_company(rank(pool)))}

    chosen = windows[-1]
    for w in windows:
        if per_window[w]["after_cap"] >= limit:
            chosen = w
            break

    pool = [r for r in candidates if r["age_days"] <= chosen]
    final = cap_per_company(rank(pool))[:limit]
    return final, chosen, per_window


def write_outputs(final: list[dict], out_dir: Path, today: date, generated: datetime,
                  window: int, per_window: dict, stats: dict, limit: int) -> tuple[Path, Path]:
    out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / f"shortlist_{today.isoformat()}.csv"
    md_path = out_dir / f"shortlist_{today.isoformat()}.md"

    cols = ["rank", "company", "title", "location", "score", "resume_match",
            "first_seen", "posted", "effective_date", "age_days", "url", "key",
            "staffing_suspect"]

    records = []
    for i, r in enumerate(final, 1):
        records.append({
            "rank": i,
            "company": r["company"],
            "title": r["title"],
            "location": r["location"] or "",
            "score": r["score"],
            "resume_match": r["resume_match"],
            "first_seen": (r["first_seen"] or "")[:10],
            "posted": r["posted"] or "",
            "effective_date": r["effective_date"].isoformat(),
            "age_days": r["age_days"],
            "url": r["url"],
            "key": r["key"],
            "staffing_suspect": "true" if staffing_suspect(r["company"]) else "false",
        })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(records)

    newest = min(r["age_days"] for r in final) if final else None
    oldest = max(r["age_days"] for r in final) if final else None
    flagged = sorted({r["company"] for r in records if r["staffing_suspect"] == "true"})

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Apply-now shortlist — {today.isoformat()}\n\n")
        f.write(f"**Generated:** {generated.strftime('%Y-%m-%d %H:%M:%S %Z')}  \n")
        f.write(f"**Freshness window used:** {window} days  \n")
        f.write(f"**Candidates considered:** {stats['sql_passed']} "
                f"(label `yes`, no feedback, description >200 chars)  \n")
        f.write(f"**Surviving company hard-exclusions:** {stats['after_exclusions']} "
                f"(dropped {stats['hard_excluded']})  \n")
        f.write(f"**In {window}-day window:** {per_window[window]['in_window']} "
                f"→ {per_window[window]['after_cap']} after max-2-per-company  \n")
        f.write(f"**Selected:** {len(records)}\n\n")

        f.write("| Window | In window | After 2-per-company cap |\n|---|---|---|\n")
        for w in sorted(per_window):
            mark = " ← used" if w == window else ""
            f.write(f"| {w}d | {per_window[w]['in_window']} | {per_window[w]['after_cap']}{mark} |\n")
        f.write("\n")

        if final:
            f.write(f"**Newest entry:** {newest} day(s) old · **Oldest entry:** {oldest} day(s) old\n\n")

        f.write("## How to read this\n\n")
        f.write("- **`effective_date`** — the job's real freshness date: the source `posted` "
                "date when it parses cleanly, otherwise `first_seen` (when Job Radar "
                "discovered it). Relative source values like \"Posted Today\" are anchored "
                "to the scrape date, never to today.\n")
        f.write("- **`age_days`** — days between `effective_date` and the generation date.\n")
        f.write("- **Ordering is freshness-first:** `effective_date` descending, then "
                "`resume_match`, then `score`, then `key`. A newer job outranks an older "
                "one even if the older one has a higher matcher score.\n")
        f.write("- **`resume_match` is the current matcher score; an unvalidated proxy.** "
                "It is not a probability of interview, callback, offer, or hire, and has "
                "never been validated against real hiring outcomes.\n")
        f.write("- **`staffing_suspect`** is a conservative flag for your review. Flagged "
                "rows are kept, never dropped.\n\n")

        if flagged:
            f.write(f"⚠️ **Staffing-suspect companies present:** {', '.join(flagged)}\n\n")

        f.write("| # | Company | Title | Location | Eff. date | Age | Score | resume_match | Staffing? | Link |\n")
        f.write("|---|---|---|---|---|---|---|---|---|---|\n")
        for r in records:
            flag = "⚠️ yes" if r["staffing_suspect"] == "true" else ""
            comp = (r["company"] or "").replace("|", "/")
            title = (r["title"] or "").replace("|", "/")
            loc = (r["location"] or "").replace("|", "/")
            f.write(f"| {r['rank']} | {comp} | {title} | {loc} | {r['effective_date']} | "
                    f"{r['age_days']}d | {r['score']} | {r['resume_match']} | {flag} | "
                    f"[apply]({r['url']}) |\n")

    return csv_path, md_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate a freshness-first application shortlist.")
    ap.add_argument("--db", default=str(REPO / "state" / "jobs.db"))
    ap.add_argument("--out-dir", default=str(REPO / "output"))
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--windows", default="7,14,30",
                    help="Comma-separated freshness windows in days, widened in order.")
    args = ap.parse_args()

    windows = sorted(int(w) for w in args.windows.split(",") if w.strip())
    generated = datetime.now(timezone.utc)
    today = generated.date()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    candidates, stats = load_candidates(conn, today)
    final, window, per_window = select(candidates, windows, args.limit)

    print(f"[1] passed SQL quality filters       : {stats['sql_passed']}")
    print(f"[2] after company hard-exclusions    : {stats['after_exclusions']} "
          f"(dropped {stats['hard_excluded']})")
    print("[3] freshness windows:")
    for w in sorted(per_window):
        mark = "  <-- used" if w == window else ""
        print(f"      {w:3}d : {per_window[w]['in_window']:5} in window, "
              f"{per_window[w]['after_cap']:4} after 2/company cap{mark}")
    print(f"[4] final shortlist                  : {len(final)}")
    if len(final) < args.limit:
        print(f"    NOTE: only {len(final)} eligible jobs exist within the widest "
              f"window ({windows[-1]}d). Not padded with older jobs.")

    csv_path, md_path = write_outputs(final, Path(args.out_dir), today, generated,
                                      window, per_window, stats, args.limit)
    if final:
        print(f"\n    newest entry: {final[0]['effective_date']} "
              f"({final[0]['age_days']}d old)")
        print(f"    oldest entry: {final[-1]['effective_date']} "
              f"({final[-1]['age_days']}d old)")
    print(f"\nCSV : {csv_path}")
    print(f"MD  : {md_path}")

    print("\n    posted-date parse breakdown:")
    for k, v in stats["how"].most_common():
        print(f"      {k:16}: {v}")


if __name__ == "__main__":
    main()
