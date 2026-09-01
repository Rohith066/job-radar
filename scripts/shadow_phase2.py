"""Shadow validation: Phase 1 screening priority vs Phase 2 application priority.

Opens the database read-only, so it cannot modify production state. Reports the
transition matrix plus the specific slices the Phase 2 brief calls for: the top
jobs, the ones fit promoted or demoted hardest, first-class families landing in
LOW, and jobs where a single missing skill drove a large penalty.

    python3 -m scripts.shadow_phase2 [--db state/jobs.db] [--top 50]
"""
from __future__ import annotations

import argparse
import collections
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.matching import ontology
from src.resume_matcher import load_resume
from src.apply.shortlist import evaluate_job
from src.apply.priority import APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW, REJECT

P1 = ("APPLY_NOW", "STRONG", "REVIEW", "LOW", "REJECT")
P2 = (APPLY_FIRST, HIGH, MEDIUM, REVIEW, LOW, REJECT)
FIRST_CLASS = {"software_engineering", "backend", "fullstack",
               "data_engineering", "ml_ai", "data_science"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="state/jobs.db")
    ap.add_argument("--resume", default="config/master_resume.txt")
    ap.add_argument("--top", type=int, default=50)
    args = ap.parse_args()

    resume = load_resume(args.resume)
    rskills = set(ontology.extract_canonical_skills(resume)) if resume else set()

    conn = sqlite3.connect(f"file:{os.path.abspath(args.db)}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT key,company,title,location,url,posted,description,resume_match FROM jobs"
    ).fetchall()
    conn.close()

    matrix = collections.Counter()
    p1c, p2c = collections.Counter(), collections.Counter()
    entries = []
    for r in rows:
        e = evaluate_job(dict(r), rskills)
        matrix[(e.screening.priority, e.priority.priority)] += 1
        p1c[e.screening.priority] += 1
        p2c[e.priority.priority] += 1
        entries.append(e)

    total = len(entries)
    print(f"\nPhase 1 -> Phase 2 shadow comparison — {total} jobs, {args.db} (read-only)\n")

    print("Phase 1 screening priority")
    for b in P1:
        print(f"  {b:<12}{p1c[b]:>6} ({p1c[b]/total*100:5.1f}%)")
    print("\nPhase 2 application priority")
    for b in P2:
        print(f"  {b:<12}{p2c[b]:>6} ({p2c[b]/total*100:5.1f}%)")

    print("\nTransitions")
    hdr = "  p1 \\ p2      " + "".join(f"{b:>12}" for b in P2)
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    for a in P1:
        if not p1c[a]:
            continue
        print(f"  {a:<13}" + "".join(f"{matrix.get((a,b),0):>12}" for b in P2))

    acted = [e for e in entries if e.priority.priority != REJECT]
    acted.sort(key=lambda e: -e.priority.application_priority_score)

    print(f"\n=== TOP {args.top} BY APPLICATION PRIORITY ===")
    for e in acted[:args.top]:
        j, f, p = e.job, e.fit, e.priority
        print(f"  {p.application_priority_score:>3} {p.priority:<11} s{p.screening_score:>3} f{p.resume_fit_score:>3} "
              f"{(j['title'] or '')[:44]:46}| {(j['company'] or '')[:20]:22}| {e.screening.priority}")

    promoted = sorted(acted, key=lambda e: -e.priority.fit_adjustment)[:15]
    demoted  = sorted(acted, key=lambda e: e.priority.fit_adjustment)[:15]
    print("\n=== MOST PROMOTED BY RESUME FIT ===")
    for e in promoted:
        print(f"  +{e.priority.fit_adjustment:<3} s{e.priority.screening_score:>3}->p{e.priority.application_priority_score:>3} "
              f"f{e.fit.resume_fit_score:>3}  {(e.job['title'] or '')[:52]}")
    print("\n=== MOST DEMOTED BY RESUME FIT ===")
    for e in demoted:
        print(f"  {e.priority.fit_adjustment:<4} s{e.priority.screening_score:>3}->p{e.priority.application_priority_score:>3} "
              f"f{e.fit.resume_fit_score:>3}  {(e.job['title'] or '')[:52]}")

    lows = [e for e in entries if e.priority.priority == LOW and e.fit.role_fit in FIRST_CLASS]
    print(f"\n=== FIRST-CLASS FAMILIES LANDING IN LOW: {len(lows)} ===")
    for e in lows[:20]:
        print(f"  p{e.priority.application_priority_score:>3} s{e.priority.screening_score:>3} f{e.fit.resume_fit_score:>3} "
              f"{(e.job['title'] or '')[:44]:46}| miss_req={list(e.fit.missing_required_skills)[:3]}")

    single = [e for e in acted
              if len(e.fit.missing_required_skills) == 1 and e.priority.fit_adjustment <= -10]
    print(f"\n=== ONE MISSING REQUIRED SKILL CAUSED A LARGE PENALTY: {len(single)} ===")
    for e in single[:15]:
        print(f"  {e.priority.fit_adjustment:<4} f{e.fit.resume_fit_score:>3} miss={list(e.fit.missing_required_skills)} "
              f"{(e.job['title'] or '')[:44]}")

    fams = collections.Counter(e.fit.role_fit for e in acted[:200])
    print(f"\n=== ROLE FAMILY MIX IN TOP 200 ===")
    for fam, n in fams.most_common():
        print(f"  {fam:<24}{n:>4}")
    print()


if __name__ == "__main__":
    main()
