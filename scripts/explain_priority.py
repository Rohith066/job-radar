"""Print the full application-priority arithmetic for real jobs.

Every component from inputs to final score, so a number in a report can be
reproduced by hand. Opens the database read-only.

    python3 -m scripts.explain_priority --limit 20
    python3 -m scripts.explain_priority --key greenhouse:oscar:7592274
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.matching import ontology
from src.resume_matcher import load_resume
from src.apply.shortlist import evaluate_job
from src.apply import priority as P


def trace(e) -> dict:
    """Recompute the score step by step from the same inputs the code uses."""
    s = e.priority.screening_score
    f = e.fit.resume_fit_score
    fam = e.fit.role_fit
    loc = e.job.get("_location_class", "")

    blend_raw = P.W_SCREENING * s + P.W_FIT * f
    floor, ceil = s - P.FIT_SWING, s + P.FIT_SWING
    clamped = max(floor, min(ceil, blend_raw))
    rounded = int(round(clamped))

    fam_w = P.ROLE_FAMILY_WEIGHTS.get(fam, 0)
    loc_w = (P.W_US_CONFIRMED if loc in ("US", "US_REMOTE")
             else P.W_AMBIGUOUS_LOC if loc == "AMBIGUOUS" else 0)
    auth_w = P.W_WORK_AUTH_RISK if e.fit.work_auth_fit == "risk" else 0
    phd_w = P.W_PHD_REQUIRED if e.fit.education_fit == "phd_required" else 0

    total = max(0, min(100, rounded + fam_w + loc_w + auth_w + phd_w))
    return {
        "screening": s, "fit": f, "family": fam, "location_class": loc,
        "blend_raw": round(blend_raw, 2), "floor": floor, "ceiling": ceil,
        "clamped": round(clamped, 2), "rounded": rounded,
        "family_w": fam_w, "loc_w": loc_w, "auth_w": auth_w, "phd_w": phd_w,
        "computed": total, "actual": e.priority.application_priority_score,
        "band": e.priority.priority,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="state/jobs.db")
    ap.add_argument("--resume", default="config/master_resume.txt")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--key", default="")
    args = ap.parse_args()

    rskills = set(ontology.extract_canonical_skills(load_resume(args.resume)))
    conn = sqlite3.connect(f"file:{os.path.abspath(args.db)}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    sql = ("SELECT key,company,title,location,url,posted,description,resume_match "
           "FROM jobs WHERE description!=''")
    rows = conn.execute(sql + (" AND key=?" if args.key else ""),
                        (args.key,) if args.key else ()).fetchall()
    conn.close()

    from src.screening import analyze_location
    entries = []
    for r in rows:
        d = dict(r)
        e = evaluate_job(d, rskills)
        if e.priority.priority == "REJECT":
            continue
        d["_location_class"] = analyze_location(d.get("location") or "", "").classification
        e.job["_location_class"] = d["_location_class"]
        entries.append(e)
    entries.sort(key=lambda e: -e.priority.application_priority_score)

    # Spread across the range so the sample is not all top jobs.
    n = args.limit
    picked = (entries[:n // 2] + entries[len(entries) // 2: len(entries) // 2 + n // 4]
              + entries[-(n - n // 2 - n // 4):]) if len(entries) > n else entries

    print(f"\nformula: round(clamp(0.65*screening + 0.35*fit, screening-18, screening+18))")
    print(f"         + family_w + loc_w + auth_w + phd_w, clamped to 0..100\n")
    hdr = (f"{'COMPANY':<18}{'TITLE':<34}{'scr':>4}{'fit':>4}  {'blend':>7}{'clamp':>7}{'rnd':>5}"
           f"{'fam':>5}{'loc':>5}{'auth':>5}{'phd':>5}{'=':>4}{'act':>5}  BAND")
    print(hdr); print("-" * len(hdr))
    mismatches = 0
    for e in picked:
        t = trace(e)
        ok = t["computed"] == t["actual"]
        mismatches += 0 if ok else 1
        print(f"{(e.job['company'] or '')[:17]:<18}{(e.job['title'] or '')[:33]:<34}"
              f"{t['screening']:>4}{t['fit']:>4}  {t['blend_raw']:>7.2f}{t['clamped']:>7.2f}"
              f"{t['rounded']:>5}{t['family_w']:>5}{t['loc_w']:>5}{t['auth_w']:>5}{t['phd_w']:>5}"
              f"{t['computed']:>4}{t['actual']:>5}  {t['band']}{'' if ok else '  <<< MISMATCH'}")
    print("-" * len(hdr))
    print(f"{len(picked)} jobs traced, {mismatches} mismatch(es) between hand calculation "
          f"and production output\n")


if __name__ == "__main__":
    main()
