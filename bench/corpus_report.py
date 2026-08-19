"""Score-distribution report over the FROZEN benchmark corpus.

Complements ``bench/benchmark_matching.py``:

* ``benchmark_matching`` measures *correctness* on hand-labelled skill decisions
  (precision / recall / F1). It answers "does the matcher classify correctly?"
* This script measures *distribution* over 200 frozen real job descriptions.
  It answers "how does the matcher behave at scale, and did a change move the
  scores?"

Both matter. The labelled set is small and I authored it, so it can flatter the
ontology; the corpus is real, unlabelled market text and cannot be gamed the
same way — but it has no ground truth, so it only shows distribution shift, not
correctness.

Because the corpus is frozen and hash-verified, a distribution change between
runs is attributable to the matcher, not to the underlying jobs changing.

Usage:
    python -m bench.corpus_report                 # hybrid (default)
    python -m bench.corpus_report --arm legacy    # pre-hybrid TF-IDF path
    python -m bench.corpus_report --compare       # both, side by side
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import statistics as stats
from collections import Counter, defaultdict
from pathlib import Path

from .corpus.build_corpus import load_corpus, verify
from src.matching.config import AUTO_INTERESTED_THRESHOLD, BAND_MODERATE, BAND_STRONG

RESULTS_PATH = Path(__file__).parent / "corpus" / "last_report.json"
RESUMES = {"de": "config/resume_de.txt", "ai": "config/resume_ai.txt"}


def _score_all(jobs: list[dict], resume: str, hybrid: bool) -> list[int]:
    os.environ["USE_HYBRID_MATCHER"] = "1" if hybrid else "0"
    import src.resume_matcher as rm
    importlib.reload(rm)
    return [rm.score_resume_vs_jd(resume, j["description"], job_title=j["title"]).overall_score
            for j in jobs]


def _pct(sorted_scores: list[int], q: float) -> int:
    if not sorted_scores:
        return 0
    return sorted_scores[min(int(q * len(sorted_scores)), len(sorted_scores) - 1)]


def summarise(scores: list[int], jobs: list[dict]) -> dict:
    s = sorted(scores)
    by_stratum: dict[str, list[int]] = defaultdict(list)
    for j, sc in zip(jobs, scores):
        by_stratum[j["stratum"]].append(sc)
    return {
        "n": len(s),
        "mean": round(stats.mean(s), 1) if s else 0,
        "median": stats.median(s) if s else 0,
        "p25": _pct(s, 0.25), "p75": _pct(s, 0.75), "p90": _pct(s, 0.90),
        "min": s[0] if s else 0, "max": s[-1] if s else 0,
        "n_auto_interested": sum(1 for x in s if x >= AUTO_INTERESTED_THRESHOLD),
        "n_band_strong": sum(1 for x in s if x >= BAND_STRONG),
        "n_band_moderate": sum(1 for x in s if BAND_MODERATE <= x < BAND_STRONG),
        "by_stratum": {k: round(stats.mean(v), 1) for k, v in sorted(by_stratum.items())},
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--arm", choices=("hybrid", "legacy"), default="hybrid")
    ap.add_argument("--compare", action="store_true", help="run both arms")
    ap.add_argument("--resume", choices=tuple(RESUMES), default="de")
    args = ap.parse_args()

    jobs = load_corpus()
    ok = verify()
    manifest = json.loads((Path(__file__).parent / "corpus" / "manifest.json").read_text())

    print("=" * 78)
    print(f"Frozen corpus report — {len(jobs)} jobs, version {manifest['version']}")
    print(f"integrity: {'OK' if ok else 'FAILED — corpus modified since manifest!'}")
    print(f"resume: {RESUMES[args.resume]}")
    print("=" * 78)
    print("mix:", ", ".join(f"{k}={v}" for k, v in sorted(manifest["mix"].items())))
    print()

    from src.resume_matcher import _load_resume_file
    resume = _load_resume_file(RESUMES[args.resume])

    arms = ("legacy", "hybrid") if args.compare else (args.arm,)
    out: dict[str, dict] = {}
    for arm in arms:
        summary = summarise(_score_all(jobs, resume, hybrid=(arm == "hybrid")), jobs)
        out[arm] = summary
        print(f"--- {arm} ---")
        print(f"  mean {summary['mean']:5.1f}  median {summary['median']:3}  "
              f"p25 {summary['p25']:3}  p75 {summary['p75']:3}  p90 {summary['p90']:3}  "
              f"range {summary['min']}-{summary['max']}")
        print(f"  >= auto-interested ({AUTO_INTERESTED_THRESHOLD}): "
              f"{summary['n_auto_interested']:3}/{summary['n']} "
              f"({100 * summary['n_auto_interested'] // max(summary['n'], 1)}%)")
        print(f"  >= band strong ({BAND_STRONG}): {summary['n_band_strong']:3}   "
              f"band moderate ({BAND_MODERATE}-{BAND_STRONG - 1}): {summary['n_band_moderate']:3}")
        print("  mean by stratum:")
        for k, v in summary["by_stratum"].items():
            print(f"    {k:20} {v:5.1f}")
        print()

    if args.compare and len(out) == 2:
        d = out["hybrid"]["mean"] - out["legacy"]["mean"]
        print(f"delta (hybrid - legacy): mean {d:+.1f}")
        print()

    RESULTS_PATH.write_text(json.dumps(
        {"corpus_version": manifest["version"], "resume": args.resume, "arms": out},
        indent=2) + "\n")
    print(f"saved -> {RESULTS_PATH}")


if __name__ == "__main__":
    main()
