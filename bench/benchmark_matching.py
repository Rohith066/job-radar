"""Benchmark: TF-IDF baseline vs semantic-only vs hybrid skill matching.

Task: given a resume and a JD skill requirement, predict whether the resume
genuinely satisfies it. Ground truth is the hand-labelled set in eval_set.py.

Arms
  baseline  — the pre-existing behaviour: substring containment of the JD skill
              surface form in the resume text (what score_resume_vs_jd used)
  semantic  — pure embedding cosine over a threshold, no ontology at all
  hybrid    — the new three-layer matcher (deterministic → family veto → semantic)

Also sweeps the semantic threshold so the operating point is chosen from data
rather than guessed, and reports the false positives/negatives each arm makes.

    python3 -m bench.benchmark_matching
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench.eval_set import CASES, flatten          # noqa: E402
from src.matching import match as hybrid_match     # noqa: E402
from src.matching import ontology, semantic        # noqa: E402
from src.matching.config import MatchConfig        # noqa: E402


def prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) else 0.0
    r = tp / (tp + fn) if (tp + fn) else 0.0
    f = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f


def predict_baseline(resume: str, jd: str, skill: str) -> bool:
    """Original behaviour: literal substring containment."""
    return skill.lower() in resume.lower()


def predict_semantic_only(resume: str, jd: str, skill: str, threshold: float) -> bool:
    """Pure embedding similarity — no aliases, no family veto."""
    from src.matching.hybrid import split_sentences
    sents = split_sentences(resume) or [resume]
    m = semantic.cosine_matrix([skill], sents)
    if m is None:
        return False
    return float(m[0].max()) >= threshold


def predict_hybrid(resume: str, jd: str, skill: str, cfg: MatchConfig) -> bool:
    """Hybrid: satisfied iff classified EXACT / EQUIVALENT / SEMANTIC."""
    res = hybrid_match(resume, jd, config=cfg)
    cid = ontology.canonical(skill)
    for mm in res.matches:
        if mm.canonical == cid or mm.jd_surface.lower() == skill.lower():
            return mm.satisfied
    return False


def evaluate(name: str, predict_fn, rows) -> dict:
    tp = fp = fn = tn = 0
    fps, fns = [], []
    for cid, cat, resume, jd, skill, truth in rows:
        pred = predict_fn(resume, jd, skill)
        if pred and truth:
            tp += 1
        elif pred and not truth:
            fp += 1
            fps.append((cid, cat, skill))
        elif not pred and truth:
            fn += 1
            fns.append((cid, cat, skill))
        else:
            tn += 1
    p, r, f = prf(tp, fp, fn)
    acc = (tp + tn) / max(len(rows), 1)
    return {"name": name, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": p, "recall": r, "f1": f, "accuracy": acc,
            "fps": fps, "fns": fns}


def main() -> None:
    rows = flatten()
    sem_ok = semantic.is_available()

    print("=" * 78)
    print(f"Resume↔JD skill-matching benchmark — {len(rows)} labelled decisions "
          f"across {len(CASES)} cases")
    print(f"semantic backend available: {sem_ok}")
    print("=" * 78)

    results = [evaluate("baseline (substring)", predict_baseline, rows)]

    if sem_ok:
        print("\nSemantic-threshold sweep (semantic-only arm):")
        print(f"  {'thr':>5} {'prec':>7} {'recall':>7} {'F1':>7}")
        best_thr, best_f1 = 0.60, -1.0
        for thr in [0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
            r = evaluate(f"sem@{thr}",
                         lambda re_, jd, sk, t=thr: predict_semantic_only(re_, jd, sk, t),
                         rows)
            print(f"  {thr:>5.2f} {r['precision']:>7.2f} {r['recall']:>7.2f} {r['f1']:>7.2f}")
            if r["f1"] > best_f1:
                best_f1, best_thr = r["f1"], thr
        print(f"  -> best semantic-only F1 {best_f1:.2f} at threshold {best_thr}")

        results.append(evaluate(f"semantic-only @{best_thr}",
                                lambda re_, jd, sk: predict_semantic_only(re_, jd, sk, best_thr),
                                rows))

        print("\nHybrid semantic-threshold sweep:")
        print(f"  {'thr':>5} {'prec':>7} {'recall':>7} {'F1':>7}")
        best_h_thr, best_h_f1 = 0.60, -1.0
        for thr in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
            cfg = MatchConfig(semantic_threshold=thr)
            r = evaluate(f"hyb@{thr}",
                         lambda re_, jd, sk, c=cfg: predict_hybrid(re_, jd, sk, c),
                         rows)
            print(f"  {thr:>5.2f} {r['precision']:>7.2f} {r['recall']:>7.2f} {r['f1']:>7.2f}")
            if r["f1"] > best_h_f1:
                best_h_f1, best_h_thr = r["f1"], thr
        print(f"  -> best hybrid F1 {best_h_f1:.2f} at threshold {best_h_thr}")
        hybrid_cfg = MatchConfig(semantic_threshold=best_h_thr)
    else:
        hybrid_cfg = MatchConfig()
        print("\n(sentence-transformers not installed — semantic arms skipped;")
        print(" hybrid runs deterministic layers only)")

    results.append(evaluate("hybrid", lambda re_, jd, sk: predict_hybrid(re_, jd, sk, hybrid_cfg), rows))

    print("\n" + "=" * 78)
    print(f"{'ARM':<26}{'PREC':>7}{'RECALL':>8}{'F1':>7}{'ACC':>7}{'FP':>5}{'FN':>5}")
    print("-" * 78)
    for r in results:
        print(f"{r['name']:<26}{r['precision']:>7.2f}{r['recall']:>8.2f}"
              f"{r['f1']:>7.2f}{r['accuracy']:>7.2f}{r['fp']:>5}{r['fn']:>5}")

    print("\nErrors by arm:")
    for r in results:
        print(f"\n  {r['name']}")
        if r["fps"]:
            print("    FALSE POSITIVES (claimed satisfied, actually not):")
            for cid, cat, sk in r["fps"]:
                print(f"      {cid:22} [{cat}] {sk}")
        if r["fns"]:
            print("    FALSE NEGATIVES (missed a genuine match):")
            for cid, cat, sk in r["fns"]:
                print(f"      {cid:22} [{cat}] {sk}")
        if not r["fps"] and not r["fns"]:
            print("    none")

    print("\nPer-category recall (hybrid):")
    cats: dict[str, list] = {}
    for cid, cat, resume, jd, skill, truth in rows:
        cats.setdefault(cat, []).append((cid, cat, resume, jd, skill, truth))
    for cat, crows in sorted(cats.items()):
        r = evaluate(cat, lambda re_, jd, sk: predict_hybrid(re_, jd, sk, hybrid_cfg), crows)
        print(f"  {cat:26} n={len(crows):<3} P={r['precision']:.2f} "
              f"R={r['recall']:.2f} F1={r['f1']:.2f}")


if __name__ == "__main__":
    main()
