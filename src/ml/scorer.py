"""ML-based job re-scorer.

Uses a Logistic Regression model trained on user feedback to adjust job scores.
The model learns which jobs the user likes (applied/interested) vs dislikes
(dismissed) and boosts or penalises future jobs accordingly.

Cold-start safe: if fewer than MIN_FEEDBACK_ROWS training samples exist, the
model is skipped and rule-based scores are left untouched.

Flow:
  1. run_main() collects jobs, rule-based classifier sets j.score
  2. ml_rescore(jobs, db) is called — adjusts j.score by ±ML_MAX_BOOST
  3. Model is retrained at end of each run via retrain(db)
  4. Trained model is persisted to state/ml_model.pkl
"""
from __future__ import annotations

import logging
import pickle
import re
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── tunables ──────────────────────────────────────────────────────────────────
MIN_FEEDBACK_ROWS = 10   # need at least this many labelled jobs before ML kicks in
ML_MAX_BOOST      = 15   # max score adjustment in either direction (±15)
MODEL_PATH        = Path("state/ml_model.pkl")
# ─────────────────────────────────────────────────────────────────────────────


# ── feature extraction ────────────────────────────────────────────────────────

_STOP = frozenset({
    "a","an","the","and","or","of","in","at","to","for","with","on","is",
    "are","this","that","we","you","your","our","as","be","by","from","us",
    "–","-","&","/","(",")",",",".",":","senior","sr","jr","ii","iii","iv",
})

def _title_tokens(title: str) -> list[str]:
    """Lowercase alphanumeric tokens from a job title, stop-words removed."""
    tokens = re.findall(r"[a-z0-9]+", title.lower())
    return [t for t in tokens if t not in _STOP and len(t) > 1]


def _source_group(source: str) -> str:
    """Bucket source names into broad groups to reduce dimensionality."""
    s = (source or "").lower()
    if s in {"microsoft", "amazon", "google", "meta", "apple", "netflix"}:
        return "faang"
    if s in {"goldman_sachs", "stripe"}:
        return "fintech"
    if s in {"ibm", "oracle", "nvidia"}:
        return "enterprise_tech"
    if s in {"linkedin"}:
        return "linkedin"
    if "greenhouse" in s or "lever" in s:
        return "ats_small"
    if "workday" in s or "smartrecruiters" in s:
        return "ats_large"
    return "other"


def extract_features(job_data: dict) -> dict:
    """Convert a job dict / Job object attrs into a flat feature dict.

    Keys:
      token_<word>  : 1 if word appears in title
      is_remote     : 1 / 0
      is_hybrid     : 1 / 0
      is_onsite     : 1 / 0
      rule_score    : normalised 0-1
      src_<group>   : 1 / 0 source group flags
    """
    title     = job_data.get("title", "") or ""
    work_type = (job_data.get("work_type", "") or "").lower()
    score     = int(job_data.get("score", 50) or 50)
    source    = job_data.get("source", "") or ""

    feats: dict[str, float] = {}

    # Title token features
    for tok in _title_tokens(title):
        feats[f"token_{tok}"] = 1.0

    # Work-type
    feats["is_remote"] = 1.0 if work_type == "remote" else 0.0
    feats["is_hybrid"] = 1.0 if work_type == "hybrid" else 0.0
    feats["is_onsite"] = 1.0 if work_type == "onsite" else 0.0

    # Normalised rule-based score
    feats["rule_score"] = score / 100.0

    # Source group one-hot
    sg = _source_group(source)
    for g in ("faang", "fintech", "enterprise_tech", "linkedin", "ats_small", "ats_large", "other"):
        feats[f"src_{g}"] = 1.0 if sg == g else 0.0

    return feats


# ── model persistence ─────────────────────────────────────────────────────────

def _save_model(model_data: dict) -> None:
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model_data, f)


def _load_model() -> Optional[dict]:
    if not MODEL_PATH.exists():
        return None
    try:
        with open(MODEL_PATH, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        log.warning("Could not load ML model: %s", e)
        return None


# ── training ──────────────────────────────────────────────────────────────────

def retrain(db) -> bool:
    """Retrain model from feedback table. Returns True if model was saved."""
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import LabelEncoder
    except ImportError:
        log.debug("scikit-learn not installed — ML scoring disabled.")
        return False

    rows = db.get_feedback_jobs()
    if len(rows) < MIN_FEEDBACK_ROWS:
        log.debug("Only %d feedback rows — need %d to train ML model.", len(rows), MIN_FEEDBACK_ROWS)
        return False

    # Build training data
    X_dicts, y = [], []
    for r in rows:
        action = r.get("action", "")
        if action not in ("applied", "dismissed", "interested"):
            continue
        label = 1 if action in ("applied", "interested") else 0
        feats = extract_features({
            "title":     r.get("title", ""),
            "work_type": r.get("work_type", ""),
            "score":     r.get("score", 50),
            "source":    r.get("source", ""),
        })
        X_dicts.append(feats)
        y.append(label)

    if len(y) < MIN_FEEDBACK_ROWS or len(set(y)) < 2:
        log.debug("Not enough class diversity to train (need both positive & negative feedback).")
        return False

    # Build vocabulary from all feature keys seen in training data
    vocab = sorted({k for d in X_dicts for k in d})

    # Convert dicts → dense matrix
    X = [[d.get(k, 0.0) for k in vocab] for d in X_dicts]

    clf = LogisticRegression(max_iter=500, class_weight="balanced", random_state=42)
    clf.fit(X, y)

    model_data = {"clf": clf, "vocab": vocab, "version": 1}
    _save_model(model_data)
    pos = sum(y)
    log.info(
        "ML model trained: %d samples (%d positive / %d negative), %d features.",
        len(y), pos, len(y) - pos, len(vocab),
    )
    return True


# ── inference ─────────────────────────────────────────────────────────────────

def ml_rescore(jobs: list, db=None) -> list:
    """Adjust job scores using the trained ML model.

    Args:
        jobs: list of Job objects (must have .title, .work_type, .score, .source, .label)
        db:   Database instance — used to retrain model before scoring

    Returns:
        The same list with .score possibly adjusted by ±ML_MAX_BOOST.
        Jobs with label='no' are never adjusted (they're already filtered out).
    """
    # Retrain first so the model reflects any new feedback added this session
    if db is not None:
        retrain(db)

    model_data = _load_model()
    if model_data is None:
        log.debug("No ML model available — using rule-based scores only.")
        return jobs

    clf   = model_data["clf"]
    vocab = model_data["vocab"]

    adjusted = boosted = penalised = 0
    for job in jobs:
        if job.label == "no":
            continue
        feats = extract_features({
            "title":     job.title,
            "work_type": getattr(job, "work_type", ""),
            "score":     job.score,
            "source":    job.source,
        })
        x = [[feats.get(k, 0.0) for k in vocab]]
        prob_positive = clf.predict_proba(x)[0][1]  # P(user likes this)

        # Map probability to a score delta in [-ML_MAX_BOOST, +ML_MAX_BOOST]
        # prob=1.0 → +ML_MAX_BOOST, prob=0.5 → 0, prob=0.0 → -ML_MAX_BOOST
        delta = int((prob_positive - 0.5) * 2 * ML_MAX_BOOST)
        new_score = max(0, min(100, job.score + delta))

        if delta != 0:
            log.debug(
                "ML rescore: [%s] %s  %d → %d  (Δ%+d, p=%.2f)",
                job.company, job.title, job.score, new_score, delta, prob_positive,
            )
            adjusted += 1
            if delta > 0:
                boosted += 1
            else:
                penalised += 1

        job.score = new_score
        # Re-classify label based on updated score
        if job.score >= 70:
            job.label = "yes"
        elif job.score >= 40:
            job.label = "maybe"
        else:
            job.label = "no"

    if adjusted:
        log.info("ML rescoring: %d jobs adjusted (%d boosted, %d penalised).", adjusted, boosted, penalised)

    return jobs


# ── model info (for health check) ────────────────────────────────────────────

def get_model_info() -> dict:
    """Return basic info about the current model for the health check email."""
    model_data = _load_model()
    if model_data is None:
        return {"trained": False, "features": 0}
    return {
        "trained":  True,
        "features": len(model_data.get("vocab", [])),
        "version":  model_data.get("version", 1),
    }
