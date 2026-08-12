"""Resume tailoring — generate a JD-specific resume cheat-sheet.

`python -m src.main --tailor <job_url_or_key>` does the following:
  1. Looks up the job in the DB (or fetches the JD if missing)
  2. Picks the right resume (DE vs AI) from the job's track
  3. Extracts the skills the JD asks for
  4. Splits them into KEEP (already on your resume) vs ADD (missing)
  5. Surfaces the 3 resume bullets most relevant to this JD
  6. Prints a reordered, JD-matched skills line you can paste in
  7. Optionally writes a tailored resume copy to output/

Pure stdlib + existing resume_matcher helpers. No new dependencies.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

from .classifier import classify
from .matching.config import TAILOR_STRONG, TAILOR_DECENT, TAILOR_STRETCH
from .resume_matcher import (
    _extract_skills_from_text,
    _is_required_context,
    _load_resume_file,
    fetch_jd_text,
    score_resume_vs_jd,
    top_resume_bullets,
)

log = logging.getLogger(__name__)

_RESUME_FILES = {
    "de": "config/resume_de.txt",
    "ai": "config/resume_ai.txt",
    "analyst": "config/resume_de.txt",   # analyst track uses DE resume
}
_TRACK_LABEL = {"de": "Data Engineering", "ai": "AI/ML Engineering", "analyst": "Data Engineering"}


def _lookup_job(db, identifier: str) -> dict | None:
    """Find a job row by exact URL/key, then partial URL match."""
    ident = identifier.strip()
    row = db._conn.execute(
        "SELECT key, company, title, url, description, resume_match "
        "FROM jobs WHERE url=? OR key=?", (ident, ident),
    ).fetchone()
    if not row:
        clean = ident.split("?")[0].rstrip("/")
        row = db._conn.execute(
            "SELECT key, company, title, url, description, resume_match "
            "FROM jobs WHERE url LIKE ?", (f"%{clean}%",),
        ).fetchone()
    return dict(row) if row else None


_AI_TITLE_HINTS = (
    "ai", "ml", "machine learning", "llm", "nlp", "deep learning",
    "applied scientist", "research engineer", "research scientist",
    "data scientist", "genai", "generative", "retrieval", "rag",
)


def _pick_track(title: str) -> str:
    t = title.lower()
    # Direct AI/ML signal in the title overrides the classifier's track
    if any(re.search(rf"(?<![a-z]){re.escape(h)}(?![a-z])", t) for h in _AI_TITLE_HINTS):
        return "ai"
    track = classify(title).track
    return track if track in _RESUME_FILES else "de"


def run_tailor(db, identifier: str, write_file: bool = False) -> None:
    """Print a JD-specific tailoring cheat-sheet for a single job."""
    job = _lookup_job(db, identifier)
    if not job:
        log.error("Job not found for: %s\nTip: copy the exact URL from the email APPLY link.", identifier)
        return

    company = job.get("company") or "?"
    title   = job.get("title") or "?"
    jd      = job.get("description") or ""
    url     = job.get("url") or ""

    # Fetch JD if the stored one is thin
    if len(jd) < 150 and url:
        fetched = fetch_jd_text(url)
        if fetched:
            jd = fetched

    if len(jd) < 150:
        log.error("No usable job description available for this job — cannot tailor.")
        return

    # Pick resume + track
    track = _pick_track(title)
    resume_path = _RESUME_FILES[track]
    resume_text = _load_resume_file(resume_path)
    if not resume_text:
        log.error("Resume file not found: %s", resume_path)
        return
    resume_lower = resume_text.lower()

    # Score + skill split
    result = score_resume_vs_jd(resume_text, jd, job_title=title)
    jd_skills = sorted(set(_extract_skills_from_text(jd)))

    keep, add, add_required = [], [], []
    for skill in jd_skills:
        if skill in resume_lower:
            keep.append(skill)
        else:
            if _is_required_context(jd, skill):
                add_required.append(skill)
            else:
                add.append(skill)

    bullets = top_resume_bullets(resume_text, jd, n=3)

    # ── Print the cheat-sheet ─────────────────────────────────────────────────
    W = 70
    print("\n" + "=" * W)
    print(f"  RESUME TAILORING — {company} · {title}")
    print("=" * W)
    print(f"  Track        : {_TRACK_LABEL[track]}  (use {Path(resume_path).name})")
    print(f"  Resume match : {result.overall_score}%   "
          f"(skills {result.skill_score}% · text {result.tfidf_score}%)")
    if result.required_experience:
        print(f"  JD wants     : {result.required_experience}+ years experience")
    print("=" * W)

    print(f"\n✅ KEEP — JD skills already on your resume ({len(keep)}):")
    print("   " + (", ".join(keep) if keep else "(none found)"))

    if add_required:
        print(f"\n🔴 ADD (REQUIRED) — JD lists these as required, you're missing them ({len(add_required)}):")
        print("   " + ", ".join(add_required))
        print("   → If you have ANY exposure, add them. If not, these are real gaps.")

    if add:
        print(f"\n🟡 ADD (nice-to-have) — mentioned in JD, not on your resume ({len(add)}):")
        print("   " + ", ".join(add))

    print(f"\n📌 LEAD WITH THESE 3 BULLETS (most relevant to this JD):")
    for i, b in enumerate(bullets, 1):
        print(f"   {i}. {b}")

    # Reordered skills line: JD-matching skills first
    print(f"\n🧩 SUGGESTED SKILLS LINE (JD-matched skills first — paste into resume):")
    ordered = keep + [s for s in (add_required + add) if s in resume_lower]
    # Dedup preserve order
    seen, ordered_unique = set(), []
    for s in ordered:
        if s not in seen:
            seen.add(s); ordered_unique.append(s)
    print("   " + ", ".join(s.title() if s.islower() else s for s in ordered_unique[:14]))

    # Match verdict
    print("\n" + "=" * W)
    score = result.overall_score
    if score >= TAILOR_STRONG:
        verdict = "STRONG match — apply now, minimal tailoring needed."
    elif score >= TAILOR_DECENT:
        verdict = "DECENT match — add the 🔴 required skills you have, then apply."
    elif score >= TAILOR_STRETCH:
        verdict = "STRETCH — only apply if you can honestly cover the 🔴 list."
    else:
        verdict = "WEAK match — likely not worth the application."
    print(f"  VERDICT: {verdict}")
    print("=" * W + "\n")

    # Optional: write a tailored resume copy with reordered skills
    if write_file:
        out_dir = Path("output")
        out_dir.mkdir(exist_ok=True)
        safe = re.sub(r"[^a-z0-9]+", "_", f"{company}_{title}".lower()).strip("_")[:60]
        out_path = out_dir / f"resume_{track}_{safe}.txt"
        header = (
            f"# TAILORED FOR: {company} — {title}\n"
            f"# Match: {result.overall_score}%  |  Add these if you have them: "
            f"{', '.join(add_required + add[:5]) or 'none'}\n\n"
        )
        out_path.write_text(header + resume_text, encoding="utf-8")
        print(f"  📄 Tailored resume copy written to: {out_path}\n")
