# CLAUDE.md — job-radar

Guidance for Claude Code working in this repository.

## What this is

An automated job discovery and matching pipeline. It scans ~11,700 ATS boards
and 15 direct sources hourly via GitHub Actions, filters hard against the
owner's constraints, matches each JD against two resumes with a hybrid matcher,
and emails ranked alerts.

**The objective is landing a job.** Every feature is instrumental to that. When
trading off engineering elegance against "does this get him interviews", the
latter wins.

## Owner context

Rohith Bayya — targeting **Data Engineering** and **AI/ML Engineering**, US only,
~3 years experience, M.S. Data Analytics Engineering (George Mason, 2025).
Needs visa sponsorship, which is why the work-authorization filter exists and is
a hard drop rather than a score penalty.

Two resumes drive matching: `config/resume_de.txt` and `config/resume_ai.txt`.
The matcher scores against both and keeps the better fit.

## Architecture

```
sources/ -> hard filters -> resume matcher -> ranking -> email
```

**Hard filters (all before matching):** US-only location (blocks "Remote —
Argentina"), 3-day freshness, $90k salary floor, <=4 years experience,
staffing/agency exclusion, work authorization (citizenship / ITAR / clearance /
explicit no-sponsorship), ghost-job detection.

**`src/matching/` — the hybrid matcher.** Three layers, and **the ordering is
the design**:

1. deterministic — canonical skills, aliases, acronyms, subsumption
2. **family veto** — a *different* canonical skill in the same family is
   `RELATED_ONLY`, zero credit
3. semantic — local MiniLM embeddings, only for skills that survived the veto

The veto runs **before** semantics on purpose. Embeddings rate
`docker`↔`kubernetes` around 0.6, enough to look like a match. Semantic-only
matching produced real false positives (PyTorch resume satisfying a TensorFlow
requirement). A sibling technology must never satisfy a named requirement.

**`src/matching/jd_parser.py`** — structured requirement extraction. Segments
JDs on headings, classifies sections, attributes skills to their section.
Replaced a 400-char window heuristic that left 63% of skills with no signal.
Responsibilities are deliberately *not* requirements: "You will build data
pipelines" is a duty, not a qualification.

## Things that must not change casually

**Calibrated thresholds** (`src/matching/config.py`). `AUTO_INTERESTED_THRESHOLD=92`,
`BAND_STRONG=85`, `BAND_MODERATE=65` are percentiles of the hybrid score over
the frozen corpus. They are not round numbers picked by taste. Changing scoring
weights shifts the distribution and silently invalidates them — when the hybrid
matcher landed it moved the mean from 44 to 86, which would have fired
auto-feedback on 40% of jobs and flooded ML training with junk positives.
**If you change scoring, re-run `bench/corpus_report.py` and recalibrate.**

**The frozen corpus** (`bench/corpus/jobs.jsonl`, 200 jobs, hash-verified).
Regenerating it invalidates every prior benchmark comparison. Production pruning
is free to run precisely *because* the benchmark no longer samples the live DB.
Do not regenerate to "get fresher data".

**Ranking**: fit primary, recency tiebreaker. Deliberately **not** a composite
like `0.8*fit + 0.2*recency` — that would make "92" mean something different
from the 92 the bands were calibrated against. Freshness is already enforced
upstream by the age filter.

**Never fabricate resume content.** The matcher may only surface JD terminology
the resume already substantiates. Adding a skill the owner cannot back up is
worse than a missed match.

## Two evaluation surfaces, both needed

- `bench/benchmark_matching.py` — correctness on 33 hand-labelled decisions.
  Small, and authored alongside the ontology, so it can flatter itself.
- `bench/corpus_report.py` — score distribution over 200 frozen real JDs. No
  ground truth, but real market text that cannot be gamed the same way.

The strongest validation to date: running both resumes over the corpus inverts
the per-stratum ordering cleanly (DE resume tops analytics_engineer 87.6 /
ai_llm 54.6; AI resume inverts it). That is evidence of genuine discrimination.

## Commands

```bash
python3 -m src.main                          # run a scan
python3 -m src.main --dry-run --no-notify    # safe local run
python3 -m src.main --tailor "<job url>"     # per-JD resume cheat-sheet
python3 -m src.main --applied "<job url>"    # log an application (feeds ML)
python3 -m src.main --followup               # follow-up reminders
python3 -m pytest tests/ -q                  # 66 tests
python3 -m bench.benchmark_matching          # labelled correctness
python3 -m bench.corpus_report --compare     # distribution, legacy vs hybrid

pip install -r requirements-semantic.txt     # optional: enables semantic layer
USE_HYBRID_MATCHER=0 python3 -m src.main     # fall back to legacy TF-IDF path
```

`sentence-transformers` is optional by design — it pulls ~2 GB of torch and CI
runs hourly. The matcher degrades to lexical + TF-IDF when it is absent.

## Operational quirks

- **GitHub Actions commits `state/jobs.db` 25-30x/day**, so `.git` grows
  ~200-400 MB/month. Periodic fix: squash history to a single baseline, then
  `git reflog expire --expire=now --all && git gc --aggressive --prune=now`.
- **Local clones fall behind fast** for the same reason. `git pull --rebase`
  before assuming the local DB is current.
- **Description retention is 30 days** (`DESCRIPTION_RETENTION_DAYS`); older
  rows keep metadata but drop JD text. Descriptions were 47 MB of a 57 MB file.
- Local runs on Python 3.14 can throw `InterfaceError` from SQLite threading.
  CI runs 3.11 and is unaffected.
- Four sources are dead (Meta, Google, Apple, Netflix) — endpoints changed.
  They fail fast and are caught per-source, so runs still report success.

## Working style

The owner wants honest reporting over reassurance. Report what benchmarks
actually show, including regressions. Flag when a number improved for the wrong
reason. When a change alters scoring behaviour, say so explicitly rather than
letting it pass silently — that discipline has caught several real defects here.
