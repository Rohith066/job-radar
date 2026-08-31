# STATUS — as of 2026-08-31

Living handoff. Update when state changes; `CLAUDE.md` holds the durable
architecture and decisions, this holds the "where things stand right now".

## Repo needs reconciling first

```
2 local commits, unpushed:
    da5a9ad  Add freshness-first application shortlist generator
    1bb6e7c  Expose application outcomes end-to-end (responded/rejected/offer)

67 commits behind origin  (all "chore: update state [skip ci]" from workflows)
working tree clean · 66/66 tests passing
```

Those two commits were authored in a different session and have **not been
reviewed**. Read them before pushing. Then `git pull --rebase` and push — local
and origin have both moved.

`.git` is 238 MB (was 25 MB after the last squash). Squash again when convenient.

## Job search — the actual bottleneck

```
applied                              10
responded / dismissed                 0
follow-ups due (7+ days, no reply)   10   <- all of them
ML scorer                            not trained (needs both classes)
referral attempts                     0
```

**All ten applications are past the follow-up window with zero responses**, and
no referral outreach has happened. Ten cold applications at a 2-5% callback rate
producing nothing is statistically unremarkable — it is not evidence the matcher
is wrong. It is evidence the conversion strategy has not been executed.

`REFERRAL_PLAYBOOK.md` (in ~/Downloads) has tiered targets and templates. A
referred application converts ~20% vs 2-5% cold. That is the highest-leverage
unused artifact.

Applications logged: FanDuel, Coinbase, Fanatics x2, Esri, SMBC, NMI,
Agility Robotics, Orion Innovation, MetLife.

## Pipeline health

| | |
|---|---|
| Main / Boards / Follow-up / Health | ✅ succeeding |
| **Board Sweep Shard 3 (Workday)** | ⚠️ **cancelled twice** (Aug 31, 07:00 and 15:02) — investigate timeout or concurrency |
| Jobs in DB | 10,089 |
| Local DB freshness | stale — newest local job Aug 24, because of the 67-commit gap |

## Engineering roadmap — complete

All eight items are done. In order built: frozen benchmark corpus, structured
requirement extraction, deterministic skill normalisation, TF-IDF baseline,
MiniLM semantic layer, RELATED_ONLY family veto, three-arm benchmark, and
fit-first alert ranking.

Benchmark: skill-matching F1 **0.58 (lexical) -> 0.83 (semantic) -> 0.98 (hybrid)**,
hybrid with zero false positives.

## Known gaps, deliberately deferred

- Four dead sources: Meta, Google, Apple, Netflix — endpoints changed
- ML scorer cannot train (10 positives, 0 negatives) — needs dismissals or outcomes
- Tiered description retention by job usefulness
- `jobs` / `job_content` table split
- Moving durable state out of git entirely (repo growth is capped, not solved)

## Unvalidated assumptions

1. **That match score predicts interview likelihood.** No outcome data exists.
   This is the load-bearing assumption of the whole system.
2. The ontology and the labelled eval set share an author, so 0.98 F1 partly
   measures internal consistency. The corpus track-discrimination result is the
   stronger evidence.
3. Resume `.docx` files have never been visually confirmed to render on one
   page — page-fit is calibrated from user feedback, not rendering.

## Suggested next actions

1. Review + push the two unpushed commits; rebase onto origin
2. Follow up on all 10 applications (all overdue)
3. Investigate Shard 3 cancellations
4. Referral outreach — the unused 4x lever
5. Log outcomes as they arrive (`--applied`, `--dismiss`, and the outcome
   actions) — this is what converts the matcher from plausible to measured
