"""Application workflow CLI.

    python3 -m src.apply queue                 # today's ranked application queue
    python3 -m src.apply list [--status APPLIED]
    python3 -m src.apply shortlist <job_key>
    python3 -m src.apply applied   <job_key> [--note "..."]
    python3 -m src.apply interview <job_key>
    python3 -m src.apply rejected  <job_key>
    python3 -m src.apply no-response <job_key>
    python3 -m src.apply skip      <job_key>
    python3 -m src.apply metrics [--days 7]
    python3 -m src.apply observe [--days 1] [--json]
    python3 -m src.apply backup
    python3 -m src.apply where            # which databases are in use

Decision support and tracking only. Nothing here contacts an employer.

User decisions are written to a user-state database outside the repository
(default ~/.job-system/user_state.db, override with $JOB_USER_STATE_DB), so
CI rewriting state/jobs.db cannot destroy application history.
"""
from __future__ import annotations

import argparse
import sys

from ..config import Config
from ..database import Database
from .queue import (
    ApplicationQueue, IllegalTransition,
    NEW, SHORTLISTED, APPLIED, INTERVIEW, REJECTED, NO_RESPONSE, SKIPPED,
)
from .shortlist import build_queue, render_queue, evaluate_job, DEFAULT_LIMIT

_STATUS_COMMANDS = {
    "shortlist": SHORTLISTED, "applied": APPLIED, "interview": INTERVIEW,
    "rejected": REJECTED, "no-response": NO_RESPONSE, "skip": SKIPPED,
}


def _resolve_key(db, ident: str) -> str | None:
    """Accept a job key, a full URL, or a unique URL fragment."""
    ident = (ident or "").strip()
    if not ident:
        # Without this an empty argument becomes LIKE '%%' and matches every job.
        return None
    row = db._conn.execute(
        "SELECT key FROM jobs WHERE key=? OR url=?", (ident, ident)
    ).fetchone()
    if row:
        return row["key"]
    rows = db._conn.execute(
        "SELECT key FROM jobs WHERE url LIKE ? LIMIT 5", (f"%{ident.split('?')[0].rstrip('/')}%",)
    ).fetchall()
    if len(rows) == 1:
        return rows[0]["key"]
    if len(rows) > 1:
        print(f"Ambiguous identifier {ident!r} — matched {len(rows)} jobs. Use the exact job key.")
    return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="src.apply", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="config.yaml")
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("queue", help="today's ranked application queue")
    q.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    q.add_argument("--include-acted", action="store_true",
                   help="also show jobs already applied to or skipped")

    l = sub.add_parser("list", help="list tracked applications")
    l.add_argument("--status", default=None)
    l.add_argument("--limit", type=int, default=50)

    for name in _STATUS_COMMANDS:
        s = sub.add_parser(name, help=f"mark a job {_STATUS_COMMANDS[name]}")
        s.add_argument("job", help="job key or URL")
        s.add_argument("--note", default="")
        s.add_argument("--force", action="store_true", help="override transition validation")

    m = sub.add_parser("metrics", help="application outcome metrics")
    m.add_argument("--days", type=int, default=None)

    o = sub.add_parser("observe", help="read-only observation snapshot")
    o.add_argument("--days", type=int, default=None)
    o.add_argument("--json", action="store_true", help="emit JSON only")

    sub.add_parser("backup", help="timestamped backup of the user-state database")
    sub.add_parser("where", help="show which databases are in use")

    args = p.parse_args(argv)
    cfg = Config.load(args.config)
    # Read-only on the discovery DB: user actions read jobs and scores from it
    # but must never modify it. All writes land in the attached user-state DB.
    db = Database(cfg.database.path, readonly=True)
    aq = ApplicationQueue(db)

    try:
        if args.cmd == "queue":
            print(render_queue(build_queue(db, limit=args.limit, include_acted=args.include_acted)))
            return 0

        if args.cmd == "list":
            apps = aq.list(status=args.status, limit=args.limit)
            if not apps:
                print("No tracked applications yet.")
                return 0
            print(f"\n{'STATUS':<13}{'PRIO':>5}{'FIT':>5}  {'UPDATED':<12} JOB")
            print("-" * 92)
            for a in apps:
                row = db._conn.execute(
                    "SELECT company, title FROM jobs WHERE key=?", (a.job_key,)
                ).fetchone()
                label = f"{row['company']} — {row['title']}" if row else a.job_key
                print(f"{a.status:<13}{a.priority_at_decision or 0:>5}{a.fit_at_decision or 0:>5}  "
                      f"{(a.updated_at or '')[:10]:<12} {label[:56]}")
            print()
            return 0

        if args.cmd in _STATUS_COMMANDS:
            key = _resolve_key(db, args.job)
            if not key:
                print(f"No job found for {args.job!r}. Copy the key or URL from the email.")
                return 1
            status = _STATUS_COMMANDS[args.cmd]
            row = db._conn.execute(
                "SELECT company,title,location,url,posted,description,resume_match "
                "FROM jobs WHERE key=?", (key,)
            ).fetchone()
            # Snapshot what the system thinks right now, so the decision can be
            # evaluated later against its outcome.
            prio = fit = screen = None
            pver = ""
            if row:
                try:
                    from ..matching import ontology
                    from ..resume_matcher import load_resume, _resume_path_from_env
                    from .provenance import profile_version
                    rt = load_resume(_resume_path_from_env())
                    rs = set(ontology.extract_canonical_skills(rt)) if rt else set()
                    pver = profile_version(rt)
                    e = evaluate_job(dict(row) | {"key": key}, rs)
                    if e:
                        prio, fit, screen = (e.priority.application_priority_score,
                                             e.fit.resume_fit_score, e.screening.score)
                except Exception:
                    pass
            try:
                app = aq.set_status(key, status, priority=prio, fit=fit, screening=screen,
                                    profile_version=pver, note=args.note, force=args.force)
            except IllegalTransition as exc:
                print(f"Refused: {exc}\nUse --force only if you are correcting a mistake.")
                return 1
            label = f"{row['company']} — {row['title']}" if row else key
            print(f"{app.status}: {label}"
                  + (f"   (priority {prio}, fit {fit})" if prio is not None else ""))
            return 0

        if args.cmd == "where":
            from .user_state import user_state_path, ENV_VAR
            print(f"\n  discovery DB (CI-owned, read-only for user actions):\n    {cfg.database.path}")
            print(f"\n  user-state DB (your decisions, durable):\n    {db.user_state_path}")
            print(f"\n  override with ${ENV_VAR}\n")
            return 0

        if args.cmd == "backup":
            from .user_state import backup as _backup
            if not db.has_user_state:
                print("No user-state database to back up.")
                return 1
            out = _backup(db.user_state_path)
            print(f"Backup written: {out} ({out.stat().st_size} bytes)")
            return 0

        if args.cmd == "observe":
            from .observe import snapshot, render
            snap = snapshot(db, days=args.days)
            if args.json:
                import json as _json
                print(_json.dumps(snap, indent=2, sort_keys=True))
            else:
                print(render(snap))
            return 0

        if args.cmd == "metrics":
            m = aq.metrics(days=args.days)
            span = f"last {args.days} days" if args.days else "all time"
            print(f"\nAPPLICATION METRICS — {span}\n" + "-" * 46)
            for s, c in m["counts"].items():
                print(f"  {s:<14}{c:>5}")
            print("-" * 46)
            print(f"  {'applications':<14}{m['applications_total']:>5}")
            print(f"  {'interviews':<14}{m['interviews']:>5}")
            rate = m["interview_rate"]
            print(f"  {'interview rate':<14}{(f'{rate:.1%}' if rate is not None else 'n/a'):>6}")
            print(f"  {'avg priority':<14}{str(m['avg_priority_at_decision'] or 'n/a'):>5}")
            if m["by_role_family"]:
                print("\n  By role family:")
                for fam, d in sorted(m["by_role_family"].items()):
                    print(f"    {fam:<24}{d['applications']:>4} applied  {d['interviews']:>3} interviews")
            if m["by_priority_band"]:
                print("\n  By priority band at decision time:")
                for band, d in m["by_priority_band"].items():
                    print(f"    {band:<24}{d['applications']:>4} applied  {d['interviews']:>3} interviews")
            print()
            return 0

        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
