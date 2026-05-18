"""Notification dispatchers: HTML email, Slack webhook, Discord webhook.

Key improvements over the original:
- HTML email with color-coded YES/MAYBE buckets and score badges
- Slack block-kit messages (rich formatting)
- Discord embed messages
- CompositeNotifier sends to all configured channels
- All notifiers are opt-in (skip gracefully if not configured)
"""
from __future__ import annotations

import json
import logging
import re as _re
import smtplib
import ssl
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests

from .sources.base import Job
from .profile import PROFILE, profile_summary_html, profile_summary_text

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Freshness helpers
# ---------------------------------------------------------------------------

def _hours_ago(posted: str) -> Optional[float]:
    """Convert a posted string to approximate hours since posted.
    Returns None if the string can't be parsed."""
    if not posted:
        return None
    now = datetime.now(timezone.utc)
    p = posted.strip().lower()
    # Relative strings — "2 hours ago", "30 minutes ago", "3 days ago"
    m = _re.search(r'(\d+)\s*(minute|hour|day|week)', p)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if 'minute' in unit: return n / 60.0
        if 'hour'   in unit: return float(n)
        if 'day'    in unit: return n * 24.0
        if 'week'   in unit: return n * 168.0
    if any(x in p for x in ('just now', 'moments ago')):
        return 0.0
    if 'today' in p:
        return 3.0       # conservative estimate — counts as same-day
    if 'yesterday' in p:
        return 28.0
    # ISO / plain date strings
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(posted.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (now - dt).total_seconds() / 3600.0)
        except ValueError:
            continue
    return None


def _freshness_badge_html(posted: str) -> str:
    """Return an HTML badge string for how fresh a posting is."""
    h = _hours_ago(posted)
    if h is None:
        return ""
    if h < 2:
        return '<span class="badge-hot">&#128293; Just Posted</span>'
    if h < 6:
        return '<span class="badge-new">&#9889; &lt; 6 hrs ago</span>'
    if h < 24:
        return '<span class="badge-today">&#128197; Today</span>'
    return ""


def _posted_friendly(posted: str) -> str:
    """Human-readable posted time — e.g. '2 hours ago', 'today', '2026-04-09'."""
    h = _hours_ago(posted)
    if h is None:
        return posted or ""
    if h < 1:
        mins = int(h * 60)
        return f"{mins}m ago" if mins > 1 else "just now"
    if h < 24:
        return f"{int(h)}h ago"
    days = int(h / 24)
    return f"{days}d ago" if days < 7 else posted


# ---------------------------------------------------------------------------
# HTML email template
# ---------------------------------------------------------------------------
_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
          background: #f5f5f5; margin: 0; padding: 20px; color: #333; }}
  .container {{ max-width: 700px; margin: 0 auto; background: #fff;
                border-radius: 8px; overflow: hidden;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
  .header {{ background: #1a1a2e; color: #fff; padding: 20px 28px; }}
  .header h1 {{ margin: 0; font-size: 22px; }}
  .header p  {{ margin: 4px 0 0; font-size: 13px; color: #aaa; }}
  .section   {{ padding: 20px 28px; }}
  .section-title {{ font-size: 14px; font-weight: 700; letter-spacing: 0.05em;
                    text-transform: uppercase; margin-bottom: 14px;
                    padding-bottom: 6px; border-bottom: 2px solid; }}
  .yes-title   {{ color: #16a34a; border-color: #16a34a; }}
  .maybe-title {{ color: #d97706; border-color: #d97706; }}
  .job-card {{ border-radius: 6px; padding: 14px 16px; margin-bottom: 12px;
               border-left: 4px solid; background: #fafafa; }}
  .job-card-yes   {{ border-color: #16a34a; }}
  .job-card-maybe {{ border-color: #d97706; }}
  .job-title  {{ font-size: 16px; font-weight: 600; margin: 0 0 4px; }}
  .job-meta   {{ font-size: 13px; color: #666; margin: 0 0 8px; }}
  .job-link   {{ display: inline-block; font-size: 13px; color: #1d4ed8;
                 text-decoration: none; font-weight: 500; }}
  .score-badge {{ display: inline-block; font-size: 11px; font-weight: 700;
                  border-radius: 999px; padding: 2px 8px; margin-left: 8px;
                  vertical-align: middle; }}
  .badge-yes      {{ background: #dcfce7; color: #15803d; }}
  .badge-maybe    {{ background: #fef9c3; color: #a16207; }}
  .badge-remote   {{ background: #dbeafe; color: #1e40af; font-size: 11px;
                     font-weight: 600; border-radius: 999px; padding: 2px 8px;
                     margin-left: 6px; vertical-align: middle; }}
  .badge-hybrid   {{ background: #ede9fe; color: #5b21b6; font-size: 11px;
                     font-weight: 600; border-radius: 999px; padding: 2px 8px;
                     margin-left: 6px; vertical-align: middle; }}
  .badge-onsite   {{ background: #fee2e2; color: #991b1b; font-size: 11px;
                     font-weight: 600; border-radius: 999px; padding: 2px 8px;
                     margin-left: 6px; vertical-align: middle; }}
  .salary-line {{ font-size: 13px; color: #15803d; font-weight: 600; margin-left: 6px; }}
  /* Freshness badges */
  .badge-hot   {{ background: #fee2e2; color: #dc2626; font-size: 11px; font-weight: 700;
                  border-radius: 999px; padding: 2px 10px; margin-right: 4px; }}
  .badge-new   {{ background: #fef9c3; color: #b45309; font-size: 11px; font-weight: 700;
                  border-radius: 999px; padding: 2px 10px; margin-right: 4px; }}
  .badge-today {{ background: #dbeafe; color: #1d4ed8; font-size: 11px; font-weight: 700;
                  border-radius: 999px; padding: 2px 10px; margin-right: 4px; }}
  .badge-match {{ background: #f0fdf4; color: #15803d; font-size: 11px; font-weight: 600;
                  border-radius: 999px; padding: 2px 8px; margin-left: 6px; vertical-align: middle; }}
  /* Apply button */
  .apply-btn   {{ display: inline-block; background: #16a34a; color: #fff !important;
                  text-decoration: none; font-weight: 700; font-size: 14px;
                  padding: 9px 22px; border-radius: 6px; margin-top: 10px;
                  letter-spacing: 0.02em; }}
  /* Ghost warning banners */
  .ghost-suspicious {{ background: #fef2f2; border: 1px solid #fca5a5; border-radius: 4px;
                        padding: 6px 10px; margin-bottom: 8px; font-size: 12px; color: #dc2626; }}
  .ghost-caution    {{ background: #fffbeb; border: 1px solid #fcd34d; border-radius: 4px;
                        padding: 6px 10px; margin-bottom: 8px; font-size: 12px; color: #92400e; }}
  /* Resume highlights */
  .highlights-box  {{ background: #f0fdf4; border-left: 3px solid #16a34a;
                       padding: 8px 12px; margin: 10px 0 6px; border-radius: 0 4px 4px 0; }}
  .highlights-title {{ font-size: 11px; font-weight: 700; color: #15803d;
                        text-transform: uppercase; letter-spacing: 0.05em; margin: 0 0 4px; }}
  .highlights-box ul {{ margin: 0; padding-left: 16px; }}
  .highlights-box li {{ font-size: 12px; color: #374151; margin-bottom: 3px; }}
  /* LinkedIn DM box */
  .dm-box   {{ background: #eff6ff; border-left: 3px solid #3b82f6;
               padding: 8px 12px; margin: 6px 0 10px; border-radius: 0 4px 4px 0; }}
  .dm-title {{ font-size: 11px; font-weight: 700; color: #1d4ed8;
               text-transform: uppercase; letter-spacing: 0.05em; margin: 0 0 4px; }}
  .dm-text  {{ font-size: 12px; color: #1e3a5f; margin: 0; line-height: 1.5; }}
  /* Follow-up email styles */
  .fu-card  {{ background: #fafafa; border-left: 4px solid #f59e0b;
               border-radius: 6px; padding: 14px 16px; margin-bottom: 12px; }}
  .fu-title {{ font-size: 15px; font-weight: 600; margin: 0 0 4px; color: #111; }}
  .fu-meta  {{ font-size: 13px; color: #666; margin: 0 0 8px; }}
  .fu-template {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 4px;
                   padding: 10px 12px; margin-top: 8px; font-size: 12px;
                   color: #374151; white-space: pre-wrap; line-height: 1.6; }}
  .footer {{ background: #f9f9f9; border-top: 1px solid #eee;
             padding: 14px 28px; font-size: 12px; color: #888; }}
  .stats {{ display: flex; gap: 20px; margin-bottom: 16px; }}
  .stat-box {{ background: #f0f4ff; border-radius: 6px; padding: 10px 16px; flex: 1; text-align: center; }}
  .stat-num {{ font-size: 24px; font-weight: 700; color: #1d4ed8; }}
  .stat-label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.05em; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Job Radar &mdash; Data Roles Alert</h1>
    <p>{timestamp} &mdash; {mode} mode &mdash; {candidate_name}</p>
  </div>
  <div class="section">
    <div class="stats">
      <div class="stat-box"><div class="stat-num">{yes_count}</div><div class="stat-label">Strong Match</div></div>
      <div class="stat-box"><div class="stat-num">{maybe_count}</div><div class="stat-label">Review Needed</div></div>
      <div class="stat-box"><div class="stat-num">{total_count}</div><div class="stat-label">Total New</div></div>
    </div>
    {profile_line}
    {yes_section}
    {maybe_section}
  </div>
  <div class="footer">
    Powered by Job Radar &mdash; targeting Data Analyst · Data Scientist · Data Engineer
    {error_section}
  </div>
</div>
</body>
</html>
"""

_JOB_CARD = """\
<div class="job-card job-card-{label}">
  {ghost_banner}
  <p style="margin:0 0 6px;">{freshness_badge}</p>
  <p class="job-title">{company} &mdash; {title}
    <span class="score-badge badge-{label}">Score {score}</span>{work_type_badge}{match_badge}
  </p>
  <p class="job-meta">{location} &middot; {posted_friendly}{salary_line}</p>
  {highlights_section}
  {dm_section}
  <a class="apply-btn" href="{url}" target="_blank">&#9889; APPLY NOW &rarr;</a>
</div>"""

_SECTION = """\
<div class="section-title {cls}">{heading} ({count})</div>
{cards}"""


def _build_html(yes_jobs: list[Job], maybe_jobs: list[Job], mode: str, source_errors: list[str] | None = None) -> str:
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    candidate_name = PROFILE["name"]
    profile_line = profile_summary_html()

    def _card(job: Job) -> str:
        import html as _html
        # Freshness
        freshness_badge = _freshness_badge_html(job.posted)
        posted_str      = _posted_friendly(job.posted)

        # Work-type badge
        wt = (job.work_type or "").strip()
        if wt == "Remote":
            work_type_badge = '<span class="badge-remote">&#127968; Remote</span>'
        elif wt == "Hybrid":
            work_type_badge = '<span class="badge-hybrid">&#9681; Hybrid</span>'
        elif wt == "Onsite":
            work_type_badge = '<span class="badge-onsite">&#127970; Onsite</span>'
        else:
            work_type_badge = ""

        # Resume match badge + track indicator
        rm    = getattr(job, "resume_match", 0)
        track = getattr(job, "resume_track", "")
        track_label = {"de": "DE resume", "ai": "AI resume"}.get(track, "")
        track_suffix = f" · {track_label}" if track_label else ""
        if rm >= 70:
            match_badge = f'<span class="badge-match">&#9989; {rm}%{track_suffix}</span>'
        elif rm >= 45:
            match_badge = f'<span class="badge-match" style="background:#fef9c3;color:#a16207;">&#128993; {rm}%{track_suffix}</span>'
        elif rm > 0:
            match_badge = f'<span class="badge-match" style="background:#f3f4f6;color:#6b7280;">{rm}%{track_suffix}</span>'
        else:
            match_badge = ""

        # Salary line
        salary_line = (
            f' &middot; <span class="salary-line">&#128176; {job.salary}</span>'
            if job.salary else ""
        )

        # Ghost warning banner
        ghost_level   = getattr(job, "ghost_level",   "")
        ghost_reasons = getattr(job, "ghost_reasons",  [])
        if ghost_level == "suspicious":
            reasons_text = " &bull; ".join(_html.escape(r) for r in ghost_reasons)
            ghost_banner = (
                f'<div class="ghost-suspicious">&#9888; <strong>Suspicious posting</strong> — '
                f'{reasons_text}</div>'
            )
        elif ghost_level == "caution":
            reasons_text = " &bull; ".join(_html.escape(r) for r in ghost_reasons)
            ghost_banner = (
                f'<div class="ghost-caution">&#128993; <strong>Caution</strong> — '
                f'{reasons_text}</div>'
            )
        else:
            ghost_banner = ""

        # Resume highlights — "lead with these on your application"
        bullets = getattr(job, "top_bullets", [])
        if bullets:
            items = "\n    ".join(
                f"<li>{_html.escape(b)}</li>" for b in bullets
            )
            highlights_section = (
                '<div class="highlights-box">'
                '<p class="highlights-title">&#128204; Lead with these on your application</p>'
                f'<ul>\n    {items}\n  </ul>'
                '</div>'
            )
        else:
            highlights_section = ""

        # LinkedIn DM — ready to copy
        dm = getattr(job, "linkedin_dm", "")
        if dm and job.label == "yes":  # only for strong matches
            dm_section = (
                '<div class="dm-box">'
                '<p class="dm-title">&#128172; LinkedIn DM &mdash; copy &amp; send</p>'
                f'<p class="dm-text">{_html.escape(dm)}</p>'
                '</div>'
            )
        else:
            dm_section = ""

        return _JOB_CARD.format(
            label=job.label,
            company=_html.escape(job.company),
            title=_html.escape(job.title),
            score=job.score,
            location=_html.escape(job.location),
            freshness_badge=freshness_badge,
            posted_friendly=posted_str,
            salary_line=salary_line,
            work_type_badge=work_type_badge,
            match_badge=match_badge,
            ghost_banner=ghost_banner,
            highlights_section=highlights_section,
            dm_section=dm_section,
            url=job.url,
        )

    yes_section = ""
    if yes_jobs:
        yes_section = _SECTION.format(
            cls="yes-title", heading="Strong Matches", count=len(yes_jobs),
            cards="\n".join(_card(j) for j in yes_jobs),
        )

    maybe_section = ""
    if maybe_jobs:
        maybe_section = _SECTION.format(
            cls="maybe-title", heading="Review Needed", count=len(maybe_jobs),
            cards="\n".join(_card(j) for j in maybe_jobs),
        )

    # Error digest footer
    error_section = ""
    if source_errors:
        names = ", ".join(source_errors[:5])
        error_section = (
            f'<br><span style="color:#dc2626;">&#9888; {len(source_errors)} source(s) failed this run: {names}</span>'
        )

    return _HTML_TEMPLATE.format(
        timestamp=ts, mode=mode, candidate_name=candidate_name,
        yes_count=len(yes_jobs), maybe_count=len(maybe_jobs),
        total_count=len(yes_jobs) + len(maybe_jobs),
        profile_line=profile_line,
        yes_section=yes_section, maybe_section=maybe_section,
        error_section=error_section,
    )


def _build_followup_html(pending: list[dict]) -> str:
    """Build an HTML reminder email for jobs applied to but not followed up on."""
    import html as _html
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cards = []
    for p in pending:
        company   = _html.escape(p.get("company", ""))
        title     = _html.escape(p.get("title", ""))
        url       = p.get("url", "#")
        location  = _html.escape(p.get("location", ""))
        wt        = _html.escape(p.get("work_type", ""))
        applied   = p.get("applied_at", "")
        h_ago     = _hours_ago(applied)
        days_ago  = int(h_ago / 24) if h_ago else 0
        applied_friendly = f"{days_ago} days ago" if days_ago else applied[:10]
        follow_msg = (
            f"Hi [Hiring Manager],\n\n"
            f"I wanted to follow up on my application for the {p.get('title','')} "
            f"position at {p.get('company','')}.\n\n"
            f"I remain very interested in this role and would love to discuss how my "
            f"experience in data analytics and business intelligence can contribute "
            f"to your team.\n\n"
            f"Please let me know if you need any additional information.\n\n"
            f"Best regards,\nRohith Bayya"
        )
        wt_str = f" &middot; {wt}" if wt else ""
        cards.append(
            f'<div class="fu-card">'
            f'<p class="fu-title">{company} &mdash; {title}</p>'
            f'<p class="fu-meta">{location}{wt_str} &middot; Applied <strong>{applied_friendly}</strong></p>'
            f'<a href="{url}" target="_blank" style="font-size:13px;color:#1d4ed8;">View Posting &rarr;</a>'
            f'<p class="fu-template">{_html.escape(follow_msg)}</p>'
            f'</div>'
        )
    cards_html = "\n".join(cards)
    return f"""\
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
          background:#f5f5f5; margin:0; padding:20px; color:#333; }}
  .container {{ max-width:700px; margin:0 auto; background:#fff;
                border-radius:8px; overflow:hidden;
                box-shadow:0 2px 8px rgba(0,0,0,0.1); }}
  .header {{ background:#92400e; color:#fff; padding:20px 28px; }}
  .header h1 {{ margin:0; font-size:20px; }}
  .header p  {{ margin:4px 0 0; font-size:13px; color:#fde68a; }}
  .section {{ padding:20px 28px; }}
  .fu-card {{ background:#fafafa; border-left:4px solid #f59e0b;
              border-radius:6px; padding:14px 16px; margin-bottom:14px; }}
  .fu-title {{ font-size:15px; font-weight:600; margin:0 0 4px; }}
  .fu-meta  {{ font-size:13px; color:#666; margin:0 0 8px; }}
  .fu-template {{ background:#fff; border:1px solid #e5e7eb; border-radius:4px;
                   padding:10px 12px; margin-top:8px; font-size:12px;
                   color:#374151; white-space:pre-wrap; line-height:1.6; }}
  .footer {{ background:#f9f9f9; border-top:1px solid #eee;
             padding:14px 28px; font-size:12px; color:#888; }}
</style></head><body>
<div class="container">
  <div class="header">
    <h1>&#9200; Follow-up Reminders</h1>
    <p>{now_str} &mdash; {len(pending)} application(s) waiting for a follow-up</p>
  </div>
  <div class="section">
    <p style="font-size:14px;color:#374151;margin:0 0 16px;">
      You applied to these roles <strong>7+ days ago</strong> with no response recorded.
      A brief follow-up email can move you to the top of the pile. Copy the template below each listing.
    </p>
    {cards_html}
  </div>
  <div class="footer">Job Radar &mdash; Follow-up Reminder &mdash; Mark a job as
    <code>--followed-up</code> to stop seeing it here.</div>
</div></body></html>"""


def _build_plaintext(yes_jobs: list[Job], maybe_jobs: list[Job], source_errors: list[str] | None = None) -> str:
    lines: list[str] = []
    def _txt_job(j: Job) -> list[str]:
        posted = _posted_friendly(j.posted)
        fresh  = _hours_ago(j.posted)
        fresh_tag = " 🔥 JUST POSTED" if fresh is not None and fresh < 2 else (
                    " ⚡ < 6 HRS"     if fresh is not None and fresh < 6 else "")
        wt  = f" | {j.work_type}" if j.work_type else ""
        sal = f" | {j.salary}"    if j.salary    else ""
        rm  = getattr(j, "resume_match", 0)
        match_str = f" | Match {rm}%" if rm > 0 else ""
        return [
            f"[{j.company}] {j.title}{fresh_tag}",
            f"  {j.location}{wt} | Posted: {posted}{sal}{match_str}",
            f"  Score: {j.score}  {j.url}",
            "",
        ]

    if yes_jobs:
        lines.append(f"=== STRONG MATCHES ({len(yes_jobs)}) ===\n")
        for j in yes_jobs:
            lines.extend(_txt_job(j))
    if maybe_jobs:
        lines.append(f"\n=== REVIEW NEEDED ({len(maybe_jobs)}) ===\n")
        for j in maybe_jobs:
            lines.extend(_txt_job(j))
    if source_errors:
        lines.append(f"\n⚠ {len(source_errors)} source(s) failed: {', '.join(source_errors[:5])}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Base notifier
# ---------------------------------------------------------------------------

class BaseNotifier(ABC):
    @abstractmethod
    def notify(self, yes_jobs: list[Job], maybe_jobs: list[Job], *, subject_prefix: str, mode: str, source_errors: list[str] | None = None) -> None:
        ...


# ---------------------------------------------------------------------------
# Email notifier
# ---------------------------------------------------------------------------

class EmailNotifier(BaseNotifier):
    def __init__(self, user: str, password: str, to: str, smtp_host: str = "smtp.gmail.com", smtp_port: int = 587) -> None:
        self.user = user
        self.password = (password or "").replace(" ", "")
        # Support multiple recipients: comma-separated string or list
        if isinstance(to, list):
            self.recipients = [r.strip() for r in to if r.strip()]
        else:
            self.recipients = [r.strip() for r in (to or "").split(",") if r.strip()]
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port

    def is_configured(self) -> bool:
        return bool(self.user and self.password and self.recipients)

    def notify(self, yes_jobs: list[Job], maybe_jobs: list[Job], *, subject_prefix: str = "[Job Radar]", mode: str = "main", source_errors: list[str] | None = None) -> None:
        if not self.is_configured():
            log.warning("Email not configured; skipping.")
            return

        all_jobs = yes_jobs + maybe_jobs
        companies = sorted({j.company for j in all_jobs if j.company})
        company_str = ", ".join(companies[:4]) + ("…" if len(companies) > 4 else "")
        fresh_jobs = [j for j in all_jobs if (_hours_ago(j.posted) or 999) <= 4]
        if fresh_jobs:
            subject = (
                f"🔥 {len(fresh_jobs)} fresh job{'s' if len(fresh_jobs) > 1 else ''} — "
                f"{len(yes_jobs)} match + {len(maybe_jobs)} review | {company_str}"
            )
        else:
            subject = f"{subject_prefix} {len(yes_jobs)} match + {len(maybe_jobs)} review — {company_str}"

        html_body = _build_html(yes_jobs, maybe_jobs, mode, source_errors)
        text_body = _build_plaintext(yes_jobs, maybe_jobs, source_errors)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.user
        msg["To"] = ", ".join(self.recipients)   # shows all recipients in email header
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        # Use certifi CA bundle when available (fixes macOS SSL cert issue)
        try:
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            ctx = ssl.create_default_context()

        if self.smtp_port == 465:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=ctx) as server:
                server.login(self.user, self.password)
                server.sendmail(self.user, self.recipients, msg.as_string())
        else:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.ehlo()
                server.starttls(context=ctx)
                server.ehlo()
                server.login(self.user, self.password)
                server.sendmail(self.user, self.recipients, msg.as_string())

        log.info("Email sent: %d yes + %d maybe to %s", len(yes_jobs), len(maybe_jobs), ", ".join(self.recipients))

    def notify_followup(self, pending: list[dict]) -> None:
        """Send a weekly follow-up reminder email for stale applications."""
        if not self.is_configured():
            log.warning("Email not configured; skipping follow-up reminder.")
            return
        if not pending:
            log.info("No follow-ups due — nothing to send.")
            return

        subject = (
            f"⏰ Follow-up reminder — {len(pending)} application"
            f"{'s' if len(pending) > 1 else ''} waiting for a response"
        )
        html_body = _build_followup_html(pending)
        text_body = "\n".join(
            f"[{p['company']}] {p['title']} — applied {p.get('applied_at','')[:10]}  {p['url']}"
            for p in pending
        )

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.user
        msg["To"]      = ", ".join(self.recipients)
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        try:
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            ctx = ssl.create_default_context()

        if self.smtp_port == 465:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=ctx) as server:
                server.login(self.user, self.password)
                server.sendmail(self.user, self.recipients, msg.as_string())
        else:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.ehlo(); server.starttls(context=ctx); server.ehlo()
                server.login(self.user, self.password)
                server.sendmail(self.user, self.recipients, msg.as_string())

        log.info("Follow-up reminder sent: %d application(s) to %s", len(pending), ", ".join(self.recipients))


# ---------------------------------------------------------------------------
# Slack notifier
# ---------------------------------------------------------------------------

class SlackNotifier(BaseNotifier):
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def is_configured(self) -> bool:
        return bool(self.webhook_url)

    def notify(self, yes_jobs: list[Job], maybe_jobs: list[Job], *, subject_prefix: str = "[Job Radar]", mode: str = "main", source_errors: list[str] | None = None) -> None:
        if not self.is_configured():
            return

        blocks: list[dict] = [
            {"type": "header", "text": {"type": "plain_text", "text": f"{subject_prefix} — {len(yes_jobs)} match + {len(maybe_jobs)} review"}},
            {"type": "divider"},
        ]

        def _job_block(job: Job, emoji: str) -> dict:
            posted = f" · {job.posted}" if job.posted else ""
            return {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{emoji} *<{job.url}|{job.title}>*\n{job.company} · {job.location}{posted} · Score: `{job.score}`",
                },
            }

        if yes_jobs:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*:white_check_mark: Strong Matches ({len(yes_jobs)})*"}})
            for j in yes_jobs[:10]:
                blocks.append(_job_block(j, ":green_circle:"))

        if maybe_jobs:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*:eyes: Review Needed ({len(maybe_jobs)})*"}})
            for j in maybe_jobs[:10]:
                blocks.append(_job_block(j, ":yellow_circle:"))

        payload = {"blocks": blocks}
        r = requests.post(self.webhook_url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("Slack notification sent.")


# ---------------------------------------------------------------------------
# Discord notifier
# ---------------------------------------------------------------------------

class DiscordNotifier(BaseNotifier):
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def is_configured(self) -> bool:
        return bool(self.webhook_url)

    def notify(self, yes_jobs: list[Job], maybe_jobs: list[Job], *, subject_prefix: str = "[Job Radar]", mode: str = "main", source_errors: list[str] | None = None) -> None:
        if not self.is_configured():
            return

        embeds: list[dict] = []

        def _embed(job: Job, color: int) -> dict:
            posted = f"\n📅 {job.posted}" if job.posted else ""
            return {
                "title": job.title,
                "url": job.url,
                "description": f"**{job.company}** · {job.location}{posted}\nScore: **{job.score}**",
                "color": color,
            }

        for j in yes_jobs[:5]:
            embeds.append(_embed(j, 0x16A34A))  # green
        for j in maybe_jobs[:5]:
            embeds.append(_embed(j, 0xD97706))  # amber

        payload = {
            "content": f"**{subject_prefix}** — {len(yes_jobs)} strong match + {len(maybe_jobs)} to review",
            "embeds": embeds,
        }
        r = requests.post(self.webhook_url, json=payload, timeout=15)
        r.raise_for_status()
        log.info("Discord notification sent.")


# ---------------------------------------------------------------------------
# Composite notifier — dispatches to all configured channels
# ---------------------------------------------------------------------------

class CompositeNotifier:
    def __init__(self, notifiers: list[BaseNotifier]) -> None:
        self._notifiers = notifiers

    def notify(self, yes_jobs: list[Job], maybe_jobs: list[Job], *, subject_prefix: str = "[Job Radar]", mode: str = "main", source_errors: list[str] | None = None) -> list[str]:
        """Send to all notifiers, collecting errors instead of raising."""
        errors: list[str] = []
        for notifier in self._notifiers:
            try:
                notifier.notify(yes_jobs, maybe_jobs, subject_prefix=subject_prefix, mode=mode, source_errors=source_errors)
            except Exception as exc:
                log.error("Notifier %s failed: %s", type(notifier).__name__, exc)
                errors.append(f"{type(notifier).__name__}: {exc}")
        return errors
