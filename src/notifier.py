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
import smtplib
import ssl
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests

from .sources.base import Job
from .profile import PROFILE, profile_summary_html, profile_summary_text

log = logging.getLogger(__name__)

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
  <p class="job-title">{company} &mdash; {title}
    <span class="score-badge badge-{label}">Score {score}</span>{work_type_badge}
  </p>
  <p class="job-meta">{location}{posted_line}{salary_line}</p>
  <a class="job-link" href="{url}" target="_blank">View Job &rarr;</a>
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
        posted_line = f" &middot; {job.posted}" if job.posted else ""
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
        # Salary line
        salary_line = (
            f' &middot; <span class="salary-line">&#128176; {job.salary}</span>'
            if job.salary else ""
        )
        return _JOB_CARD.format(
            label=job.label,
            company=job.company,
            title=job.title,
            score=job.score,
            location=job.location,
            posted_line=posted_line,
            salary_line=salary_line,
            work_type_badge=work_type_badge,
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


def _build_plaintext(yes_jobs: list[Job], maybe_jobs: list[Job], source_errors: list[str] | None = None) -> str:
    lines: list[str] = []
    def _txt_job(j: Job) -> list[str]:
        posted = f" | {j.posted}" if j.posted else ""
        wt = f" | {j.work_type}" if j.work_type else ""
        sal = f" | {j.salary}" if j.salary else ""
        return [
            f"[{j.company}] {j.title} | {j.location}{wt}{posted}{sal}",
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
