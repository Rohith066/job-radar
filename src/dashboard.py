"""Simple local feedback dashboard — no extra dependencies needed.

Run with:
    python3 -m src.main --dashboard

Opens http://localhost:5100 in your browser showing recent YES/MAYBE jobs.
Click ✅ Applied / 🔖 Interested / ❌ Dismiss buttons to record feedback.
Uses Python's built-in http.server — zero extra packages required.
"""
from __future__ import annotations

import json
import logging
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)

PORT = 5100
_db = None   # set by run_dashboard()


# ---------------------------------------------------------------------------
# HTML page
# ---------------------------------------------------------------------------

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Job Radar — Feedback</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
          background: #f0f4ff; min-height: 100vh; padding: 24px 16px; color: #1a1a2e; }}
  .header {{ max-width: 860px; margin: 0 auto 24px;
             background: #1a1a2e; color: #fff; border-radius: 10px;
             padding: 20px 28px; display: flex; align-items: center; justify-content: space-between; }}
  .header h1 {{ font-size: 20px; }}
  .header p  {{ font-size: 13px; color: #aaa; margin-top: 4px; }}
  .stats {{ display: flex; gap: 12px; }}
  .stat {{ background: rgba(255,255,255,.1); border-radius: 8px;
           padding: 8px 16px; text-align: center; }}
  .stat-num {{ font-size: 22px; font-weight: 700; }}
  .stat-lbl {{ font-size: 11px; color: #aaa; text-transform: uppercase; }}
  .container {{ max-width: 860px; margin: 0 auto; }}
  .section-head {{ font-size: 13px; font-weight: 700; text-transform: uppercase;
                   letter-spacing: .06em; padding: 8px 0; margin: 20px 0 10px;
                   border-bottom: 2px solid; }}
  .yes-head   {{ color: #16a34a; border-color: #16a34a; }}
  .maybe-head {{ color: #d97706; border-color: #d97706; }}
  .card {{ background: #fff; border-radius: 10px; padding: 16px 20px;
           margin-bottom: 12px; border-left: 4px solid #e5e7eb;
           box-shadow: 0 1px 4px rgba(0,0,0,.06); display: flex;
           justify-content: space-between; align-items: flex-start; gap: 16px; }}
  .card.yes   {{ border-color: #16a34a; }}
  .card.maybe {{ border-color: #d97706; }}
  .card.done  {{ opacity: .45; }}
  .job-info {{ flex: 1; min-width: 0; }}
  .job-title {{ font-size: 15px; font-weight: 600; margin-bottom: 4px;
                white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  .job-meta  {{ font-size: 13px; color: #666; margin-bottom: 8px; }}
  .job-link  {{ font-size: 13px; color: #1d4ed8; text-decoration: none; font-weight: 500; }}
  .job-link:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; font-size: 11px; font-weight: 600;
            border-radius: 999px; padding: 2px 8px; margin-left: 6px; vertical-align: middle; }}
  .badge-score  {{ background: #dbeafe; color: #1e40af; }}
  .badge-remote {{ background: #dbeafe; color: #1e40af; }}
  .badge-hybrid {{ background: #ede9fe; color: #5b21b6; }}
  .badge-onsite {{ background: #fee2e2; color: #991b1b; }}
  .actions {{ display: flex; flex-direction: column; gap: 8px; min-width: 120px; }}
  .btn {{ border: none; border-radius: 8px; padding: 8px 12px; cursor: pointer;
          font-size: 13px; font-weight: 600; width: 100%; transition: opacity .15s; }}
  .btn:hover   {{ opacity: .8; }}
  .btn-applied {{ background: #dcfce7; color: #15803d; }}
  .btn-interested {{ background: #ede9fe; color: #5b21b6; }}
  .btn-dismiss {{ background: #fee2e2; color: #991b1b; }}
  .feedback-done {{ font-size: 12px; color: #888; text-align: center; margin-top: 4px; }}
  .empty {{ text-align: center; color: #888; padding: 48px; font-size: 15px; }}
  .toast {{ position: fixed; bottom: 24px; right: 24px; background: #1a1a2e; color: #fff;
            padding: 12px 20px; border-radius: 8px; font-size: 14px; font-weight: 500;
            opacity: 0; transition: opacity .3s; pointer-events: none; z-index: 999; }}
  .toast.show {{ opacity: 1; }}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>🎯 Job Radar — Feedback Dashboard</h1>
    <p>Click buttons to record feedback · Closes when you press Ctrl+C in the terminal</p>
  </div>
  <div class="stats">
    <div class="stat"><div class="stat-num" id="cnt-applied">-</div><div class="stat-lbl">Applied</div></div>
    <div class="stat"><div class="stat-num" id="cnt-interested">-</div><div class="stat-lbl">Saved</div></div>
    <div class="stat"><div class="stat-num" id="cnt-dismissed">-</div><div class="stat-lbl">Dismissed</div></div>
  </div>
</div>

<div class="container" id="app">
  <div class="empty">Loading jobs…</div>
</div>

<div class="toast" id="toast"></div>

<script>
const API = '';

function badge(text, cls) {{
  return text ? `<span class="badge ${{cls}}">${{text}}</span>` : '';
}}

function workBadge(wt) {{
  if (!wt) return '';
  const map = {{ Remote:'badge-remote', Hybrid:'badge-hybrid', Onsite:'badge-onsite' }};
  return badge(wt, map[wt] || '');
}}

function showToast(msg) {{
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}}

function loadStats() {{
  fetch('/api/stats').then(r => r.json()).then(s => {{
    document.getElementById('cnt-applied').textContent    = s.applied    ?? 0;
    document.getElementById('cnt-interested').textContent = s.interested ?? 0;
    document.getElementById('cnt-dismissed').textContent  = s.dismissed  ?? 0;
  }});
}}

function recordFeedback(jobKey, action, card) {{
  fetch('/api/feedback', {{
    method: 'POST',
    headers: {{ 'Content-Type': 'application/json' }},
    body: JSON.stringify({{ job_key: jobKey, action }})
  }}).then(r => r.json()).then(d => {{
    if (d.ok) {{
      card.classList.add('done');
      const labels = {{ applied:'✅ Applied', interested:'🔖 Saved', dismissed:'❌ Dismissed' }};
      card.querySelector('.actions').innerHTML =
        `<div class="feedback-done">${{labels[action] || action}}</div>`;
      showToast(d.message || 'Saved!');
      loadStats();
    }}
  }});
}}

function renderJobs(jobs) {{
  const app = document.getElementById('app');
  if (!jobs.length) {{
    app.innerHTML = '<div class="empty">No YES or MAYBE jobs in the database yet.<br>Run a fetch first, then come back here.</div>';
    return;
  }}

  const yes   = jobs.filter(j => j.label === 'yes');
  const maybe = jobs.filter(j => j.label === 'maybe');

  let html = '';

  function section(list, headCls, headText) {{
    if (!list.length) return '';
    let cards = list.map(j => {{
      const scoreBadge = badge('Score ' + j.score, 'badge-score');
      const wt         = workBadge(j.work_type || '');
      const sal        = j.salary ? `<span style="color:#15803d;font-weight:600;margin-left:6px;">💰 ${{j.salary}}</span>` : '';
      const posted     = j.posted ? ` · ${{j.posted}}` : '';
      const fb         = j.feedback ? `<div class="feedback-done">${{
        {{applied:'✅ Applied', interested:'🔖 Saved', dismissed:'❌ Dismissed'}}[j.feedback] || j.feedback
      }}</div>` : `
        <button class="btn btn-applied"     onclick="recordFeedback('${{j.key}}','applied',this.closest('.card'))">✅ Applied</button>
        <button class="btn btn-interested"  onclick="recordFeedback('${{j.key}}','interested',this.closest('.card'))">🔖 Save</button>
        <button class="btn btn-dismiss"     onclick="recordFeedback('${{j.key}}','dismissed',this.closest('.card'))">❌ Dismiss</button>`;
      return `
      <div class="card ${{j.label}}${{j.feedback ? ' done' : ''}}">
        <div class="job-info">
          <div class="job-title">${{j.company}} — ${{j.title}} ${{scoreBadge}}${{wt}}</div>
          <div class="job-meta">${{j.location}}${{posted}}${{sal}}</div>
          <a class="job-link" href="${{j.url}}" target="_blank">View Job →</a>
        </div>
        <div class="actions">${{fb}}</div>
      </div>`;
    }}).join('');
    return `<div class="section-head ${{headCls}}">${{headText}} (${{list.length}})</div>${{cards}}`;
  }}

  html += section(yes,   'yes-head',   '✅ Strong Matches');
  html += section(maybe, 'maybe-head', '⚠️ Review Needed');
  app.innerHTML = html;
}}

// Initial load
fetch('/api/jobs').then(r => r.json()).then(renderJobs);
loadStats();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress request logging noise

    def _send(self, code: int, content_type: str, body: str | bytes) -> None:
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/" or parsed.path == "/index.html":
            self._send(200, "text/html; charset=utf-8", _HTML)

        elif parsed.path == "/api/jobs":
            jobs = _get_recent_jobs()
            self._send(200, "application/json", json.dumps(jobs))

        elif parsed.path == "/api/stats":
            stats = _db.get_feedback_stats() if _db else {}
            self._send(200, "application/json", json.dumps(stats))

        else:
            self._send(404, "text/plain", "Not found")

    def do_POST(self):
        if self.path == "/api/feedback":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            job_key = body.get("job_key", "")
            action  = body.get("action", "")

            if not job_key or action not in ("applied", "dismissed", "interested"):
                self._send(400, "application/json", json.dumps({"ok": False, "message": "Invalid input"}))
                return

            try:
                _db.record_feedback(job_key, action)
                emoji = {"applied": "✅ Applied", "dismissed": "❌ Dismissed", "interested": "🔖 Saved"}
                self._send(200, "application/json", json.dumps({
                    "ok": True,
                    "message": f"{emoji.get(action, action)} — saved!",
                }))
            except Exception as e:
                self._send(500, "application/json", json.dumps({"ok": False, "message": str(e)}))
        else:
            self._send(404, "text/plain", "Not found")


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _get_recent_jobs() -> list[dict]:
    """Return recent YES/MAYBE jobs with any existing feedback."""
    if not _db:
        return []

    rows = _db._conn.execute(
        """SELECT j.key, j.company, j.title, j.url, j.location,
                  j.posted, j.score, j.label, j.work_type, j.salary,
                  f.action as feedback
           FROM jobs j
           LEFT JOIN (
               SELECT job_key, action FROM feedback
               GROUP BY job_key ORDER BY created_at DESC
           ) f ON f.job_key = j.key
           WHERE j.label IN ('yes', 'maybe')
             AND j.first_seen >= datetime('now', '-14 days')
           ORDER BY j.label DESC, j.score DESC
           LIMIT 200"""
    ).fetchall()

    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_dashboard(db) -> None:
    """Start the feedback dashboard and open the browser."""
    global _db
    _db = db

    server = HTTPServer(("127.0.0.1", PORT), _Handler)
    url = f"http://localhost:{PORT}"

    print(f"\n{'='*55}")
    print(f"  🎯  Job Radar Feedback Dashboard")
    print(f"  📌  {url}")
    print(f"  ℹ️   Press Ctrl+C to stop")
    print(f"{'='*55}\n")

    # Open browser after a short delay so server is ready
    threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n✅ Dashboard stopped.")
    finally:
        server.server_close()
