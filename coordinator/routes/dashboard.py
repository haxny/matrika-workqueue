"""Minimal HTML dashboard for workqueue status."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = request.app.state.db
    stats = db.get_stats()
    workers = db.get_workers()
    activity = db.get_recent_activity(limit=20)

    # Cooldown info
    cooldowns = db.conn.execute(
        "SELECT domain, cooldown_until FROM rate_limits WHERE cooldown_until IS NOT NULL"
    ).fetchall()
    cooldown_html = ""
    from datetime import datetime
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for domain, until in cooldowns:
        if until > now_str:
            cooldown_html += (
                f'<div class="stat" style="border-left:4px solid #ef4444">'
                f'<div class="num" style="font-size:16px;color:#ef4444">{domain}</div>'
                f'<div class="label">Cooldown until {until} UTC</div></div>'
            )

    rows_workers = ""
    for w in workers:
        seen = w.last_seen_at or "never"
        mb_today = db.get_worker_bytes_today(w.worker_id) / 1024 / 1024
        mb_limit = 488
        pct = min(mb_today / mb_limit * 100, 100)
        bar_color = "#22c55e" if pct < 75 else ("#eab308" if pct < 95 else "#ef4444")
        bytes_cell = (
            f'<div style="background:#eee;border-radius:3px;height:18px;width:100px;display:inline-block">'
            f'<div style="background:{bar_color};height:100%;width:{pct:.0f}%;border-radius:3px"></div>'
            f'</div> {mb_today:.0f}/{mb_limit} MB'
        )
        rows_workers += (
            f"<tr><td>{w.worker_id}</td><td>{w.capabilities}</td>"
            f"<td>{seen}</td><td>{w.last_ip or '-'}</td>"
            f"<td>{w.tasks_completed}</td><td>{w.tasks_failed}</td>"
            f"<td>{bytes_cell}</td>"
            f"<td>{'active' if w.is_active else 'inactive'}</td></tr>\n"
        )

    rows_activity = ""
    for a in activity:
        rows_activity += (
            f"<tr><td>{a['timestamp']}</td><td>{a['worker_id'] or '-'}</td>"
            f"<td>{a['action']}</td><td>{a['details']}</td></tr>\n"
        )

    by_type_rows = ""
    for tt, statuses in stats.get("by_type", {}).items():
        pending = statuses.get("pending", 0)
        assigned = statuses.get("assigned", 0)
        completed = statuses.get("completed", 0)
        failed = statuses.get("failed", 0)
        by_type_rows += (
            f"<tr><td>{tt}</td><td>{pending}</td><td>{assigned}</td>"
            f"<td>{completed}</td><td>{failed}</td></tr>\n"
        )

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Workqueue Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="30">
<style>
body {{ font-family: -apple-system, system-ui, sans-serif; margin: 2em; background: #f5f5f5; }}
h1 {{ color: #333; }}
h2 {{ color: #555; margin-top: 1.5em; }}
table {{ border-collapse: collapse; width: 100%; margin: 0.5em 0; background: #fff; }}
th, td {{ padding: 6px 12px; border: 1px solid #ddd; text-align: left; font-size: 14px; }}
th {{ background: #f0f0f0; }}
.stat {{ display: inline-block; background: #fff; padding: 12px 20px; margin: 4px;
         border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.stat .num {{ font-size: 24px; font-weight: bold; color: #2563eb; }}
.stat .label {{ font-size: 12px; color: #888; }}
</style>
</head>
<body>
<h1>Workqueue Dashboard</h1>

<div>
<div class="stat"><div class="num">{stats.get('total', 0)}</div><div class="label">Total</div></div>
<div class="stat"><div class="num">{stats.get('pending', 0)}</div><div class="label">Pending</div></div>
<div class="stat"><div class="num">{stats.get('assigned', 0)}</div><div class="label">Assigned</div></div>
<div class="stat"><div class="num">{stats.get('completed', 0)}</div><div class="label">Completed</div></div>
<div class="stat"><div class="num">{stats.get('failed', 0)}</div><div class="label">Failed</div></div>
<div class="stat"><div class="num">{stats.get('completed_last_hour', 0)}</div><div class="label">Last Hour</div></div>
{cooldown_html}
</div>

<h2>By Task Type</h2>
<table>
<tr><th>Type</th><th>Pending</th><th>Assigned</th><th>Completed</th><th>Failed</th></tr>
{by_type_rows or '<tr><td colspan="5">No tasks yet</td></tr>'}
</table>

<h2>Workers</h2>
<table>
<tr><th>ID</th><th>Capabilities</th><th>Last Seen</th><th>IP</th><th>Completed</th><th>Failed</th><th>Today (488 MB limit)</th><th>Status</th></tr>
{rows_workers or '<tr><td colspan="8">No workers registered</td></tr>'}
</table>

<h2>Recent Activity</h2>
<table>
<tr><th>Time</th><th>Worker</th><th>Action</th><th>Details</th></tr>
{rows_activity or '<tr><td colspan="4">No activity yet</td></tr>'}
</table>

<h2>Help Download Pages</h2>
<div style="background:#fff;padding:16px 20px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.1);line-height:1.6">

<h3 style="margin-top:0">Desktop (Windows / Mac / Linux)</h3>
<p>Install the Chrome extension to download parish register pages using your browser and IP address.</p>

<p><strong>Windows:</strong></p>
<pre style="background:#f0f0f0;padding:8px 12px;border-radius:4px;overflow-x:auto">curl -sL https://g.book.cz/install-mza.bat -o %TEMP%\\install-mza.bat &amp;&amp; %TEMP%\\install-mza.bat</pre>
<p style="font-size:13px;color:#666">Paste into Command Prompt (Win+R &rarr; cmd). Downloads the extension, then follow 3 steps in Chrome.</p>

<p><strong>Mac / Linux:</strong></p>
<pre style="background:#f0f0f0;padding:8px 12px;border-radius:4px;overflow-x:auto">curl -sL https://g.book.cz/install-mza.sh | bash</pre>

<p><strong>Manual install:</strong> Download <a href="https://g.book.cz/mza-helper-extension.zip">mza-helper-extension.zip</a>,
unzip, open <code>chrome://extensions</code>, enable Developer mode, click Load unpacked, select the folder.</p>
<p style="font-size:13px;color:#666">Works on Chrome, Edge, Brave. The extension auto-starts &mdash; no configuration needed.</p>

<h3>Android (Termux)</h3>
<p>Install <a href="https://f-droid.org/en/packages/com.termux/">Termux</a> from F-Droid, then run:</p>
<pre style="background:#f0f0f0;padding:8px 12px;border-radius:4px;overflow-x:auto">pkg install python curl &amp;&amp; curl -sL https://g.book.cz/mza-worker.py | python</pre>
<p style="font-size:13px;color:#666">Uses your phone's mobile data IP. Runs in the background while Termux is open.</p>

</div>

<p style="color:#999; font-size:12px;">Auto-refreshes every 30s</p>
</body>
</html>"""
    return HTMLResponse(content=html)
