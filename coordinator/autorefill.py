"""Auto-refill: keep the task queue fed from mza.db pending pages.

Runs as a background asyncio task inside the coordinator.
Reads mza.db (read-only) and pushes tasks into the workqueue DB
whenever pending task count drops below a threshold.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger("workqueue.autorefill")

# Env config
MZA_DB_PATH = os.environ.get("MZA_DB_PATH", "/mza_data/mza.db")
REFILL_INTERVAL = int(os.environ.get("WQ_REFILL_INTERVAL", "3600"))  # seconds (1 hour)
REFILL_THRESHOLD = int(os.environ.get("WQ_REFILL_THRESHOLD", "200"))  # refill when pending < this
REFILL_BATCH = int(os.environ.get("WQ_REFILL_BATCH", "500"))  # how many to push per cycle


def _get_mza_conn() -> Optional[sqlite3.Connection]:
    """Open mza.db read-only. Returns None if not available."""
    path = Path(MZA_DB_PATH)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_pending_pages(mza_conn: sqlite3.Connection, limit: int, existing_refs: set[str]) -> list[dict]:
    """Fetch pending pages from mza.db with book/municipality info, excluding already-queued ones."""
    rows = mza_conn.execute("""
        SELECT
            p.book_id, p.page_number, p.jp2_path, p.dzi_url,
            b.book_type, b.year_from, b.year_to, b.municipality_id,
            m.name AS municipality_name
        FROM pages p
        JOIN books b ON p.book_id = b.id
        JOIN municipalities m ON b.municipality_id = m.obec_id
        WHERE p.status = 'pending' AND p.dzi_url IS NOT NULL AND p.dzi_url != ''
        ORDER BY b.id, p.page_number
    """).fetchall()

    result = []
    for r in rows:
        ref = f"{r['book_id']}:{r['page_number']}"
        if ref not in existing_refs:
            result.append(dict(r))
            if len(result) >= limit:
                break
    return result


def _build_task(page: dict) -> dict:
    """Build a WQ task dict from a mza.db page row."""
    muni_dir = f"{page['municipality_name']}-{page['municipality_id']}"
    years = ""
    if page["year_from"] and page["year_to"]:
        years = f"{page['year_from']}-{page['year_to']}"
    elif page["year_from"]:
        years = str(page["year_from"])
    book_dir = f"{page['book_id']}_{page['book_type'] or 'mixed'}_{years}"
    output_path = f"images/{muni_dir}/{book_dir}/page_{page['page_number']:03d}.jpg"

    return {
        "task_type": "mza",
        "payload": {
            "jp2_path": page["jp2_path"],
            "dzi_url": page["dzi_url"],
            "output_path": output_path,
            "book_id": page["book_id"],
            "page_number": page["page_number"],
            "domain": "www.mza.cz",
        },
        "priority": 0,
        "source": "mza",
        "source_ref": f"{page['book_id']}:{page['page_number']}",
    }


def refill_once(db) -> dict:
    """Check pending count and push a batch if needed. Returns {created, skipped, pending_before}."""
    stats = db.get_stats()
    pending = stats.get("pending", 0)

    if pending >= REFILL_THRESHOLD:
        return {"created": 0, "skipped": 0, "pending_before": pending, "reason": "above_threshold"}

    mza_conn = _get_mza_conn()
    if not mza_conn:
        return {"created": 0, "skipped": 0, "pending_before": pending, "reason": "no_mza_db"}

    try:
        # Get all existing source_refs from WQ to avoid re-pushing completed/pending tasks
        existing = db.conn.execute(
            "SELECT source_ref FROM tasks WHERE source = 'mza'"
        ).fetchall()
        existing_refs = {r[0] for r in existing if r[0]}

        pages = _fetch_pending_pages(mza_conn, REFILL_BATCH, existing_refs)
        if not pages:
            return {"created": 0, "skipped": 0, "pending_before": pending, "reason": "no_pending_pages"}

        tasks = [_build_task(p) for p in pages]
        result = db.create_tasks_bulk(tasks)
        created = result.get("created", 0)
        skipped = result.get("skipped", 0)

        if created > 0:
            db.log_activity(None, "autorefill", f"created={created} skipped={skipped} pending_before={pending}")
            logger.info("Auto-refill: pushed %d tasks (skipped %d, pending was %d)", created, skipped, pending)

        return {"created": created, "skipped": skipped, "pending_before": pending}
    finally:
        mza_conn.close()


async def autorefill_loop(app):
    """Background loop that refills the queue periodically."""
    logger.info("Auto-refill started (interval=%ds, threshold=%d, batch=%d, mza_db=%s)",
                REFILL_INTERVAL, REFILL_THRESHOLD, REFILL_BATCH, MZA_DB_PATH)

    # Wait a bit for startup
    await asyncio.sleep(5)

    while True:
        try:
            db = app.state.db
            refill_once(db)
        except Exception as e:
            logger.warning("Auto-refill error: %s", e)

        await asyncio.sleep(REFILL_INTERVAL)
