"""SQLite schema and CRUD operations for the workqueue coordinator."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    api_key_hash TEXT NOT NULL,
    capabilities TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT,
    last_ip TEXT,
    tasks_completed INTEGER NOT NULL DEFAULT 0,
    tasks_failed INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    bytes_today INTEGER NOT NULL DEFAULT 0,
    bytes_today_date TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    payload TEXT NOT NULL DEFAULT '{}',
    worker_id TEXT,
    assigned_at TEXT,
    result_json TEXT,
    result_file_path TEXT,
    completed_at TEXT,
    error_msg TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    source TEXT,
    source_ref TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS rate_limits (
    domain TEXT PRIMARY KEY,
    max_requests_per_hour INTEGER NOT NULL DEFAULT 120,
    max_concurrent INTEGER NOT NULL DEFAULT 2,
    current_hour_count INTEGER NOT NULL DEFAULT 0,
    hour_reset_at TEXT,
    cooldown_until TEXT
);

CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (datetime('now')),
    worker_id TEXT,
    action TEXT NOT NULL,
    details TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_type_status ON tasks(task_type, status);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority DESC, id ASC);
CREATE INDEX IF NOT EXISTS idx_tasks_source_ref ON tasks(source, source_ref);
"""

STALE_MINUTES = 5


def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


@dataclass
class TaskRow:
    id: int
    task_type: str
    status: str
    priority: int
    payload: str
    worker_id: Optional[str]
    assigned_at: Optional[str]
    result_json: Optional[str]
    result_file_path: Optional[str]
    completed_at: Optional[str]
    error_msg: Optional[str]
    attempts: int
    max_attempts: int
    source: Optional[str]
    source_ref: Optional[str]
    created_at: str
    updated_at: str

    @property
    def payload_dict(self) -> dict:
        return json.loads(self.payload) if self.payload else {}

    @property
    def result_dict(self) -> dict | None:
        return json.loads(self.result_json) if self.result_json else None


@dataclass
class WorkerRow:
    worker_id: str
    api_key_hash: str
    capabilities: str
    last_seen_at: Optional[str]
    last_ip: Optional[str]
    tasks_completed: int
    tasks_failed: int
    is_active: int
    bytes_today: int = 0
    bytes_today_date: Optional[str] = None

    @property
    def capability_list(self) -> list[str]:
        return [c.strip() for c in self.capabilities.split(",") if c.strip()]


DAILY_BYTES_LIMIT = 488 * 1024 * 1024  # 488 MB per worker per day
BOOK_COOLDOWN_HOURS = 8


class Database:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self):
        """Add columns that may be missing from older DBs."""
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(workers)").fetchall()}
        if "bytes_today" not in cols:
            self.conn.execute("ALTER TABLE workers ADD COLUMN bytes_today INTEGER NOT NULL DEFAULT 0")
            self.conn.execute("ALTER TABLE workers ADD COLUMN bytes_today_date TEXT")

    def close(self):
        self.conn.close()

    # --- Workers ---

    def register_worker(self, worker_id: str, api_key: str, capabilities: str = ""):
        self.conn.execute(
            "INSERT INTO workers (worker_id, api_key_hash, capabilities) VALUES (?, ?, ?) "
            "ON CONFLICT(worker_id) DO UPDATE SET api_key_hash=excluded.api_key_hash, "
            "capabilities=excluded.capabilities, is_active=1",
            (worker_id, hash_api_key(api_key), capabilities),
        )
        self.conn.commit()

    def authenticate_worker(self, worker_id: str, api_key: str) -> bool:
        row = self.conn.execute(
            "SELECT api_key_hash, is_active FROM workers WHERE worker_id=?",
            (worker_id,),
        ).fetchone()
        if not row:
            return False
        return row[0] == hash_api_key(api_key) and row[1] == 1

    def update_worker_heartbeat(self, worker_id: str, ip: Optional[str] = None):
        self.conn.execute(
            "UPDATE workers SET last_seen_at=datetime('now'), last_ip=COALESCE(?, last_ip) "
            "WHERE worker_id=?",
            (ip, worker_id),
        )
        self.conn.commit()

    def increment_worker_stats(self, worker_id: str, completed: int = 0, failed: int = 0):
        self.conn.execute(
            "UPDATE workers SET tasks_completed=tasks_completed+?, tasks_failed=tasks_failed+? "
            "WHERE worker_id=?",
            (completed, failed, worker_id),
        )
        self.conn.commit()

    def get_workers(self) -> List[WorkerRow]:
        rows = self.conn.execute(
            "SELECT worker_id, api_key_hash, capabilities, last_seen_at, last_ip, "
            "tasks_completed, tasks_failed, is_active, bytes_today, bytes_today_date "
            "FROM workers ORDER BY worker_id"
        ).fetchall()
        return [WorkerRow(*r) for r in rows]

    # --- Tasks ---

    def create_task(
        self,
        task_type: str,
        payload: dict,
        priority: int = 0,
        source: Optional[str] = None,
        source_ref: Optional[str] = None,
        max_attempts: int = 3,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO tasks (task_type, payload, priority, source, source_ref, max_attempts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (task_type, json.dumps(payload), priority, source, source_ref, max_attempts),
        )
        self.conn.commit()
        return cur.lastrowid

    def create_tasks_bulk(self, tasks: list[dict]) -> dict:
        """Create tasks in bulk, dedup by source_ref. Returns {created, skipped}."""
        created = 0
        skipped = 0
        for t in tasks:
            source = t.get("source")
            source_ref = t.get("source_ref")
            # Dedup: skip if same source+source_ref already exists and not failed/cancelled
            if source_ref:
                existing = self.conn.execute(
                    "SELECT id, status FROM tasks WHERE source=? AND source_ref=? "
                    "AND status NOT IN ('failed', 'cancelled')",
                    (source, source_ref),
                ).fetchone()
                if existing:
                    skipped += 1
                    continue
            self.conn.execute(
                "INSERT INTO tasks (task_type, payload, priority, source, source_ref, max_attempts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    t["task_type"],
                    json.dumps(t.get("payload", {})),
                    t.get("priority", 0),
                    source,
                    source_ref,
                    t.get("max_attempts", 3),
                ),
            )
            created += 1
        self.conn.commit()
        return {"created": created, "skipped": skipped}

    def assign_task(self, worker_id: str, capabilities: list[str],
                    skip_rate_limits: bool = False) -> Optional[TaskRow]:
        """Find and assign the highest-priority pending task matching capabilities.

        Checks: worker daily bytes limit, domain rate limits, domain cooldown.
        Extension workers (skip_rate_limits=True) bypass domain rate limits
        since they use their own IPs.
        """
        # First, reassign stale tasks
        self._reassign_stale()

        # Check worker daily bytes limit
        if not self._check_worker_bytes_limit(worker_id):
            return None

        # Build capability filter
        if not capabilities:
            return None
        placeholders = ",".join("?" for _ in capabilities)
        row = self.conn.execute(
            f"SELECT id, task_type, status, priority, payload, worker_id, assigned_at, "
            f"result_json, result_file_path, completed_at, error_msg, attempts, max_attempts, "
            f"source, source_ref, created_at, updated_at "
            f"FROM tasks WHERE status='pending' AND task_type IN ({placeholders}) "
            f"ORDER BY priority DESC, id ASC LIMIT 1",
            capabilities,
        ).fetchone()
        if not row:
            return None

        task = TaskRow(*row)

        # Check rate limits for this task (skip for extension workers)
        payload = task.payload_dict
        domain = payload.get("domain", "")
        if domain and not skip_rate_limits and not self._check_rate_limit(domain):
            return None

        # Assign
        self.conn.execute(
            "UPDATE tasks SET status='assigned', worker_id=?, assigned_at=datetime('now'), "
            "attempts=attempts+1, updated_at=datetime('now') WHERE id=?",
            (worker_id, task.id),
        )
        if domain:
            self._increment_rate_count(domain)
        self.conn.commit()

        # Re-fetch to get updated fields
        return self.get_task(task.id)

    def complete_task(
        self,
        task_id: int,
        worker_id: Optional[str] = None,
        result_json: Optional[dict] = None,
        result_file_path: Optional[str] = None,
        error_msg: Optional[str] = None,
    ):
        """Mark task as completed or failed.

        On success: tracks worker daily bytes, checks if book is finished
        (all pages for same book_id completed) → sets 8hr domain cooldown.
        """
        task = self.get_task(task_id)
        if not task:
            return

        if error_msg:
            # Check if we can retry
            if task.attempts < task.max_attempts:
                self.conn.execute(
                    "UPDATE tasks SET status='pending', worker_id=NULL, error_msg=?, "
                    "updated_at=datetime('now') WHERE id=?",
                    (error_msg, task_id),
                )
            else:
                self.conn.execute(
                    "UPDATE tasks SET status='failed', error_msg=?, completed_at=datetime('now'), "
                    "updated_at=datetime('now') WHERE id=?",
                    (error_msg, task_id),
                )
        else:
            self.conn.execute(
                "UPDATE tasks SET status='completed', result_json=?, result_file_path=?, "
                "completed_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
                (json.dumps(result_json) if result_json else None, result_file_path, task_id),
            )

            # Track worker daily bytes
            file_size = (result_json or {}).get("file_size", 0)
            w_id = worker_id or task.worker_id
            if w_id and file_size:
                self._add_worker_bytes(w_id, file_size)

            # Check if a full book just finished → cooldown
            # Skip cooldown for ext/mob workers — they use their own IPs
            payload = task.payload_dict
            book_id = payload.get("book_id")
            domain = payload.get("domain")
            w_id = worker_id or task.worker_id
            skip_cooldown = w_id and (w_id.startswith("ext-") or w_id.startswith("mob-"))
            if book_id and domain and not skip_cooldown:
                self._check_book_completed(book_id, domain)

        self.conn.commit()

    def get_task(self, task_id: int) -> Optional[TaskRow]:
        row = self.conn.execute(
            "SELECT id, task_type, status, priority, payload, worker_id, assigned_at, "
            "result_json, result_file_path, completed_at, error_msg, attempts, max_attempts, "
            "source, source_ref, created_at, updated_at FROM tasks WHERE id=?",
            (task_id,),
        ).fetchone()
        return TaskRow(*row) if row else None

    def get_completed_tasks(
        self, source: Optional[str] = None, limit: int = 100
    ) -> List[TaskRow]:
        sql = (
            "SELECT id, task_type, status, priority, payload, worker_id, assigned_at, "
            "result_json, result_file_path, completed_at, error_msg, attempts, max_attempts, "
            "source, source_ref, created_at, updated_at FROM tasks WHERE status='completed'"
        )
        params: list = []
        if source:
            sql += " AND source=?"
            params.append(source)
        sql += " ORDER BY completed_at DESC LIMIT ?"
        params.append(limit)
        return [TaskRow(*r) for r in self.conn.execute(sql, params).fetchall()]

    def cancel_tasks(self, source: Optional[str] = None, status: str = "pending") -> int:
        sql = "UPDATE tasks SET status='cancelled', updated_at=datetime('now') WHERE status=?"
        params: list = [status]
        if source:
            sql += " AND source=?"
            params.append(source)
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.rowcount

    def get_stats(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) FROM tasks GROUP BY status"
        ).fetchall()
        stats = {r[0]: r[1] for r in rows}
        stats["total"] = sum(stats.values())

        # Per task_type breakdown
        type_rows = self.conn.execute(
            "SELECT task_type, status, COUNT(*) FROM tasks GROUP BY task_type, status"
        ).fetchall()
        by_type: dict = {}
        for task_type, status, count in type_rows:
            by_type.setdefault(task_type, {})[status] = count
        stats["by_type"] = by_type

        # Recent activity
        recent = self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status='completed' "
            "AND completed_at > datetime('now', '-1 hour')"
        ).fetchone()
        stats["completed_last_hour"] = recent[0] if recent else 0

        return stats

    def _reassign_stale(self):
        """Reassign tasks stuck in 'assigned' for too long."""
        cutoff = (datetime.utcnow() - timedelta(minutes=STALE_MINUTES)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.conn.execute(
            "UPDATE tasks SET status='pending', worker_id=NULL, updated_at=datetime('now') "
            "WHERE status='assigned' AND assigned_at < ?",
            (cutoff,),
        )

    # --- Rate limits ---

    def set_rate_limit(self, domain: str, max_per_hour: int = 120, max_concurrent: int = 2):
        self.conn.execute(
            "INSERT INTO rate_limits (domain, max_requests_per_hour, max_concurrent) "
            "VALUES (?, ?, ?) ON CONFLICT(domain) DO UPDATE SET "
            "max_requests_per_hour=excluded.max_requests_per_hour, "
            "max_concurrent=excluded.max_concurrent",
            (domain, max_per_hour, max_concurrent),
        )
        self.conn.commit()

    def set_cooldown(self, domain: str, minutes: int):
        until = (datetime.utcnow() + timedelta(minutes=minutes)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.conn.execute(
            "UPDATE rate_limits SET cooldown_until=? WHERE domain=?",
            (until, domain),
        )
        self.conn.commit()

    def _check_rate_limit(self, domain: str) -> bool:
        row = self.conn.execute(
            "SELECT max_requests_per_hour, max_concurrent, current_hour_count, "
            "hour_reset_at, cooldown_until FROM rate_limits WHERE domain=?",
            (domain,),
        ).fetchone()
        if not row:
            return True  # No limit configured

        max_per_hour, max_concurrent, hour_count, reset_at, cooldown = row

        # Check cooldown
        if cooldown:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            if now < cooldown:
                return False

        # Check hourly count (reset if needed)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if reset_at and now >= reset_at:
            self.conn.execute(
                "UPDATE rate_limits SET current_hour_count=0, "
                "hour_reset_at=datetime('now', '+1 hour') WHERE domain=?",
                (domain,),
            )
            hour_count = 0

        if hour_count >= max_per_hour:
            return False

        # Check concurrent
        concurrent = self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status='assigned' AND "
            "json_extract(payload, '$.domain')=?",
            (domain,),
        ).fetchone()[0]
        if concurrent >= max_concurrent:
            return False

        return True

    def _increment_rate_count(self, domain: str):
        self.conn.execute(
            "UPDATE rate_limits SET current_hour_count=current_hour_count+1, "
            "hour_reset_at=COALESCE(hour_reset_at, datetime('now', '+1 hour')) "
            "WHERE domain=?",
            (domain,),
        )

    # --- Worker daily bytes ---

    def _check_worker_bytes_limit(self, worker_id: str) -> bool:
        """Return False if worker exceeded daily bytes limit."""
        row = self.conn.execute(
            "SELECT bytes_today, bytes_today_date FROM workers WHERE worker_id=?",
            (worker_id,),
        ).fetchone()
        if not row:
            return True
        bytes_today, date_str = row
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if date_str != today:
            # New day — reset
            self.conn.execute(
                "UPDATE workers SET bytes_today=0, bytes_today_date=? WHERE worker_id=?",
                (today, worker_id),
            )
            return True
        return bytes_today < DAILY_BYTES_LIMIT

    def _add_worker_bytes(self, worker_id: str, nbytes: int):
        """Add downloaded bytes to worker's daily total."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        # Reset if new day
        self.conn.execute(
            "UPDATE workers SET "
            "bytes_today = CASE WHEN bytes_today_date = ? THEN bytes_today + ? ELSE ? END, "
            "bytes_today_date = ? "
            "WHERE worker_id = ?",
            (today, nbytes, nbytes, today, worker_id),
        )

    def get_worker_bytes_today(self, worker_id: str) -> int:
        """Return bytes downloaded today by this worker."""
        row = self.conn.execute(
            "SELECT bytes_today, bytes_today_date FROM workers WHERE worker_id=?",
            (worker_id,),
        ).fetchone()
        if not row:
            return 0
        today = datetime.utcnow().strftime("%Y-%m-%d")
        return row[0] if row[1] == today else 0

    # --- Book completion cooldown ---

    def _check_book_completed(self, book_id: int, domain: str):
        """If all tasks for this book_id are completed, set an 8hr domain cooldown."""
        remaining = self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE source='mza' "
            "AND json_extract(payload, '$.book_id') = ? "
            "AND status IN ('pending', 'assigned')",
            (book_id,),
        ).fetchone()[0]
        if remaining == 0:
            # All pages for this book are done — set cooldown
            until = (datetime.utcnow() + timedelta(hours=BOOK_COOLDOWN_HOURS)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            self.conn.execute(
                "UPDATE rate_limits SET cooldown_until=? WHERE domain=?",
                (until, domain),
            )
            self.log_activity(
                None, "book_cooldown",
                f"book_id={book_id} cooldown={BOOK_COOLDOWN_HOURS}h until={until}",
            )

    # --- Priority management ---

    def update_priority_by_book_ids(self, book_ids: list[int], priority: int) -> int:
        """Update priority for pending/assigned tasks matching given book_ids."""
        if not book_ids:
            return 0
        placeholders = ",".join("?" for _ in book_ids)
        cur = self.conn.execute(
            f"UPDATE tasks SET priority=?, updated_at=datetime('now') "
            f"WHERE status IN ('pending', 'assigned') "
            f"AND json_extract(payload, '$.book_id') IN ({placeholders})",
            [priority] + book_ids,
        )
        self.conn.commit()
        return cur.rowcount

    def get_task_summary_by_book(self) -> list[dict]:
        """Get task counts grouped by book_id."""
        rows = self.conn.execute(
            "SELECT "
            "  json_extract(payload, '$.book_id') AS book_id, "
            "  SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending, "
            "  SUM(CASE WHEN status = 'assigned' THEN 1 ELSE 0 END) AS assigned, "
            "  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed, "
            "  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed, "
            "  MAX(priority) AS max_priority "
            "FROM tasks "
            "WHERE json_extract(payload, '$.book_id') IS NOT NULL "
            "GROUP BY json_extract(payload, '$.book_id')"
        ).fetchall()
        return [
            {
                "book_id": r[0],
                "pending": r[1],
                "assigned": r[2],
                "completed": r[3],
                "failed": r[4],
                "max_priority": r[5],
            }
            for r in rows
        ]

    # --- Activity log ---

    def log_activity(self, worker_id: Optional[str], action: str, details: str = ""):
        self.conn.execute(
            "INSERT INTO activity_log (worker_id, action, details) VALUES (?, ?, ?)",
            (worker_id, action, details),
        )
        self.conn.commit()

    def get_recent_activity(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT timestamp, worker_id, action, details FROM activity_log "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {"timestamp": r[0], "worker_id": r[1], "action": r[2], "details": r[3]}
            for r in rows
        ]
