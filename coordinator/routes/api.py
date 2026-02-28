"""Worker-facing and admin API endpoints."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, UploadFile, File, Form

logger = logging.getLogger("workqueue")

router = APIRouter()


def _get_db(request: Request):
    return request.app.state.db


def _get_config(request: Request):
    return request.app.state.config


def _auth_worker(
    request: Request,
    x_worker_id: str = Header(...),
    x_api_key: str = Header(...),
):
    db = request.app.state.db
    config = request.app.state.config

    # Auto-register browser extension and mobile workers
    if (
        (x_worker_id.startswith("ext-") or x_worker_id.startswith("mob-"))
        and config.extension_api_key
        and x_api_key == config.extension_api_key
    ):
        # Register on first contact (idempotent — ON CONFLICT DO UPDATE)
        db.register_worker(x_worker_id, x_api_key, capabilities="mza")
        db.update_worker_heartbeat(x_worker_id, request.client.host if request.client else None)
        return x_worker_id

    if not db.authenticate_worker(x_worker_id, x_api_key):
        raise HTTPException(status_code=401, detail="Invalid worker credentials")
    db.update_worker_heartbeat(x_worker_id, request.client.host if request.client else None)
    return x_worker_id


def _auth_admin(
    request: Request,
    x_api_key: str = Header(...),
):
    config = request.app.state.config
    if x_api_key != config.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")


# --- Worker endpoints ---


@router.get("/task")
def get_task(
    request: Request,
    worker_id: str = Depends(_auth_worker),
    capabilities: str = "",
):
    """Get one pending task matching worker capabilities. Returns 204 if none."""
    db = _get_db(request)
    caps = [c.strip() for c in capabilities.split(",") if c.strip()]
    if not caps:
        # Fall back to worker's registered capabilities
        workers = db.get_workers()
        for w in workers:
            if w.worker_id == worker_id:
                caps = w.capability_list
                break
    if not caps:
        raise HTTPException(status_code=400, detail="No capabilities specified")

    # Check daily bytes limit before trying to assign
    if not db._check_worker_bytes_limit(worker_id):
        bytes_today = db.get_worker_bytes_today(worker_id)
        logger.info("Worker %s hit daily limit: %.1f MB", worker_id, bytes_today / 1024 / 1024)
        return {"status": "daily_limit", "bytes_today_mb": round(bytes_today / 1024 / 1024, 1)}

    # Extension/mobile workers use their own IPs — skip domain rate limits/cooldowns
    skip_rate = worker_id.startswith("ext-") or worker_id.startswith("mob-")
    task = db.assign_task(worker_id, caps, skip_rate_limits=skip_rate)
    if not task:
        return {"status": "no_task"}

    db.log_activity(worker_id, "task_assigned", f"task={task.id} type={task.task_type}")
    return {
        "task_id": task.id,
        "task_type": task.task_type,
        "payload": task.payload_dict,
        "attempt": task.attempts,
        "max_attempts": task.max_attempts,
    }


@router.post("/result")
async def submit_result(
    request: Request,
    worker_id: str = Depends(_auth_worker),
    task_id: int = Form(...),
    success: str = Form(...),
    result_json: str = Form(default="{}"),
    error: str = Form(default=""),
    file: Optional[UploadFile] = File(default=None),
):
    """Submit task result. Multipart for file uploads, form data for JSON results."""
    db = _get_db(request)
    config = _get_config(request)

    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.worker_id != worker_id:
        raise HTTPException(status_code=403, detail="Task not assigned to this worker")

    is_success = success.lower() in ("true", "1", "yes")
    result_data = json.loads(result_json) if result_json else {}
    file_path = None

    if file and is_success:
        # Determine upload path from task payload or config
        payload = task.payload_dict
        relative_path = payload.get("output_path")

        if relative_path and config.upload_dir:
            dest = config.upload_dir / relative_path
        else:
            dest = (config.data_dir / "uploads" / f"task_{task_id}" /
                    (file.filename or "result.bin"))

        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            content = await file.read()
            f.write(content)
        file_path = str(dest)
        result_data["file_size"] = len(content)

    if is_success:
        db.complete_task(task_id, worker_id=worker_id, result_json=result_data, result_file_path=file_path)
        db.increment_worker_stats(worker_id, completed=1)
        db.log_activity(worker_id, "task_completed", f"task={task_id} bytes={result_data.get('file_size', 0)}")
    else:
        db.complete_task(task_id, worker_id=worker_id, error_msg=error or "Unknown error")
        db.increment_worker_stats(worker_id, failed=1)
        db.log_activity(worker_id, "task_failed", f"task={task_id} error={error}")

    return {"status": "ok", "task_id": task_id}


@router.post("/heartbeat")
def heartbeat(
    request: Request,
    worker_id: str = Depends(_auth_worker),
):
    """Worker keepalive."""
    return {"status": "ok", "worker_id": worker_id}


# --- Admin endpoints ---


@router.post("/tasks/bulk")
def create_tasks_bulk(
    request: Request,
    tasks: list[dict],
    _: None = Depends(_auth_admin),
):
    """Create tasks in batch, dedup by source_ref."""
    db = _get_db(request)
    result = db.create_tasks_bulk(tasks)
    db.log_activity(None, "bulk_create", f"created={result['created']} skipped={result['skipped']}")
    return result


@router.post("/tasks")
def create_task(
    request: Request,
    task: dict,
    _: None = Depends(_auth_admin),
):
    """Create a single task."""
    db = _get_db(request)
    task_id = db.create_task(
        task_type=task["task_type"],
        payload=task.get("payload", {}),
        priority=task.get("priority", 0),
        source=task.get("source"),
        source_ref=task.get("source_ref"),
        max_attempts=task.get("max_attempts", 3),
    )
    db.log_activity(None, "task_created", f"task={task_id} type={task['task_type']}")
    return {"task_id": task_id}


@router.patch("/tasks/priority")
def update_priority(
    request: Request,
    body: dict,
    _: None = Depends(_auth_admin),
):
    """Update priority for tasks by book_id."""
    db = _get_db(request)
    book_ids = body.get("book_ids", [])
    priority = body.get("priority", 0)
    if not book_ids or not isinstance(book_ids, list):
        raise HTTPException(status_code=400, detail="book_ids must be a non-empty list")
    count = db.update_priority_by_book_ids(book_ids, priority)
    db.log_activity(None, "priority_update", f"book_ids={book_ids} priority={priority} updated={count}")
    return {"updated": count, "priority": priority}


@router.get("/tasks/summary")
def get_task_summary(request: Request):
    """Book-level task summary (public, no auth)."""
    db = _get_db(request)
    return db.get_task_summary_by_book()


@router.get("/tasks/{task_id}")
def get_task_status(
    request: Request,
    task_id: int,
    _: None = Depends(_auth_admin),
):
    """Check task status."""
    db = _get_db(request)
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "id": task.id,
        "task_type": task.task_type,
        "status": task.status,
        "payload": task.payload_dict,
        "worker_id": task.worker_id,
        "result": task.result_dict,
        "result_file_path": task.result_file_path,
        "error_msg": task.error_msg,
        "attempts": task.attempts,
        "created_at": task.created_at,
        "completed_at": task.completed_at,
    }


@router.delete("/tasks")
def cancel_tasks(
    request: Request,
    source: Optional[str] = None,
    status: str = "pending",
    _: None = Depends(_auth_admin),
):
    """Cancel pending tasks, optionally filtered by source."""
    db = _get_db(request)
    count = db.cancel_tasks(source=source, status=status)
    db.log_activity(None, "tasks_cancelled", f"count={count} source={source}")
    return {"cancelled": count}


@router.get("/stats")
def get_stats(request: Request):
    """JSON stats (public, no auth required)."""
    db = _get_db(request)
    stats = db.get_stats()
    workers = db.get_workers()
    stats["workers"] = [
        {
            "worker_id": w.worker_id,
            "capabilities": w.capabilities,
            "last_seen_at": w.last_seen_at,
            "tasks_completed": w.tasks_completed,
            "tasks_failed": w.tasks_failed,
            "is_active": bool(w.is_active),
            "bytes_today_mb": round(db.get_worker_bytes_today(w.worker_id) / 1024 / 1024, 1),
        }
        for w in workers
    ]
    return stats


@router.get("/completed")
def get_completed(
    request: Request,
    source: Optional[str] = None,
    limit: int = 100,
    _: None = Depends(_auth_admin),
):
    """Get completed tasks for pulling results."""
    db = _get_db(request)
    tasks = db.get_completed_tasks(source=source, limit=limit)
    return [
        {
            "id": t.id,
            "task_type": t.task_type,
            "payload": t.payload_dict,
            "result": t.result_dict,
            "result_file_path": t.result_file_path,
            "source_ref": t.source_ref,
            "completed_at": t.completed_at,
        }
        for t in tasks
    ]
