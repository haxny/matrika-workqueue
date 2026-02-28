"""Worker agent — poll loop + dispatch to executors."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
import yaml

from .executors import mza, http_fetch

logger = logging.getLogger("workqueue.worker")


@dataclass
class WorkerConfig:
    coordinator_url: str
    worker_id: str
    api_key: str
    capabilities: list[str]
    poll_interval: float = 5.0
    dezoomify_bin: Optional[str] = None
    work_dir: Path = Path("wq_work")
    min_delay: float = 30.0   # min seconds between MZA page downloads
    max_delay: float = 90.0   # max seconds between MZA page downloads


def load_worker_config(path: str | Path) -> WorkerConfig:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return WorkerConfig(
        coordinator_url=raw["coordinator_url"].rstrip("/"),
        worker_id=raw["worker_id"],
        api_key=raw["api_key"],
        capabilities=raw.get("capabilities", ["mza", "http"]),
        poll_interval=raw.get("poll_interval", 5.0),
        dezoomify_bin=raw.get("dezoomify_bin"),
        work_dir=Path(raw.get("work_dir", "wq_work")),
        min_delay=raw.get("min_delay", 30.0),
        max_delay=raw.get("max_delay", 90.0),
    )


def _headers(config: WorkerConfig) -> dict:
    return {
        "X-Worker-Id": config.worker_id,
        "X-API-Key": config.api_key,
    }


def poll_task(config: WorkerConfig) -> Optional[dict]:
    """Poll coordinator for a task. Returns task dict or None.

    Returns {"status": "no_task"} when nothing available.
    Returns {"status": "daily_limit"} when worker hit 488 MB daily cap.
    Returns {"status": "cooldown"} when domain is in post-book cooldown.
    """
    url = f"{config.coordinator_url}/api/task"
    params = {"capabilities": ",".join(config.capabilities)}
    try:
        resp = requests.get(url, headers=_headers(config), params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") in ("no_task", "daily_limit", "cooldown"):
            return data  # Return status so caller can act on it
        return data
    except requests.RequestException as e:
        logger.warning("Poll failed: %s", e)
        return None


def submit_result(
    config: WorkerConfig,
    task_id: int,
    success: bool,
    result_json: dict | None = None,
    error: str = "",
    file_path: Optional[Path] = None,
):
    """Submit task result to coordinator."""
    url = f"{config.coordinator_url}/api/result"
    data = {
        "task_id": str(task_id),
        "success": "true" if success else "false",
        "result_json": __import__("json").dumps(result_json or {}),
        "error": error,
    }
    files = None
    if file_path and file_path.exists():
        files = {"file": (file_path.name, open(file_path, "rb"), "application/octet-stream")}

    try:
        resp = requests.post(url, headers=_headers(config), data=data, files=files, timeout=60)
        resp.raise_for_status()
        logger.info("Result submitted for task %d: success=%s", task_id, success)
    except requests.RequestException as e:
        logger.error("Failed to submit result for task %d: %s", task_id, e)
    finally:
        if files:
            files["file"][1].close()


def heartbeat(config: WorkerConfig):
    """Send keepalive to coordinator."""
    url = f"{config.coordinator_url}/api/heartbeat"
    try:
        requests.post(url, headers=_headers(config), timeout=5)
    except requests.RequestException:
        pass


def execute_task(config: WorkerConfig, task: dict):
    """Dispatch task to the appropriate executor and submit result."""
    task_id = task["task_id"]
    task_type = task["task_type"]
    payload = task["payload"]

    logger.info("Executing task %d (type=%s)", task_id, task_type)
    config.work_dir.mkdir(parents=True, exist_ok=True)

    if task_type == "mza":
        result = mza.execute(payload, config.work_dir, config.dezoomify_bin)
        if result.success:
            file_path = Path(result.file_path) if result.file_path else None
            submit_result(
                config, task_id, True,
                result_json={
                    "sha256": result.sha256,
                    "width": result.width,
                    "height": result.height,
                },
                file_path=file_path,
            )
            # Clean up local file after upload
            if file_path and file_path.exists():
                file_path.unlink()
        else:
            submit_result(config, task_id, False, error=result.error or "Unknown error")

    elif task_type == "http":
        result = http_fetch.execute(payload, config.work_dir)
        if result.success:
            file_path = Path(result.file_path) if result.file_path else None
            submit_result(
                config, task_id, True,
                result_json={
                    "status_code": result.status_code,
                    "body": result.body,
                    "headers": result.headers,
                },
                file_path=file_path,
            )
        else:
            submit_result(config, task_id, False, error=result.error or "Unknown error")

    else:
        submit_result(config, task_id, False, error=f"Unknown task type: {task_type}")


def run(config: WorkerConfig):
    """Main poll loop. Runs until interrupted."""
    logger.info("Worker %s starting (coordinator=%s, capabilities=%s, delay=%.0f-%.0fs)",
                config.worker_id, config.coordinator_url, config.capabilities,
                config.min_delay, config.max_delay)

    heartbeat_interval = 60
    last_heartbeat = 0
    idle_count = 0

    while True:
        try:
            # Periodic heartbeat
            now = time.time()
            if now - last_heartbeat > heartbeat_interval:
                heartbeat(config)
                last_heartbeat = now

            # Poll for task
            result = poll_task(config)

            if result is None:
                # Network error — back off
                idle_count += 1
                time.sleep(min(config.poll_interval * (1 + idle_count), 60))
                continue

            status = result.get("status")

            if status == "daily_limit":
                logger.info("Daily download limit reached (488 MB). Sleeping 1 hour.")
                time.sleep(3600)
                continue

            if status == "cooldown":
                logger.info("Domain cooldown active (book just finished). Sleeping 30 min.")
                time.sleep(1800)
                continue

            if status == "no_task":
                idle_count += 1
                sleep_time = min(config.poll_interval * (1 + idle_count * 0.5), 60)
                time.sleep(sleep_time)
                continue

            # Got a real task
            idle_count = 0
            task_type = result.get("task_type", "")
            execute_task(config, result)

            # Random delay between MZA downloads to look human
            if task_type == "mza":
                delay = random.uniform(config.min_delay, config.max_delay)
                logger.info("Sleeping %.0fs before next download...", delay)
                time.sleep(delay)
            else:
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Worker %s shutting down", config.worker_id)
            break
        except Exception:
            logger.exception("Unexpected error in worker loop")
            time.sleep(config.poll_interval * 2)
