"""Simple HTTP GET executor for generic fetch tasks."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger("workqueue.worker")


@dataclass
class HttpResult:
    success: bool
    status_code: Optional[int] = None
    body: Optional[str] = None
    headers: Optional[dict] = None
    file_path: Optional[str] = None
    error: Optional[str] = None


def execute(payload: dict, work_dir: Path) -> HttpResult:
    """Execute an HTTP GET request.

    Expected payload keys:
        url: str — URL to fetch
        headers: dict (optional) — extra headers
        timeout: int (optional) — seconds, default 30
        save_to: str (optional) — relative path to save response body as file
    """
    url = payload.get("url", "")
    if not url:
        return HttpResult(success=False, error="No URL in payload")

    headers = payload.get("headers", {})
    timeout = payload.get("timeout", 30)
    save_to = payload.get("save_to")

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        return HttpResult(success=False, error=str(e))

    if save_to:
        dest = work_dir / save_to
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            f.write(resp.content)
        return HttpResult(
            success=resp.ok,
            status_code=resp.status_code,
            file_path=str(dest),
            headers=dict(resp.headers),
        )

    return HttpResult(
        success=resp.ok,
        status_code=resp.status_code,
        body=resp.text[:50000],  # cap body size
        headers=dict(resp.headers),
    )
