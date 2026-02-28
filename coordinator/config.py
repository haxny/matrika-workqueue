"""YAML config loader for workqueue coordinator."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class WorkerConfig:
    worker_id: str
    api_key: str
    capabilities: str = ""


@dataclass
class RateLimitConfig:
    domain: str
    max_requests_per_hour: int = 120
    max_concurrent: int = 2


@dataclass
class CoordinatorConfig:
    data_dir: Path = field(default_factory=lambda: Path("wq_data"))
    upload_dir: Optional[Path] = None
    admin_api_key: str = "changeme"
    extension_api_key: str = ""  # Shared key for browser extension auto-registration
    host: str = "0.0.0.0"
    port: int = 8200
    workers: list[WorkerConfig] = field(default_factory=list)
    rate_limits: list[RateLimitConfig] = field(default_factory=list)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "workqueue.db"


def load_config(path: str | Path) -> CoordinatorConfig:
    path = Path(path)
    if not path.exists():
        return _from_env()

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    cfg = CoordinatorConfig(
        data_dir=Path(raw.get("data_dir", os.environ.get("WQ_DATA_DIR", "wq_data"))),
        upload_dir=Path(raw["upload_dir"]) if raw.get("upload_dir") else (
            Path(os.environ["WQ_UPLOAD_DIR"]) if os.environ.get("WQ_UPLOAD_DIR") else None
        ),
        admin_api_key=raw.get("admin_api_key", os.environ.get("WQ_ADMIN_KEY", "changeme")),
        extension_api_key=raw.get("extension_api_key", os.environ.get("WQ_EXT_KEY", "")),
        host=raw.get("host", "0.0.0.0"),
        port=raw.get("port", 8200),
    )

    for w in raw.get("workers", []):
        cfg.workers.append(WorkerConfig(
            worker_id=w["worker_id"],
            api_key=w["api_key"],
            capabilities=w.get("capabilities", ""),
        ))

    for rl in raw.get("rate_limits", []):
        cfg.rate_limits.append(RateLimitConfig(
            domain=rl["domain"],
            max_requests_per_hour=rl.get("max_requests_per_hour", 120),
            max_concurrent=rl.get("max_concurrent", 2),
        ))

    return cfg


def _from_env() -> CoordinatorConfig:
    """Fallback: build config from environment variables."""
    return CoordinatorConfig(
        data_dir=Path(os.environ.get("WQ_DATA_DIR", "wq_data")),
        upload_dir=Path(os.environ["WQ_UPLOAD_DIR"]) if os.environ.get("WQ_UPLOAD_DIR") else None,
        admin_api_key=os.environ.get("WQ_ADMIN_KEY", "changeme"),
    )
