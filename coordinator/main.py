"""FastAPI app assembly for the workqueue coordinator."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import load_config
from .db import Database

logger = logging.getLogger("workqueue")

CONFIG_PATH = os.environ.get("WQ_CONFIG", "config.yaml")
_db: Database | None = None


def _init_db(config) -> Database:
    """Create DB, register workers, set rate limits from config."""
    db = Database(config.db_path)
    for w in config.workers:
        db.register_worker(w.worker_id, w.api_key, w.capabilities)
        logger.info("Registered worker: %s (%s)", w.worker_id, w.capabilities)
    for rl in config.rate_limits:
        db.set_rate_limit(rl.domain, rl.max_requests_per_hour, rl.max_concurrent)
        logger.info("Rate limit: %s — %d/h, %d concurrent",
                     rl.domain, rl.max_requests_per_hour, rl.max_concurrent)
    return db


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db
    config = load_config(CONFIG_PATH)
    _db = _init_db(config)
    app.state.db = _db
    app.state.config = config
    logger.info("Workqueue coordinator started (db=%s)", config.db_path)
    yield
    if _db is not None:
        _db.close()


app = FastAPI(
    title="Workqueue Coordinator",
    root_path=os.environ.get("ROOT_PATH", ""),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://g.book.cz", "chrome-extension://*"],
    allow_methods=["GET", "POST"],
    allow_headers=["X-Worker-Id", "X-API-Key", "Content-Type"],
)

from .routes.api import router as api_router
from .routes.dashboard import router as dashboard_router

app.include_router(dashboard_router)
app.include_router(api_router, prefix="/api")
