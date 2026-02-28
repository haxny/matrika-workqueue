"""CLI entry point: python -m workqueue coordinator|worker"""

from __future__ import annotations

import argparse
import logging
import sys


def cmd_coordinator(args):
    import uvicorn
    from .coordinator.main import app, CONFIG_PATH

    # Override config path if specified
    import workqueue.coordinator.main as m
    if args.config:
        m.CONFIG_PATH = args.config

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info",
    )


def cmd_worker(args):
    from .worker.agent import load_worker_config, run

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    config = load_worker_config(args.config)
    run(config)


def cmd_register(args):
    """Register a worker in the coordinator DB (admin utility)."""
    from .coordinator.config import load_config
    from .coordinator.db import Database

    config = load_config(args.coordinator_config or "config.yaml")
    db = Database(config.db_path)
    db.register_worker(args.worker_id, args.api_key, args.capabilities or "")
    print(f"Registered worker: {args.worker_id}")
    db.close()


def main():
    parser = argparse.ArgumentParser(
        prog="workqueue",
        description="Universal multi-worker job queue system",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # coordinator
    p_coord = sub.add_parser("coordinator", help="Start the coordinator server")
    p_coord.add_argument("-c", "--config", default="config.yaml", help="Config file path")
    p_coord.add_argument("--host", default="127.0.0.1", help="Bind host")
    p_coord.add_argument("--port", type=int, default=8200, help="Bind port")
    p_coord.set_defaults(func=cmd_coordinator)

    # worker
    p_worker = sub.add_parser("worker", help="Start a worker agent")
    p_worker.add_argument("-c", "--config", default="worker-config.yaml", help="Worker config file")
    p_worker.set_defaults(func=cmd_worker)

    # register
    p_reg = sub.add_parser("register", help="Register a worker (admin)")
    p_reg.add_argument("worker_id", help="Worker ID")
    p_reg.add_argument("api_key", help="Worker API key")
    p_reg.add_argument("--capabilities", help="Comma-separated capabilities")
    p_reg.add_argument("--coordinator-config", help="Coordinator config path")
    p_reg.set_defaults(func=cmd_register)

    args = parser.parse_args()
    args.func(args)
