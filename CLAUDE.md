# Workqueue — Universal Multi-Worker Job Queue

## Overview
Distributes HTTP fetch jobs (MZA parish register downloads, Find a Grave scraping, etc.) across multiple machines using their own IPs. NAS coordinator manages the queue; workers poll, execute, and upload results.

## Architecture
```
NAS Docker (coordinator)          Workers (Mac, PCs)
FastAPI + SQLite, port 8200       Poll GET /api/task
https://g.book.cz/wq/             Execute (dezoomify-rs, HTTP GET)
                                   Upload POST /api/result
```

## Key Files
- `coordinator/main.py` — FastAPI app assembly
- `coordinator/db.py` — SQLite schema + CRUD (workers, tasks, rate_limits, activity_log)
- `coordinator/config.py` — YAML config loader
- `coordinator/routes/api.py` — Worker + admin API endpoints
- `coordinator/routes/dashboard.py` — HTML status dashboard
- `worker/agent.py` — Poll loop + dispatch
- `worker/executors/mza.py` — dezoomify-rs subprocess
- `worker/executors/http_fetch.py` — requests.get wrapper
- `cli.py` — Entry point (coordinator/worker/register subcommands)
- `config.yaml` — Coordinator config (workers, rate limits, admin key)
- `worker-config.yaml` — Worker config (coordinator URL, credentials, capabilities)

## Running Locally
```bash
# Terminal 1: coordinator
python -m workqueue coordinator

# Terminal 2: worker
python -m workqueue worker --config worker-config.yaml
```

## MZA Integration
```bash
# Push pending pages from mza-sync DB to workqueue
python -m mza_sync.cli wq-push --wq-url http://127.0.0.1:8200 --wq-key changeme --limit 10

# Pull completed results back
python -m mza_sync.cli wq-pull --wq-url http://127.0.0.1:8200 --wq-key changeme
```

## NAS Deployment
- **Path:** `/volume3/py/workqueue/`
- **Docker:** port 8200, volume `wq-data` for SQLite + uploads
- **nginx:** `location /wq/` → proxy_pass 127.0.0.1:8200, client_max_body_size 10m
- **Deploy:** `tar czf - --exclude='__pycache__' --exclude='wq_data' --exclude='wq_work' coordinator/ worker/ cli.py __init__.py __main__.py requirements.txt config.yaml Dockerfile docker-compose.yml | ssh root@nasx "cd /volume3/py/workqueue && tar xzf -"` then `docker compose build && docker compose up -d`

## API Endpoints
| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/api/task` | Worker | Get one pending task |
| POST | `/api/result` | Worker | Submit result (multipart) |
| POST | `/api/heartbeat` | Worker | Keepalive |
| POST | `/api/tasks/bulk` | Admin | Create tasks in batch |
| POST | `/api/tasks` | Admin | Create single task |
| GET | `/api/tasks/{id}` | Admin | Check task status |
| DELETE | `/api/tasks` | Admin | Cancel pending tasks |
| GET | `/api/stats` | Public | JSON stats |
| GET | `/api/completed` | Admin | List completed tasks |
| GET | `/` | Public | HTML dashboard |

## Task Types
- `mza` — DZI tile download via dezoomify-rs (payload: jp2_path, output_path, book_id, page_number)
- `http` — Generic HTTP GET (payload: url, headers, timeout, save_to)

## Security
- Workers authenticate via `X-Worker-Id` + `X-API-Key` headers
- Admin endpoints require `X-API-Key` matching `admin_api_key` in config
- API keys are SHA256-hashed in the DB
- **TODO:** Change default `changeme` keys before deployment

## Rate Limiting
- Per-domain: max requests/hour, max concurrent, cooldown support
- Configured in `config.yaml` under `rate_limits`
- Stale tasks (assigned >5min) auto-reassigned to pending
