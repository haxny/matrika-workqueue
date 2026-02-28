#!/usr/bin/env python3
"""
Lightweight MZA page downloader — works on Android (Termux), Mac, Linux.
No dezoomify-rs needed. Pure Python DZI tile stitching with Pillow.

Usage:
    python mza-worker.py
    # or pipe from curl:
    curl -sL https://g.book.cz/mza-worker.py | python
"""

import io
import json
import math
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree

# Auto-install Pillow if missing
try:
    from PIL import Image
except ImportError:
    print("Installing Pillow...")
    os.system(f"{sys.executable} -m pip install -q Pillow")
    from PIL import Image

try:
    import requests
except ImportError:
    print("Installing requests...")
    os.system(f"{sys.executable} -m pip install -q requests")
    import requests

COORDINATOR_URL = "https://g.book.cz/wq"
API_KEY = "s_uM8iZvL3A1F0lAdcgfWxdcpWau12RD"
REFERER = "https://www.mza.cz/actapublica/"
USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36"
)
WORKER_ID = f"mob-{random.randint(10000000, 99999999)}"
MAX_TILE_WORKERS = 4
DELAY_MIN = 45
DELAY_MAX = 90


def fetch_task(session):
    """Get a task from the coordinator."""
    resp = session.get(
        f"{COORDINATOR_URL}/api/task",
        params={"capabilities": "mza"},
        headers={"X-Worker-Id": WORKER_ID, "X-API-Key": API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") in ("no_task", "daily_limit"):
        return None
    return data


def upload_result(session, task_id, jpeg_bytes, width, height):
    """Upload stitched JPEG to coordinator."""
    resp = session.post(
        f"{COORDINATOR_URL}/api/result",
        headers={"X-Worker-Id": WORKER_ID, "X-API-Key": API_KEY},
        data={
            "task_id": str(task_id),
            "success": "true",
            "result_json": json.dumps({
                "file_size": len(jpeg_bytes),
                "width": width,
                "height": height,
                "source": "mobile-worker",
            }),
        },
        files={"file": ("page.jpg", jpeg_bytes, "image/jpeg")},
        timeout=60,
    )
    resp.raise_for_status()


def report_error(session, task_id, error_msg):
    """Report task failure."""
    try:
        session.post(
            f"{COORDINATOR_URL}/api/result",
            headers={"X-Worker-Id": WORKER_ID, "X-API-Key": API_KEY},
            data={"task_id": str(task_id), "success": "false", "error": error_msg},
            timeout=15,
        )
    except Exception:
        pass


def stitch_dzi(session, dzi_url):
    """Fetch DZI descriptor, download all tiles, stitch into JPEG."""
    # 1. Fetch and parse DZI XML
    resp = session.get(dzi_url, headers={"Referer": REFERER, "User-Agent": USER_AGENT}, timeout=15)
    resp.raise_for_status()
    root = ElementTree.fromstring(resp.text)

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    tile_size = int(root.get("TileSize", "256"))
    overlap = int(root.get("Overlap", "0"))
    fmt = root.get("Format", "jpg")

    size_el = root.find(f"{ns}Size")
    width = int(size_el.get("Width"))
    height = int(size_el.get("Height"))

    # 2. Calculate max zoom level and tile grid
    max_level = math.ceil(math.log2(max(width, height)))
    cols = math.ceil(width / tile_size)
    rows = math.ceil(height / tile_size)

    # 3. Build tile URLs
    base_url = dzi_url.replace(".dzi", "")
    tile_base = f"{base_url}_files/{max_level}"

    print(f"  DZI: {width}x{height}, {cols}x{rows} tiles, level {max_level}")

    # 4. Fetch tiles with concurrency
    tiles = {}

    def fetch_tile(col, row):
        url = f"{tile_base}/{col}_{row}.{fmt}"
        r = session.get(url, headers={"Referer": REFERER, "User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        return col, row, Image.open(io.BytesIO(r.content))

    with ThreadPoolExecutor(max_workers=MAX_TILE_WORKERS) as pool:
        futures = []
        for r in range(rows):
            for c in range(cols):
                futures.append(pool.submit(fetch_tile, c, r))

        for f in futures:
            col, row, img = f.result()
            tiles[(col, row)] = img

    # 5. Stitch
    canvas = Image.new("RGB", (width, height))
    for (col, row), tile_img in tiles.items():
        # Crop overlap from non-edge tiles
        sx = overlap if col > 0 else 0
        sy = overlap if row > 0 else 0
        cropped = tile_img.crop((sx, sy, tile_img.width, tile_img.height))

        dx = col * tile_size
        dy = row * tile_size
        canvas.paste(cropped, (dx, dy))

    # 6. Export JPEG
    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=95)
    return buf.getvalue(), width, height


def main():
    print(f"=== MZA Worker [{WORKER_ID}] ===")
    print(f"Coordinator: {COORDINATOR_URL}")
    print()

    session = requests.Session()
    completed = 0
    errors = 0

    while True:
        try:
            task = fetch_task(session)
        except Exception as e:
            print(f"Poll error: {e}")
            time.sleep(60)
            continue

        if not task:
            print("No tasks available. Waiting 60s...")
            time.sleep(60)
            continue

        task_id = task["task_id"]
        payload = task["payload"]
        dzi_url = payload.get("dzi_url", "")
        output = payload.get("output_path", "?")
        print(f"Task #{task_id}: {output}")

        try:
            jpeg_bytes, w, h = stitch_dzi(session, dzi_url)
            upload_result(session, task_id, jpeg_bytes, w, h)
            completed += 1
            mb = len(jpeg_bytes) / 1024 / 1024
            print(f"  Done: {w}x{h}, {mb:.1f} MB (total: {completed} pages)")
        except Exception as e:
            errors += 1
            print(f"  ERROR: {e}")
            report_error(session, task_id, str(e))

        delay = random.randint(DELAY_MIN, DELAY_MAX)
        print(f"  Waiting {delay}s...")
        time.sleep(delay)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
