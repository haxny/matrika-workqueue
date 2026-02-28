"""MZA dezoomify-rs executor — downloads parish register page images."""

from __future__ import annotations

import hashlib
import logging
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger("workqueue.worker")

IIP_BASE = "https://www.mza.cz/iipsrv/iipsrv.fcgi"
REFERER = "https://www.mza.cz/actapublica/"


@dataclass
class MzaResult:
    success: bool
    file_path: Optional[str] = None
    sha256: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    error: Optional[str] = None


def execute(payload: dict, work_dir: Path, dezoomify_bin: Optional[str] = None) -> MzaResult:
    """Download a single MZA page via dezoomify-rs.

    Expected payload keys:
        jp2_path: str — path on MZA IIP server (e.g. "MZA_Scan/...")
        dzi_url: str (optional) — full DZI URL override
        output_path: str — relative path for the output file
        book_id: int
        page_number: int
    """
    jp2_path = payload.get("jp2_path", "")
    dzi_url = payload.get("dzi_url") or f"{IIP_BASE}?Deepzoom={jp2_path}.dzi"
    output_rel = payload.get("output_path", f"page_{payload.get('page_number', 0):03d}.jpg")

    output_path = work_dir / output_rel
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".tmp.jpg")

    bin_path = dezoomify_bin or shutil.which("dezoomify-rs") or "dezoomify-rs"

    cmd = [
        bin_path,
        "--largest",
        "--header", f"Referer: {REFERER}",
        "--compression", "0",
        "--retries", "3",
        "--retry-delay", "2s",
        "-n", "16",
        "--min-interval", "50ms",
        "--timeout", "30s",
        dzi_url,
        str(tmp_path),
    ]

    logger.info("dezoomify-rs: %s → %s", dzi_url, output_path)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        tmp_path.unlink(missing_ok=True)
        return MzaResult(success=False, error="dezoomify-rs timed out after 300s")
    except FileNotFoundError:
        return MzaResult(
            success=False,
            error=f"dezoomify-rs not found at '{bin_path}'. Install: brew install dezoomify-rs",
        )

    output = (proc.stdout + "\n" + proc.stderr).strip()

    if proc.returncode != 0:
        tmp_path.unlink(missing_ok=True)
        return MzaResult(success=False, error=f"dezoomify-rs failed (rc={proc.returncode}): {output}")

    if not tmp_path.exists():
        return MzaResult(success=False, error="dezoomify-rs produced no output file")

    # Extract dimensions
    width, height = None, None
    m = re.search(r'\(\s*(\d+)\s*x\s*(\d+)\s*pixels', output)
    if m:
        width = int(m.group(1))
        height = int(m.group(2))

    # SHA256
    sha = hashlib.sha256()
    with open(tmp_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha.update(chunk)

    # Atomic rename
    tmp_path.rename(output_path)

    return MzaResult(
        success=True,
        file_path=str(output_path),
        sha256=sha.hexdigest(),
        width=width,
        height=height,
    )
