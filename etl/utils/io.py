from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Final, Optional
from zipfile import ZipFile

import requests

log: Final = logging.getLogger(__name__)
CHUNK: Final[int] = 8192  # 8 KiB streaming buffer


def _format_bytes(bytes_val: int) -> str:
    """🔄 Format bytes to human-readable string."""
    val: float = float(bytes_val)
    for unit in ["B", "KB", "MB", "GB"]:
        if val < 1024.0:
            return f"{val:.1f} {unit}"
        val /= 1024.0
    return f"{val:.1f} TB"


def download(url: str, dest: Path) -> Path:
    """🔄 Download file with progress logging."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.debug("✓ cached %s", dest.name)
        return dest

    # Get content length if available for size estimate
    try:
        with requests.head(url, timeout=10) as head_resp:
            content_length: Optional[str] = head_resp.headers.get(
                "content-length")
            total_size: Optional[int] = int(
                content_length) if content_length else None
    except BaseException:
        total_size = None

    # Single console message with size if known
    if total_size:
        log.info("⬇ %s (%s)", dest.name, _format_bytes(total_size))
    else:
        log.info("⬇ %s", dest.name)

    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()

        downloaded: int = 0
        last_progress_log: float = time.time()
        progress_interval: float = 5.0  # Log progress every 5 seconds to debug only

        with dest.open("wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                if chunk:  # Filter out keep-alive chunks
                    fh.write(chunk)
                    downloaded += len(chunk)

                    # Log detailed progress to debug only
                    current_time = time.time()
                    if current_time - last_progress_log >= progress_interval:
                        if total_size:
                            progress_pct = (downloaded / total_size) * 100
                            log.debug(
                                "📊 %s: %.1f%% (%s / %s)",
                                dest.name,
                                progress_pct,
                                _format_bytes(downloaded),
                                _format_bytes(total_size),
                            )
                        else:
                            log.debug(
                                "📊 %s: %s downloaded",
                                dest.name,
                                _format_bytes(downloaded),
                            )
                        last_progress_log = current_time

        # Final completion message - brief
        log.info("✅ %s", dest.name)

    return dest


def extract_zip(archive: Path, dest: Path) -> None:
    log.info("📦 Extracting %s → %s", archive.name, dest)
    dest.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive) as zf:
        zf.extractall(dest)
