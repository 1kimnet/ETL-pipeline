import logging
from pathlib import Path
from typing import Final
from zipfile import ZipFile

import requests

CHUNK: Final[int] = 8192  # 8 KiB streaming buffer


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        logging.info("✓ cached %s", dest.name)
        return dest
    logging.info("⬇ %s", url)
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                fh.write(chunk)
    return dest


def extract_zip(archive: Path, dest: Path) -> None:
    logging.info("📦 Extracting %s → %s", archive.name, dest)
    with ZipFile(archive) as zf:
        zf.extractall(dest)
