from __future__ import annotations

from pathlib import Path

import requests


def download(url: str, dest: Path) -> Path:
    resp = requests.get(url, timeout=60)
    dest.write_bytes(resp.content)
    return dest
