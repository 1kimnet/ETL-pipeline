from __future__ import annotations

from pathlib import Path

from mini_etl.models import Source
from mini_etl.handlers.http import fetch


def fetch_atom(src: Source, download_dir: Path) -> Path:
    return fetch(src, download_dir)
