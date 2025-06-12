from __future__ import annotations

from pathlib import Path
from typing import Final

from mini_etl.models import Source
from mini_etl.utils import requests as req
from mini_etl.utils.logging import setup_logger


log: Final = setup_logger(__name__)


def fetch(src: Source, download_dir: Path) -> Path:
    log.info("🔄 downloading %s", src.name)
    dest = download_dir / Path(src.url).name
    path = req.download(src.url, dest)
    log.info("✅ downloaded → %s", path.name)
    return path
