from __future__ import annotations

from pathlib import Path
import json

from mini_etl.utils.logging import setup_logger

log = setup_logger(__name__)


def load_json(path: Path, dest: Path) -> None:
    log.info("🔄 loading json %s", path.name)
    data = json.loads(path.read_text())
    dest.write_text(json.dumps(data))
    log.info("✅ json loaded")
