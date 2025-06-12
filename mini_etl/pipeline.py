from __future__ import annotations

from pathlib import Path
from typing import Final
from mini_etl.handlers.http import fetch

from mini_etl.loaders.sde_loader import load
from mini_etl.utils.config import load_config, load_sources
from mini_etl.utils.gdb_utils import ensure_gdb
from mini_etl.utils.logging import setup_logger
from mini_etl.utils.naming import sanitize


log: Final = setup_logger(__name__)


class Pipeline:
    def __init__(self, cfg_path: Path, src_path: Path) -> None:
        self.cfg = load_config(cfg_path)
        self.sources = list(load_sources(src_path))
        ensure_gdb(self.cfg.download_dir)
        ensure_gdb(self.cfg.staging_dir)

    def run(self) -> None:
        for src in self.sources:
            if not src.enabled:
                continue
            dl = fetch(src, self.cfg.download_dir)
            staged = self.cfg.staging_dir / sanitize(dl.name)
            staged.write_bytes(dl.read_bytes())
            load(staged, self.cfg.sde_path)
