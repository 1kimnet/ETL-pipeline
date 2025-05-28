# etl/pipeline.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .handlers import HANDLER_MAP
from .loaders import ArcPyFileGDBLoader
from .models import Source
from .utils import ensure_dirs, paths
from .utils.run_summary import Summary


class Pipeline:
    """One end-to-end ETL run."""

    # ------------------------------------------------------------------ ctor
    def __init__(
        self,
        sources_yaml: Path,
        *,
        config_yaml_path: Optional[Path] = None,
        extra_handler_map: Dict[str, Any] | None = None,
        summary: Summary | None = None,
    ) -> None:
        self.sources_yaml_path = sources_yaml
        self.handler_map: Dict[str, Any] = {
            **HANDLER_MAP,
            **(extra_handler_map or {}),
        }
        self.summary = summary or Summary()

        # -- global config --------------------------------------------------
        if config_yaml_path and config_yaml_path.exists():
            try:
                with config_yaml_path.open(encoding="utf-8") as fh:
                    self.global_cfg = yaml.safe_load(fh) or {}
                logging.getLogger("summary").info("🛠  Using global config %s", config_yaml_path)
            except Exception as exc:
                logging.getLogger("summary").warning("⚠️  Could not load %s (%s) – using defaults", config_yaml_path, exc)
                self.global_cfg = {}
        else:
            self.global_cfg = {}
            logging.getLogger("summary").info("ℹ️  No global config file supplied – using defaults")

        ensure_dirs()

    # ------------------------------------------------------------------ run
    def run(self) -> None:
        lg_sum = logging.getLogger("summary")

        # ---------- Download & staging ------------------------------------
        for src in Source.load_all(self.sources_yaml_path):
            if not src.enabled:
                lg_sum.info("⏭  Skipped (disabled): %s", src.name)
                self.summary.log_download("skip")
                continue

            handler_cls = self.handler_map.get(src.type)
            if not handler_cls:
                lg_sum.warning("🤷  Unknown type '%s' → skipped: %s", src.type, src.name)
                self.summary.log_download("skip")
                continue

            try:
                lg_sum.info("🚚 Downloading : %s", src.name)
                handler_cls(src, global_config=self.global_cfg).fetch()
                self.summary.log_download("done")
            except Exception as exc:
                self.summary.log_download("error")
                self.summary.log_error(src.name, str(exc))
                lg_sum.error("❌ Failed        : %s  (%s)", src.name, exc)
                if not self.global_cfg.get("continue_on_failure", True):
                    raise

        # ---------- Consolidate into FileGDB ------------------------------
        lg_sum.info("📦 Staging complete → building FileGDB …")
        try:
            loader = ArcPyFileGDBLoader(
                summary=self.summary,  # ← 1st positional or keyword!
                gdb_path=paths.GDB,
                sources_yaml_path=self.sources_yaml_path,
            )
            loader.load_from_staging(paths.STAGING)
            self.summary.log_staging("done")
        except Exception as exc:
            self.summary.log_staging("error")
            self.summary.log_error("GDB loader", str(exc))
            lg_sum.error("❌ GDB load failed (%s)", exc, exc_info=True)
        else:
            lg_sum.info("🏁 Pipeline finished → %s", paths.GDB)

        # ---------- Dump storybook summary --------------------------------
        self.summary.dump()
