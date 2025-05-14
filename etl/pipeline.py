import logging
from pathlib import Path
from typing import Any, Dict

from .handlers import HANDLER_MAP
from .loaders import ArcPyFileGDBLoader
from .models import Source
from .utils import ensure_dirs, paths


class Pipeline:  # noqa: D101
    def __init__(self, sources_yaml: Path, extra_handler_map: Dict[str, Any] | None = None):
        self.sources_yaml = sources_yaml
        self.handler_map: Dict[str, Any] = {**HANDLER_MAP, **(extra_handler_map or {})}
        ensure_dirs()

    def run(self) -> None:  # noqa: D401
        for src in Source.load_all(self.sources_yaml):
            if not src.enabled:
                logging.info("⚙️ %s disabled – skipping", src.name)
                continue
            handler_cls = self.handler_map.get(src.type)
            if not handler_cls:
                logging.warning("🤷 Unknown type %s (src %s)", src.type, src.name)
                continue
            try:
                handler_cls(src).fetch()
            except Exception as exc:  # pylint: disable=broad-except
                logging.error("✗ %s – %s", src.name, exc, exc_info=False)

        logging.info("✅ Download stage complete – building FileGDB…")
        try:
            ArcPyFileGDBLoader().load_from_staging(paths.STAGING)
        except Exception:  # pylint: disable=broad-except
            logging.error("❌ GDB load failed – see above for ArcPy details", exc_info=False)
        else:
            logging.info("🏁 Pipeline finished – consolidated GDB at %s", paths.GDB)
