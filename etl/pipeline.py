import logging
from pathlib import Path
from typing import Any, Dict

from .handlers import HANDLER_MAP
from .loaders import ArcPyFileGDBLoader
from .models import Source
from .utils import ensure_dirs, paths
class Pipeline:
    def __init__(self, sources_yaml: Path, # sources_yaml is Path to your config
                 extra_handler_map: Dict[str, Any] | None = None):
        self.sources_yaml_path = sources_yaml # Store the path
        self.handler_map: Dict[str, Any] = {**HANDLER_MAP, **(extra_handler_map or {})}
        ensure_dirs()

    def run(self) -> None:
        # --- Download and Staging Phase ---
        # This part uses self.sources_yaml_path to load sources for downloading
        for src_config in Source.load_all(self.sources_yaml_path):
            if not src_config.enabled:
                logging.info("⚙️ Source '%s' disabled – skipping download/staging", src_config.name)
                continue
            handler_cls = self.handler_map.get(src_config.type)
            if not handler_cls:
                logging.warning("🤷 Unknown type %s (src '%s') – skipping download/staging", src_config.type, src_config.name)
                continue
            try:
                logging.info("🚀 Handling source: %s", src_config.name)
                handler_cls(src_config).fetch()
            except Exception as exc:
                logging.error("✗ Download/Staging failed for source '%s' – %s", src_config.name, exc, exc_info=True)
                # Decide if you want to continue with other sources or stop
                # if not config.get("continue_on_failure", True): raise

        # --- GDB Loading Phase ---
        logging.info("✅ Download and staging complete – building FileGDB…")
        try:
            # *** CRUCIAL CHANGE HERE: Pass self.sources_yaml_path ***
            loader = ArcPyFileGDBLoader(
                gdb_path=paths.GDB, # Or however you define the target GDB path
                sources_yaml_path=self.sources_yaml_path # Pass the path to the YAML
            )
            loader.load_from_staging(paths.STAGING) # paths.STAGING is your root staging folder
        except Exception as exc:
            logging.error("✗ GDB load failed – %s", exc, exc_info=True)
        else:
            logging.info("🏁 Pipeline finished – consolidated GDB at %s", paths.GDB)
