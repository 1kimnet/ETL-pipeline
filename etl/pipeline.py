# ETL-pipeline/etl/pipeline.py
import logging
from pathlib import Path
from typing import Any, Dict, Optional
import yaml # For loading config.yaml

from .handlers import HANDLER_MAP
from .loaders import ArcPyFileGDBLoader
from .models import Source
from .utils import ensure_dirs, paths

class Pipeline:
    def __init__(self, sources_yaml: Path,
                 config_yaml_path: Optional[Path] = None, # Ensure this parameter is present
                 extra_handler_map: Dict[str, Any] | None = None):
        self.sources_yaml_path = sources_yaml
        self.handler_map: Dict[str, Any] = {**HANDLER_MAP, **(extra_handler_map or {})}
        self.global_config: Dict[str, Any] = {}

        if config_yaml_path and config_yaml_path.exists():
            try:
                with config_yaml_path.open("r", encoding="utf-8") as f:
                    self.global_config = yaml.safe_load(f) or {}
                logging.info("✅ Successfully loaded global config from %s", config_yaml_path)
            except yaml.YAMLError as e:
                logging.error("❌ Error parsing global config YAML %s: %s", config_yaml_path, e)
            except Exception as e:
                logging.error("❌ Unexpected error loading global config %s: %s", config_yaml_path, e)
        elif config_yaml_path:
            logging.warning("⚠️ Global config file not found at %s. Using default handler settings.", config_yaml_path)
        else:
            logging.info("ℹ️ No global config file specified. Using default handler settings.")

        ensure_dirs()

    def run(self) -> None:
        # --- Download and Staging Phase ---
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
                # Pass the loaded global_config to the handler
                handler_instance = handler_cls(src_config, global_config=self.global_config)
                handler_instance.fetch()
            except Exception as exc:
                logging.error("✗ Download/Staging failed for source '%s' – %s", src_config.name, exc, exc_info=True)
                # Use self.global_config to check continue_on_failure
                if not self.global_config.get("continue_on_failure", True):
                    raise

        # --- GDB Loading Phase ---
        logging.info("✅ Download and staging complete – building FileGDB…")
        try:
            loader = ArcPyFileGDBLoader(
                gdb_path=paths.GDB,
                sources_yaml_path=self.sources_yaml_path
            )
            loader.load_from_staging(paths.STAGING)
        except Exception as exc:
            logging.error("✗ GDB load failed – %s", exc, exc_info=True)
        else:
            logging.info("🏁 Pipeline finished – consolidated GDB at %s", paths.GDB)