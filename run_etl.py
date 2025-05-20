# ETL-pipeline/run_etl.py
import logging
import sys
from pathlib import Path

from etl.pipeline import Pipeline
from etl.utils.paths import paths # Assuming paths.ROOT is defined

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    
    # Default paths
    # Assuming config/ is at the same level as the run_etl.py or paths.ROOT is project root
    default_sources_path = Path("config/sources.yaml")
    default_config_path = Path("config/config.yaml")

    sources_cfg_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_sources_path
    # Allow specifying config.yaml as a second argument, otherwise use default
    config_cfg_path = Path(sys.argv[2]) if len(sys.argv) > 2 else default_config_path
    
    # Ensure paths are absolute or relative to a known root if necessary
    # For instance, if paths.ROOT is your project root:
    # if not sources_cfg_path.is_absolute():
    #     sources_cfg_path = paths.ROOT / sources_cfg_path
    # if not config_cfg_path.is_absolute():
    #     config_cfg_path = paths.ROOT / config_cfg_path

    logging.info("Using sources config: %s", sources_cfg_path)
    logging.info("Using global config: %s", config_cfg_path)
    
    Pipeline(sources_yaml=sources_cfg_path, config_yaml_path=config_cfg_path).run()