# ETL-pipeline/run_etl.py
from __future__ import annotations

import sys
from pathlib import Path

from etl.pipeline import Pipeline
from etl.utils.logging_cfg import configure_logging
from etl.utils.run_summary import Summary

# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # 1) set up logs (summary file + debug file + console)
    summary = Summary()                       # collector for human summary
    configure_logging(level_on_console="INFO")

    # 2) determine YAML paths (allow overrides via CLI args)
    default_sources = Path("config/sources.yaml")
    default_config  = Path("config/config.yaml")
    default_mappings = Path("config/mappings.yaml")

    sources_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_sources
    config_path  = Path(sys.argv[2]) if len(sys.argv) > 2 else default_config
    mappings_path = Path(sys.argv[3]) if len(sys.argv) > 3 else (default_mappings if default_mappings.exists() else None)

    # 3) run the pipeline
    Pipeline(
        sources_yaml=sources_path,
        config_yaml_path=config_path,
        mappings_yaml_path=mappings_path,
        summary=summary,                      # pass collector inside
    ).run()

    # 4) print the emoji-style storybook block
    summary.dump()
