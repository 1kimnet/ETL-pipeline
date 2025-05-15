"""Entry‑point wrapper so you can run ``python run_etl.py path/to/sources.yaml``."""

import logging
import sys
from pathlib import Path

from etl.pipeline import Pipeline

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    cfg = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("sources.yaml")
    Pipeline(cfg).run()
