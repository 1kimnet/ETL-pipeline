from __future__ import annotations

from pathlib import Path
import sys

from mini_etl.pipeline import Pipeline


if __name__ == "__main__":
    cfg = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("config/config.yaml")
    src = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("config/sources.yaml")
    Pipeline(cfg, src).run()
