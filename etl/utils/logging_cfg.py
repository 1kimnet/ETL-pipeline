# etl/utils/logging_cfg.py
from __future__ import annotations

import logging
import logging.config
from datetime import datetime
from pathlib import Path
from typing import Final

# ---------------------------------------------------------------------------

LOG_DIR: Final = Path("logs")         # <— adjust if you want a different folder
LOG_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging(level_on_console: str = "VERBOSE") -> None:
    """Initialise two log files + console summary output."""
    today = datetime.now().strftime("%Y%m%d")

    summary_file = LOG_DIR / f"etl-{today}.log"
    debug_file   = LOG_DIR / f"etl-{today}-debug.log"

    cfg: dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "summary": {
                "format": "%(asctime)s  %(levelname)-7s  %(message)s",
                "datefmt": "%H:%M:%S",
            },
            "debug": {
                "format": (
                    "%(asctime)s  %(levelname)-7s  "
                    "[%(name)s:%(lineno)d]  %(message)s"
                ),
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level_on_console,
                "formatter": "summary",
            },
            "summary_file": {
                "class": "logging.FileHandler",
                "level": "INFO",
                "filename": str(summary_file),
                "encoding": "utf-8",
                "formatter": "summary",
            },
            "debug_file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "filename": str(debug_file),
                "encoding": "utf-8",
                "formatter": "debug",
            },
        },
        "loggers": {
            "summary": {
                "level": "INFO",
                "handlers": ["console", "summary_file"],
                "propagate": False,
            },
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["debug_file"],
        },
    }
    logging.config.dictConfig(cfg)
    logging.getLogger("summary").info("🟢 Logging initialised → %s", LOG_DIR)
