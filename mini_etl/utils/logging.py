from __future__ import annotations

import logging
from typing import Final


def setup_logger(name: str = "mini_etl", level: str = "INFO") -> logging.Logger:
    log: Final = logging.getLogger(name)
    if not log.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(levelname)s %(message)s")
        handler.setFormatter(fmt)
        log.addHandler(handler)
        log.setLevel(level)
    return log
