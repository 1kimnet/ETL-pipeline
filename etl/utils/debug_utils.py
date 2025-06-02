# etl/utils/debug_utils.py
"""🔍 Debug utilities for logging and diagnostics."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

log: Final = logging.getLogger(__name__)


def log_directory_contents(directory: Path, context: str) -> None:
    """📋 Log directory contents for debugging purposes."""
    try:
        if not directory.exists():
            log.debug("Directory does not exist for %s: %s", context, directory)
            return
        
        contents = list(directory.iterdir())
        log.debug("Directory contents for %s (%s): %d items", 
                 context, directory, len(contents))
        
        for item in contents:
            if item.is_file():
                log.debug("  📄 File: %s (%d bytes)", item.name, item.stat().st_size)
            elif item.is_dir():
                log.debug("  📁 Dir:  %s", item.name)
                
    except Exception as e:
        log.debug("Could not list directory contents for %s: %s", context, e)
