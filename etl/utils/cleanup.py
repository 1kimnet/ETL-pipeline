"""🧹 Download folder cleanup utilities."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Final

from . import paths

log: Final = logging.getLogger(__name__)


def cleanup_downloads_folder() -> int:
    """🧹 Clean the downloads folder completely before pipeline run.
    
    Returns:
        Number of items removed.
    """
    downloads_dir = paths.DOWNLOADS
    
    if not downloads_dir.exists():
        log.info("📁 Downloads folder doesn't exist, nothing to clean")
        return 0
    
    # Count items before cleanup
    items_before = sum(1 for _ in downloads_dir.rglob("*") if _.is_file())
    
    if items_before == 0:
        log.info("📁 Downloads folder is already empty")
        return 0
    
    log.info("🧹 Cleaning downloads folder: %s (%d files)", downloads_dir, items_before)
    
    try:
        # Remove all contents but keep the directory
        for item in downloads_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
                log.debug("🗑️ Removed directory: %s", item.name)
            else:
                item.unlink()
                log.debug("🗑️ Removed file: %s", item.name)
        
        log.info("✅ Downloads folder cleaned: removed %d items", items_before)
        return items_before
        
    except Exception as e:
        log.error("❌ Failed to clean downloads folder: %s", e)
        raise


def cleanup_staging_folder() -> int:
    """🧹 Clean the staging folder completely before pipeline run.
    
    Returns:
        Number of items removed.
    """
    staging_dir = paths.STAGING
    
    if not staging_dir.exists():
        log.info("📁 Staging folder doesn't exist, nothing to clean")
        return 0
    
    # Count items before cleanup
    items_before = sum(1 for _ in staging_dir.rglob("*") if _.is_file())
    
    if items_before == 0:
        log.info("📁 Staging folder is already empty")
        return 0
    
    log.info("🧹 Cleaning staging folder: %s (%d files)", staging_dir, items_before)
    
    try:
        # Remove all contents but keep the directory
        for item in staging_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
                log.debug("🗑️ Removed directory: %s", item.name)
            else:
                item.unlink()
                log.debug("🗑️ Removed file: %s", item.name)
        
        log.info("✅ Staging folder cleaned: removed %d items", items_before)
        return items_before
        
    except Exception as e:
        log.error("❌ Failed to clean staging folder: %s", e)
        raise


def cleanup_before_pipeline_run(clean_downloads: bool = True, clean_staging: bool = True) -> None:
    """🧹 Perform complete cleanup before pipeline run.
    
    Args:
        clean_downloads: Whether to clean the downloads folder.
        clean_staging: Whether to clean the staging folder.
    """
    lg_sum = logging.getLogger("summary")
    
    total_cleaned = 0
    
    if clean_downloads:
        downloads_cleaned = cleanup_downloads_folder()
        total_cleaned += downloads_cleaned
    
    if clean_staging:
        staging_cleaned = cleanup_staging_folder()
        total_cleaned += staging_cleaned
    
    if total_cleaned > 0:
        lg_sum.info("🧹 Pre-pipeline cleanup complete: %d items removed", total_cleaned)
    else:
        lg_sum.info("📁 Folders already clean, ready to start pipeline")
