"""🧹 Enhanced cleanup utilities for downloads, staging, and temporary files."""

from __future__ import annotations

import atexit
import logging
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Final, Set
from datetime import datetime, timedelta

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


class TempFileManager:
    """Manages temporary files and directories with automatic cleanup."""
    
    def __init__(self, max_age_hours: int = 24):
        self.max_age_hours = max_age_hours
        self.tracked_paths: Set[Path] = set()
        self.lock = threading.RLock()
        self.cleanup_registered = False
    
    def track_path(self, path: Path) -> None:
        """Track a path for cleanup."""
        with self.lock:
            self.tracked_paths.add(path)
            self._register_cleanup()
        log.debug("Started tracking path for cleanup: %s", path)
    
    def untrack_path(self, path: Path) -> None:
        """Stop tracking a path."""
        with self.lock:
            self.tracked_paths.discard(path)
        log.debug("Stopped tracking path: %s", path)
    
    def cleanup_path(self, path: Path) -> bool:
        """Clean up a specific path."""
        try:
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
                log.debug("Cleaned up path: %s", path)
                return True
        except Exception as e:
            log.warning("Failed to cleanup path %s: %s", path, e)
        return False
    
    def cleanup_all(self) -> None:
        """Clean up all tracked paths."""
        with self.lock:
            paths_to_cleanup = list(self.tracked_paths)
            self.tracked_paths.clear()
        
        cleaned_count = 0
        for path in paths_to_cleanup:
            if self.cleanup_path(path):
                cleaned_count += 1
        
        if cleaned_count > 0:
            log.info("Cleaned up %d temporary paths", cleaned_count)
    
    def cleanup_old_temp_files(self) -> None:
        """Clean up old temporary files in system temp directory."""
        temp_dir = Path(tempfile.gettempdir())
        if not temp_dir.exists():
            return
        
        cutoff_time = datetime.now() - timedelta(hours=self.max_age_hours)
        cutoff_timestamp = cutoff_time.timestamp()
        
        cleaned_count = 0
        for item in temp_dir.iterdir():
            try:
                # Check if it's an ETL temp file/directory
                if not item.name.startswith(('etl_temp_', 'shp_', 'unzip_')):
                    continue
                
                # Check if it's old enough
                if item.stat().st_mtime < cutoff_timestamp:
                    if item.is_dir():
                        shutil.rmtree(item, ignore_errors=True)
                    else:
                        item.unlink(missing_ok=True)
                    cleaned_count += 1
                    log.debug("Cleaned up old temp item: %s", item)
                    
            except Exception as e:
                log.debug("Failed to cleanup old temp item %s: %s", item, e)
        
        if cleaned_count > 0:
            log.info("Cleaned up %d old temporary files", cleaned_count)
    
    def _register_cleanup(self) -> None:
        """Register cleanup function to run at exit."""
        if not self.cleanup_registered:
            atexit.register(self.cleanup_all)
            self.cleanup_registered = True


# Global temporary file manager
_temp_manager = TempFileManager()


def track_temp_path(path: Path) -> None:
    """Track a path for automatic cleanup."""
    _temp_manager.track_path(path)


def untrack_temp_path(path: Path) -> None:
    """Stop tracking a path for cleanup."""
    _temp_manager.untrack_path(path)


def cleanup_temp_files() -> None:
    """Clean up all tracked temporary files."""
    _temp_manager.cleanup_all()


def cleanup_old_temp_files() -> None:
    """Clean up old temporary files."""
    _temp_manager.cleanup_old_temp_files()


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
    
    # Clean up old temporary files
    cleanup_old_temp_files()
    
    if total_cleaned > 0:
        lg_sum.info("🧹 Pre-pipeline cleanup complete: %d items removed", total_cleaned)
    else:
        lg_sum.info("📁 Folders already clean, ready to start pipeline")


# Register cleanup at module level
atexit.register(cleanup_temp_files)
