# etl/utils/gdb_utils.py
"""🗄️ File GDB utility functions."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Final, Set

import arcpy

log: Final = logging.getLogger(__name__)


def ensure_unique_name(base_name: str, used_names: Set[str], max_length: int = 128) -> str:
    """🔧 Ensure the name is unique within the GDB.
    
    Args:
        base_name: Base name to make unique.
        used_names: Set of already used names.
        max_length: Maximum length for the name.
        
    Returns:
        Unique name that hasn't been used.
        
    Raises:
        ValueError: If unable to generate unique name within constraints.
    """
    import re
    
    # Sanitize the base name first
    sanitized_name = base_name
    
    # Replace problematic characters
    sanitized_name = re.sub(r'[-\s\.]+', '_', sanitized_name)  # hyphens, spaces, dots → underscore
    sanitized_name = re.sub(r'[åäö]', lambda m: {'å': 'a', 'ä': 'a', 'ö': 'o'}[m.group()], sanitized_name)  # Swedish chars
    sanitized_name = re.sub(r'[^\w]', '_', sanitized_name)  # Any remaining non-word chars → underscore
    sanitized_name = re.sub(r'_{2,}', '_', sanitized_name)  # Multiple underscores → single underscore
    sanitized_name = sanitized_name.strip('_')  # Remove leading/trailing underscores
    
    # Ensure it starts with letter or underscore (not number)
    if sanitized_name and sanitized_name[0].isdigit():
        sanitized_name = f"fc_{sanitized_name}"
    
    # Ensure not empty
    if not sanitized_name:
        sanitized_name = "unnamed_fc"
    
    # Only truncate if necessary (rarely should happen with 128 chars)
    candidate: str = sanitized_name[:max_length] if len(sanitized_name) > max_length else sanitized_name
    
    if not candidate:
        raise ValueError(f"Base name '{base_name}' resulted in empty string after sanitization")
    
    final_candidate: str = candidate
    idx: int = 1
    
    while final_candidate.lower() in (n.lower() for n in used_names):
        suffix: str = f"_{idx}"
        # Calculate available space for the base part
        available_length: int = max_length - len(suffix)
        
        if available_length <= 0:
            raise ValueError(f"Cannot generate unique name for '{base_name}' within {max_length} characters")
        
        truncated_base: str = candidate[:available_length]
        final_candidate = f"{truncated_base}{suffix}"
        idx += 1
        
        if idx > 9999:
            raise ValueError(f"Could not find unique name for '{base_name}' after {idx-1} attempts")
    
    used_names.add(final_candidate)
    
    # Log sanitization if name changed
    if final_candidate != base_name:
        log.info("🧹 Sanitized GDB name: %s → %s", base_name, final_candidate)
    
    return final_candidate


def reset_gdb(gdb_path: Path) -> None:
    """🔄 Reset (delete and recreate) the target GDB.
    
    Args:
        gdb_path: Path to the GDB to reset.
        
    Raises:
        RuntimeError: If GDB operations fail.
    """
    gdb_full_path: Path = gdb_path.resolve()
    log.info("🔄 Target GDB path for reset: %s", gdb_full_path)
    
    if gdb_path.exists():
        _remove_existing_gdb(gdb_full_path)
    else:
        log.info("ℹ️ GDB does not currently exist at %s, no removal needed.", gdb_full_path)
    
    _ensure_parent_directory_exists(gdb_path)
    _create_new_gdb(gdb_path)


def _remove_existing_gdb(gdb_full_path: Path) -> None:
    """🗑️ Remove existing GDB with error handling."""
    log.info("🗑️ Attempting to remove existing GDB: %s", gdb_full_path)
    try:
        shutil.rmtree(gdb_full_path)
        log.info("✅ Successfully removed existing GDB: %s", gdb_full_path)
    except Exception as removal_error:
        log.error("❌ Failed to remove existing GDB '%s': %s", gdb_full_path, removal_error, exc_info=True)
        raise RuntimeError(f"Failed to remove existing GDB '{gdb_full_path}': {removal_error}") from removal_error


def _ensure_parent_directory_exists(gdb_path: Path) -> None:
    """📁 Ensure the parent directory for the GDB exists."""
    parent_dir: Path = gdb_path.parent.resolve()
    if not parent_dir.exists():
        log.info("🆕 Parent directory %s for GDB does not exist. Attempting to create.", parent_dir)
        try:
            parent_dir.mkdir(parents=True, exist_ok=True)
            log.info("✅ Successfully created parent directory: %s", parent_dir)
        except Exception as dir_creation_error:
            log.error("❌ Failed to create parent directory '%s' for GDB: %s", 
                     parent_dir, dir_creation_error, exc_info=True)
            raise RuntimeError(f"Failed to create parent directory '{parent_dir}' for GDB: {dir_creation_error}") from dir_creation_error


def _create_new_gdb(gdb_path: Path) -> None:
    """🆕 Create a new FileGDB with error handling."""
    log.info("🆕 Attempting to create new FileGDB: %s in folder %s",
             gdb_path.name, gdb_path.parent.resolve())
    try:
        arcpy.management.CreateFileGDB(str(gdb_path.parent), gdb_path.name)
        log.info("✅ Successfully created new GDB: %s", gdb_path.resolve())
    except arcpy.ExecuteError:
        msg: str = arcpy.GetMessages(2)
        log.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s", gdb_path.resolve(), msg, exc_info=True)
        raise RuntimeError(f"CreateFileGDB failed for '{gdb_path.resolve()}': {msg}") from None
    except Exception as unexpected_gdb_error:
        log.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s",
                  gdb_path.resolve(), unexpected_gdb_error, exc_info=True)
        raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_path.resolve()}': {unexpected_gdb_error}") from unexpected_gdb_error


