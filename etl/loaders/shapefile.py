# etl/loaders/shapefile.py
"""📐 Shapefile loader for FileGDB."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, Set, Optional

import arcpy

from ..utils.naming import generate_fc_name
from ..utils.run_summary import Summary
from ..utils.validation import (
    validate_shapefile_components,
    find_alternative_shapefile,
    log_directory_contents
)

log: Final = logging.getLogger(__name__)


def load_shapefile(
    shp_file_path: Path,
    authority: str,
    gdb_path: Path,
    used_names_set: Set[str],
    summary: Summary,
    ensure_unique_name_func
) -> None:
    """📐 Process a single shapefile into the GDB.
    
    Args:
        shp_file_path: Path to the .shp file.
        authority: Source authority for naming.
        gdb_path: Target GDB path.
        used_names_set: Set of already used names for uniqueness.
        summary: Summary object for tracking progress.
        ensure_unique_name_func: Function to ensure unique naming.
    """
    log.debug("📐 Processing shapefile - Authority: '%s' for file: %s", authority, shp_file_path.name)
    lg_sum = logging.getLogger("summary")
    original_workspace: Optional[str] = arcpy.env.workspace
    
    if not shp_file_path.exists():
        log.error("❌ Shapefile does not exist: %s", shp_file_path)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, "File does not exist")
        return
    
    tgt_name: str = "UNKNOWN"
    out_fc_full_path: str = "UNKNOWN_PATH"
    
    # Enhanced shapefile validation
    validation_result = validate_shapefile_components(shp_file_path)
    
    if not validation_result.is_valid:
        log.warning("⚠️ Shapefile validation failed for %s: %s", 
                   shp_file_path.name, validation_result.error_message)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Validation failed: {validation_result.error_message}")
        
        # List what files are actually present for debugging
        log_directory_contents(shp_file_path.parent, "shapefile validation failed")
        
        # Try to find alternative shapefiles in the same directory
        alternative_shp: Optional[Path] = find_alternative_shapefile(shp_file_path.parent)
        if alternative_shp and alternative_shp != shp_file_path:
            log.info("🔄 Found alternative shapefile: %s", alternative_shp.name)
            load_shapefile(alternative_shp, authority, gdb_path, used_names_set, summary, ensure_unique_name_func)
            return
        else:
            log.error("❌ No valid alternative shapefiles found in directory: %s", shp_file_path.parent)
            return
    
    try:
        # Set workspace to the directory containing the shapefile
        shp_directory: str = str(shp_file_path.parent)
        arcpy.env.workspace = shp_directory
        
        # Use just the filename (without path) for ArcPy input
        input_shp_name: str = shp_file_path.name
        
        base_name: str = generate_fc_name(authority, shp_file_path.stem)
        tgt_name = ensure_unique_name_func(base_name, used_names_set)
        out_fc_full_path = str(gdb_path / tgt_name)
        
        log.info("📥 Copying SHP ('%s') → GDB:/'%s' (Authority: '%s')",
                 shp_file_path.name, tgt_name, authority)
        
        arcpy.management.CopyFeatures(
            in_features=input_shp_name,  # Use filename only
            out_feature_class=out_fc_full_path
        )
        log.info("✅ SUCCESS: Copied shapefile '%s' to '%s'", shp_file_path.name, tgt_name)
        lg_sum.info("   📄 SHP  ➜ staged : %s", tgt_name)
        summary.log_staging("done")
        
    except arcpy.ExecuteError as arc_error:
        arcpy_messages: str = arcpy.GetMessages(2)
        log.error("❌ arcpy.management.CopyFeatures failed for SHP %s → %s: %s. ArcPy Messages: %s",
                   (shp_file_path.name, tgt_name, arc_error, arcpy_messages), exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"CopyFeatures failed: {arc_error}")
        # Try alternative approach with full path if workspace method fails
        if "000732" in arcpy_messages:  # Dataset does not exist error
            _retry_with_full_path(shp_file_path, out_fc_full_path, tgt_name, summary)
                
    except Exception as generic_error:
        log.error("❌ Unexpected error processing SHP %s → %s: %s", 
                 shp_file_path.name, out_fc_full_path, generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Unexpected error: {generic_error}")
    finally:
        arcpy.env.workspace = original_workspace


def _retry_with_full_path(
    shp_file_path: Path, 
    out_fc_full_path: str, 
    tgt_name: str, 
    summary: Summary
) -> None:
    """🔄 Retry shapefile processing with full path approach."""
    lg_sum = logging.getLogger("summary")
    log.info("🔄 Retrying with full path approach for shapefile: %s", shp_file_path.name)
    try:
        input_shp_full_path: str = str(shp_file_path.resolve())
        arcpy.management.CopyFeatures(
            in_features=input_shp_full_path,
            out_feature_class=out_fc_full_path
        )
        log.info("✅ SUCCESS (retry): Copied shapefile '%s' to '%s'", shp_file_path.name, tgt_name)
        lg_sum.info("   📄 SHP  ➜ staged : %s", tgt_name)
        summary.log_staging("done")
    except arcpy.ExecuteError as retry_arc_error:
        log.error("❌ Retry also failed for SHP %s → %s: %s", 
                 shp_file_path.name, tgt_name, arcpy.GetMessages(2), exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Retry failed: {retry_arc_error}")
    except Exception as retry_generic_error:
        log.error("❌ Unexpected error on retry for SHP %s → %s: %s", 
                 shp_file_path.name, out_fc_full_path, retry_generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Retry unexpected error: {retry_generic_error}")
