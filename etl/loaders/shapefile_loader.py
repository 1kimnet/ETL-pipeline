# etl/loaders/shapefile_loader.py
"""📐 Shapefile format loader - clean architecture."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, Set, Optional, List, NamedTuple

import arcpy

from ..utils.gdb_utils import ensure_unique_name
from ..utils.naming import generate_fc_name
from ..utils.run_summary import Summary

log: Final = logging.getLogger(__name__)


class ShapefileValidationResult(NamedTuple):
    """📋 Result of shapefile validation."""
    is_valid: bool
    error_message: str
    missing_components: List[str]


def validate_shapefile_components(shp_file_path: Path) -> ShapefileValidationResult:
    """✅ Validate that all required shapefile components exist."""
    shp_stem: str = shp_file_path.stem
    shp_directory: Path = shp_file_path.parent
    
    required_extensions: List[str] = ['.shx', '.dbf']
    missing_components: List[str] = []
    
    log.debug("Validating shapefile components for stem: '%s' in directory: %s", 
             shp_stem, shp_directory)
    
    for ext in required_extensions:
        component_file: Path = shp_directory / f"{shp_stem}{ext}"
        log.debug("Checking for component file: %s", component_file)
        
        if not component_file.exists():
            missing_components.append(ext)
            log.debug("Missing component: %s", component_file)
        else:
            log.debug("Found component: %s", component_file)
    
    if missing_components:
        error_msg: str = f"Missing required components: {', '.join(missing_components)}"
        return ShapefileValidationResult(False, error_msg, missing_components)
    
    log.debug("✅ All shapefile components validated for: %s", shp_file_path.name)
    return ShapefileValidationResult(True, "All components present", [])


def find_alternative_shapefile(directory: Path) -> Optional[Path]:
    """🔍 Find a valid alternative shapefile in the given directory."""
    if not directory.exists():
        return None
    
    shp_files: List[Path] = list(directory.glob("*.shp"))
    log.debug("Found %d shapefile(s) in directory %s", len(shp_files), directory)
    
    for shp_file in shp_files:
        log.debug("Validating shapefile: %s", shp_file.name)
        validation_result = validate_shapefile_components(shp_file)
        if validation_result.is_valid:
            log.info("✅ Found valid shapefile: %s", shp_file.name)
            return shp_file
        else:
            log.debug("Invalid shapefile %s: %s", shp_file.name, validation_result.error_message)
    
    log.warning("⚠️ No valid shapefiles found in directory: %s", directory)
    return None


def retry_shapefile_with_full_path(
    shp_file_path: Path, 
    gdb_path: Path,
    tgt_name: str, 
    summary: Summary
) -> None:
    """🔄 Retry shapefile processing with full path approach."""
    lg_sum = logging.getLogger("summary")
    log.info("🔄 Retrying with full path approach for shapefile: %s", shp_file_path.name)
    
    try:
        input_shp_full_path: str = str(shp_file_path.resolve())
        out_fc_full_path = str(gdb_path / tgt_name)
        
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
        log.error("❌ Unexpected error on retry for SHP %s: %s", 
                 shp_file_path.name, retry_generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Retry unexpected error: {retry_generic_error}")


def process_shapefile(
    shp_file_path: Path, 
    authority: str, 
    gdb_path: Path,
    used_names_set: Set[str], 
    summary: Summary
) -> None:
    """📐 Process a single shapefile into the GDB."""
    log.debug("📐 Processing shapefile - Authority: '%s' for file: %s", authority, shp_file_path.name)
    lg_sum = logging.getLogger("summary")
    original_workspace: Optional[str] = arcpy.env.workspace  # type: ignore[attr-defined]
    
    if not shp_file_path.exists():
        log.error("❌ Shapefile does not exist: %s", shp_file_path)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, "File does not exist")
        return
    
    # Initialize variables before try block
    tgt_name: str = "UNKNOWN"
    
    # Enhanced shapefile validation
    validation_result = validate_shapefile_components(shp_file_path)
    
    if not validation_result.is_valid:
        log.warning("⚠️ Shapefile validation failed for %s: %s", 
                   shp_file_path.name, validation_result.error_message)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Validation failed: {validation_result.error_message}")
        
        # Try to find alternative shapefiles in the same directory
        alternative_shp = find_alternative_shapefile(shp_file_path.parent)
        if alternative_shp and alternative_shp != shp_file_path:
            log.info("🔄 Found alternative shapefile: %s", alternative_shp.name)
            process_shapefile(alternative_shp, authority, gdb_path, used_names_set, summary)
            return
        else:
            log.error("❌ No valid alternative shapefiles found in directory: %s", shp_file_path.parent)
            return
    
    try:
        # Set workspace to the directory containing the shapefile
        shp_directory: str = str(shp_file_path.parent)
        arcpy.env.workspace = shp_directory  # type: ignore[attr-defined]
        
        # Use just the filename (without path) for ArcPy input
        input_shp_name: str = shp_file_path.name
        
        base_name: str = generate_fc_name(authority, shp_file_path.stem)
        tgt_name = ensure_unique_name(base_name, used_names_set)
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
        log.error("❌ CopyFeatures failed for SHP %s → %s: %s. ArcPy Messages: %s",
                  shp_file_path.name, tgt_name, arc_error, arcpy_messages, exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"CopyFeatures failed: {arc_error}")
        
        # Try alternative approach with full path if workspace method fails
        if "000732" in arcpy_messages:  # Dataset does not exist error
            retry_shapefile_with_full_path(shp_file_path, gdb_path, tgt_name, summary)
                
    except Exception as generic_error:
        log.error("❌ Unexpected error processing SHP %s: %s", 
                 shp_file_path.name, generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(shp_file_path.name, f"Unexpected error: {generic_error}")
    finally:
        arcpy.env.workspace = original_workspace  # type: ignore[attr-defined]
