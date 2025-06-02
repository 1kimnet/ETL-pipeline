# etl/loaders/gpkg_loader.py
"""📦 GeoPackage format loader - clean architecture."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Final, Set, Optional, List

import arcpy

from ..utils.gdb_utils import ensure_unique_name
from ..utils.naming import generate_fc_name
from ..utils.run_summary import Summary

log: Final = logging.getLogger(__name__)

# Regex pattern for stripping 'main.' prefix from GPKG feature class names
_MAIN_RE: Final = re.compile(r"^main\.", re.IGNORECASE)


def retry_gpkg_with_stripped_name(input_fc_name: str, target_name: str, gdb_path: Path) -> bool:
    """🔄 Retry GPKG feature class copy with stripped name."""
    try:
        stripped_input_name = _MAIN_RE.sub("", input_fc_name)
        if stripped_input_name != input_fc_name:
            log.info("🔄 Retrying with stripped name: '%s' → '%s'", input_fc_name, stripped_input_name)
            
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=stripped_input_name,
                out_path=str(gdb_path),
                out_name=target_name
            )
            log.info("✅ SUCCESS (retry): Copied GPKG FC '%s' to '%s'", stripped_input_name, target_name)
            return True
        return False
    except Exception as retry_error:
        log.debug("Retry with stripped name also failed: %s", retry_error)
        return False


def copy_gpkg_feature_class(input_fc_name: str, target_name: str, gdb_path: Path, summary: Summary) -> None:
    """📦 Copy a single feature class from GPKG to GDB with retry logic."""
    lg_sum = logging.getLogger("summary")
    
    try:
        log.info("📥 Copying GPKG FC ('%s') → GDB:/'%s'", input_fc_name, target_name)
        arcpy.conversion.FeatureClassToFeatureClass(
            in_features=input_fc_name,
            out_path=str(gdb_path),
            out_name=target_name
        )
        log.info("✅ SUCCESS: Copied GPKG FC '%s' to '%s'", input_fc_name, target_name)
        lg_sum.info("   📄 GPKG ➜ staged : %s", target_name)
        summary.log_staging("done")
        
    except arcpy.ExecuteError as arc_error:
        arcpy_messages: str = arcpy.GetMessages(2)
        log.error("❌ FeatureClassToFeatureClass failed for GPKG FC %s → %s: %s. ArcPy Messages: %s",
                  input_fc_name, target_name, arc_error, arcpy_messages, exc_info=True)
        
        # Try retry with stripped name
        if retry_gpkg_with_stripped_name(input_fc_name, target_name, gdb_path):
            lg_sum.info("   📄 GPKG ➜ staged : %s", target_name)
            summary.log_staging("done")
        else:
            summary.log_staging("error")
            summary.log_error(input_fc_name, f"FeatureClassToFeatureClass failed: {arc_error}")
            
    except Exception as generic_error:
        log.error("❌ Unexpected error copying GPKG FC %s → %s: %s",
                  input_fc_name, target_name, generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(input_fc_name, f"Unexpected error: {generic_error}")


def process_gpkg_feature_class(
    fc_name_listed_by_arcpy: str,
    authority: str,
    gdb_path: Path,
    used_names_set: Set[str],
    summary: Summary,
    normalized_include_filter: Optional[Set[str]] = None
) -> None:
    """📦 Process a single feature class from GPKG."""
    stem_for_output_naming: str = _MAIN_RE.sub("", fc_name_listed_by_arcpy)
    
    if normalized_include_filter:
        comparable_fc_name: str = stem_for_output_naming.lower()
        if comparable_fc_name not in normalized_include_filter:
            log.info("⏭️ Skipping GPKG FC '%s' (normalized: '%s') as it's not in the include filter.",
                     fc_name_listed_by_arcpy, comparable_fc_name)
            return
        else:
            log.info("✅ GPKG FC '%s' (normalized: '%s') is in include filter. Proceeding.",
                     fc_name_listed_by_arcpy, comparable_fc_name)
    
    if stem_for_output_naming != fc_name_listed_by_arcpy:
        log.info("Stripped 'main.' from '%s' → '%s' for output naming",
                 fc_name_listed_by_arcpy, stem_for_output_naming)
    
    base_name: str = generate_fc_name(authority, stem_for_output_naming)
    tgt_name: str = ensure_unique_name(base_name, used_names_set)
    
    copy_gpkg_feature_class(fc_name_listed_by_arcpy, tgt_name, gdb_path, summary)


def process_gpkg_contents(
    gpkg_file_path: Path,
    authority: str,
    gdb_path: Path,
    used_names_set: Set[str],
    summary: Summary,
    include_filter: Optional[List[str]] = None
) -> None:
    """📦 Copy GPKG contents to staging GDB with comprehensive error handling."""
    log.info("📦 Processing GeoPackage: %s (Authority: '%s', Include Filter: %s)",
             gpkg_file_path.name, authority, include_filter or "None")
    
    if not gpkg_file_path.exists():
        log.error("❌ GeoPackage file does not exist: %s", gpkg_file_path)
        return
    
    gpkg_workspace: str = str(gpkg_file_path)
    current_arc_workspace: Optional[str] = arcpy.env.workspace  # type: ignore[attr-defined]
    
    try:
        arcpy.env.workspace = gpkg_workspace  # type: ignore[attr-defined]
        log.debug("Temporarily set workspace to GPKG: %s", gpkg_workspace)
        
        feature_classes_in_gpkg: List[str] = arcpy.ListFeatureClasses()
        if not feature_classes_in_gpkg:
            log.info("ℹ️ No feature classes found in GeoPackage: %s", gpkg_file_path.name)
            return
        
        log.info("Found %d feature classes in %s: %s",
                 len(feature_classes_in_gpkg), gpkg_file_path.name, feature_classes_in_gpkg)
        
        # Normalize include filter for comparison
        normalized_include_filter: Optional[Set[str]] = None
        if include_filter:
            normalized_include_filter = {_MAIN_RE.sub("", item).lower() for item in include_filter if item}
            log.info("Normalized include filter for %s: %s", gpkg_file_path.name, normalized_include_filter)
        
        for fc_name_listed_by_arcpy in feature_classes_in_gpkg:
            process_gpkg_feature_class(
                fc_name_listed_by_arcpy, authority, gdb_path, used_names_set,
                summary, normalized_include_filter
            )
            
    except Exception as gpkg_processing_error:
        log.error("❌ Failed to list or process feature classes in GeoPackage '%s': %s",
                  gpkg_file_path.name, gpkg_processing_error, exc_info=True)
    finally:
        arcpy.env.workspace = current_arc_workspace  # type: ignore[attr-defined]
        log.debug("Restored workspace after GPKG %s to: %s", gpkg_file_path.name, arcpy.env.workspace)  # type: ignore[attr-defined]