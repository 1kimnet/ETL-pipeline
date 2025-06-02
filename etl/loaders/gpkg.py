# etl/loaders/gpkg.py
"""📦 GeoPackage loader for FileGDB."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Final, Set, Optional, List

import arcpy

from ..utils import paths
from ..utils.naming import generate_fc_name
from ..utils.run_summary import Summary

log: Final = logging.getLogger(__name__)

_MAIN_RE: Final[re.Pattern[str]] = re.compile(r"^main\.", re.IGNORECASE)


def load_gpkg_contents(
    gpkg_file_path: Path, 
    authority: str, 
    gdb_path: Path,
    used_names_set: Set[str], 
    summary: Summary,
    ensure_unique_name_func,
    include_filter: Optional[List[str]] = None
) -> None:
    """� Copy GPKG contents to staging GDB.
    
    Args:
        gpkg_file_path: Path to the GeoPackage file.
        authority: Source authority for naming.
        gdb_path: Target GDB path.
        used_names_set: Set of already used names for uniqueness.
        summary: Summary object for tracking progress.
        ensure_unique_name_func: Function to ensure unique naming.
        include_filter: Optional list of feature classes to include.
    """
    log.info("📦 Processing GeoPackage: %s (Authority: '%s', Include Filter: %s)",
             gpkg_file_path.relative_to(paths.ROOT), authority, include_filter or "None")
    
    if not gpkg_file_path.exists():
        log.error("❌ GeoPackage file does not exist: %s", gpkg_file_path)
        return
    
    gpkg_workspace: str = str(gpkg_file_path)
    current_arc_workspace: Optional[str] = arcpy.env.workspace  # type: ignore[attr-defined]
    
    try:
        arcpy.env.workspace = gpkg_workspace  # type: ignore[attr-defined]
        log.debug("📦 Temporarily set workspace to GPKG: %s", gpkg_workspace)
        
        feature_classes_in_gpkg: List[str] = arcpy.ListFeatureClasses()
        if not feature_classes_in_gpkg:
            log.info("ℹ️ No feature classes found in GeoPackage: %s", gpkg_file_path.name)
            return
        
        log.info("📦 Found %d feature classes in %s: %s",
                 len(feature_classes_in_gpkg), gpkg_file_path.name, feature_classes_in_gpkg)
        
        normalized_include_filter: Optional[Set[str]] = None
        if include_filter:
            normalized_include_filter = {_MAIN_RE.sub("", item).lower() for item in include_filter if item}
            log.info("📦 Normalized include filter for %s: %s", gpkg_file_path.name, normalized_include_filter)
        
        for fc_name_listed_by_arcpy in feature_classes_in_gpkg:
            _process_feature_class(
                fc_name_listed_by_arcpy, 
                authority, 
                gdb_path,
                used_names_set, 
                summary,
                ensure_unique_name_func,
                normalized_include_filter
            )
            
    except Exception as gpkg_processing_error:
        log.error("❌ Failed to list or process feature classes in GeoPackage '%s': %s",
                  gpkg_file_path.name, gpkg_processing_error, exc_info=True)
    finally:
        arcpy.env.workspace = current_arc_workspace  # type: ignore[attr-defined]
        log.debug("📦 Restored workspace after GPKG %s to: %s", gpkg_file_path.name, arcpy.env.workspace)  # type: ignore[attr-defined]


def _process_feature_class(
    fc_name_listed_by_arcpy: str,
    authority: str,
    gdb_path: Path,
    used_names_set: Set[str],
    summary: Summary,
    ensure_unique_name_func,
    normalized_include_filter: Optional[Set[str]]
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
        log.info("📦 Stripped 'main.' from '%s' → '%s' for output naming",
                 fc_name_listed_by_arcpy, stem_for_output_naming)
    
    base_name: str = generate_fc_name(authority, stem_for_output_naming)
    tgt_name: str = ensure_unique_name_func(base_name, used_names_set)
    
    _copy_feature_class(fc_name_listed_by_arcpy, tgt_name, gdb_path, summary)


def _copy_feature_class(input_fc_name: str, target_name: str, gdb_path: Path, summary: Summary) -> None:
    """📦 Copy a single feature class from GPKG to GDB with retry logic."""
    lg_sum = logging.getLogger("summary")
    copied_successfully: bool = False
    
    log.info("📦 Attempt 1: GPKG FC copy using input '%s' (listed name) → STAGING_GDB:/'%s'",
             input_fc_name, target_name)
    
    try:
        arcpy.conversion.FeatureClassToFeatureClass(
            in_features=input_fc_name, 
            out_path=str(gdb_path), 
            out_name=target_name
        )
        log.info("✅ Attempt 1 SUCCESS: Copied GPKG FC '%s' to '%s'", input_fc_name, target_name)
        lg_sum.info("   📄 GPKG FC ➜ staged : %s", target_name)
        summary.log_staging("done")
        copied_successfully = True
        
    except arcpy.ExecuteError as attempt1_error:
        arcpy_messages_e1: str = arcpy.GetMessages(2)
        log.warning("⚠️ Attempt 1 FAILED for input '%s': %s. ArcPy Messages: %s",
                   input_fc_name, attempt1_error, arcpy_messages_e1)
        
        if "000732" in arcpy_messages_e1 and _MAIN_RE.match(input_fc_name):
            copied_successfully = _retry_with_stripped_name(input_fc_name, target_name, gdb_path, summary)
        
        if not copied_successfully:
            summary.log_staging("error")
            summary.log_error(input_fc_name, f"GPKG FC copy failed: {attempt1_error}")
    except Exception as attempt1_generic_error:
        log.error("❌ Unexpected error on Attempt 1 for input '%s': %s",
                  input_fc_name, attempt1_generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(input_fc_name, f"GPKG FC unexpected error: {attempt1_generic_error}")
    
    if not copied_successfully:
        log.error("❗ Ultimately FAILED to copy GPKG FC '%s' to staging GDB.", input_fc_name)


def _retry_with_stripped_name(input_fc_name: str, target_name: str, gdb_path: Path, summary: Summary) -> bool:
    """📦 Retry GPKG feature class copy with stripped name."""
    lg_sum = logging.getLogger("summary")
    input_fc_name_attempt2: str = _MAIN_RE.sub("", input_fc_name)
    if input_fc_name_attempt2 != input_fc_name:
        log.info("� Attempt 2: GPKG FC copy using input '%s' (stripped name) → STAGING_GDB:/'%s'",
                 input_fc_name_attempt2, target_name)
        try:
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=input_fc_name_attempt2, 
                out_path=str(gdb_path), 
                out_name=target_name
            )
            log.info("✅ Attempt 2 SUCCESS: Copied GPKG FC '%s' (listed as '%s') to '%s'",
                     input_fc_name_attempt2, input_fc_name, target_name)
            lg_sum.info("   📄 GPKG FC ➜ staged : %s", target_name)
            summary.log_staging("done")
            return True
        except arcpy.ExecuteError as attempt2_error:
            arcpy_messages_e2: str = arcpy.GetMessages(2)
            log.error("❌ Attempt 2 FAILED for input '%s': %s. ArcPy Messages: %s",
                      input_fc_name_attempt2, attempt2_error, arcpy_messages_e2, exc_info=True)
            summary.log_staging("error")
            summary.log_error(input_fc_name_attempt2, f"GPKG FC retry failed: {attempt2_error}")
        except Exception as attempt2_generic_error:
            log.error("❌ Unexpected error on Attempt 2 for input '%s': %s",
                      input_fc_name_attempt2, attempt2_generic_error, exc_info=True)
            summary.log_staging("error")
            summary.log_error(input_fc_name_attempt2, f"GPKG FC retry unexpected error: {attempt2_generic_error}")
    return False
