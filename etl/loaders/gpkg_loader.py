# etl/loaders/gpkg_loader.py
"""📦 GeoPackage format loader."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Final, Set, Optional, List

import arcpy

from ..utils.gdb_utils import ensure_unique_name
from ..utils.naming import generate_fc_name
from ..utils.run_summary import Summary
from ..utils.arcpy_context import arcpy_workspace, safe_arcpy_operation

log: Final = logging.getLogger(__name__)

# Regex pattern for stripping 'main.' prefix from GPKG feature class names
_MAIN_RE: Final = re.compile(r"^main\.", re.IGNORECASE)


@safe_arcpy_operation
def copy_gpkg_feature_class(
        input_fc_name: str,
        target_name: str,
        gdb_path: Path,
        summary: Summary) -> None:
    """📦 Copy a single feature class from GPKG to GDB with comprehensive retry logic."""
    lg_sum = logging.getLogger("summary")

    # Strategy 1: Try with original listed name
    if _attempt_copy_with_name(input_fc_name, target_name, gdb_path):
        log.info(
            "✅ SUCCESS: Copied GPKG FC '%s' to '%s'",
            input_fc_name,
            target_name)
        lg_sum.info("   📄 GPKG ➜ staged : %s", target_name)
        summary.log_staging("done")
        return

    # Strategy 2: Try with stripped name (remove 'main.' prefix)
    stripped_name = _MAIN_RE.sub("", input_fc_name)
    if stripped_name != input_fc_name:
        log.info(
            "🔄 Retrying with stripped name: '%s' → '%s'",
            input_fc_name,
            stripped_name)
        if _attempt_copy_with_name(stripped_name, target_name, gdb_path):
            log.info(
                "✅ SUCCESS (retry): Copied GPKG FC '%s' to '%s'",
                stripped_name,
                target_name)
            lg_sum.info("   📄 GPKG ➜ staged : %s", target_name)
            summary.log_staging("done")
            return

    # Strategy 3: Try with full workspace path
    current_workspace = arcpy.env.workspace  # type: ignore[attr-defined]
    if current_workspace:
        for candidate_name in [input_fc_name, stripped_name]:
            full_path = f"{current_workspace}\\{candidate_name}"
            log.info("🔄 Trying full path: '%s'", full_path)
            if _attempt_copy_with_name(full_path, target_name, gdb_path):
                log.info(
                    "✅ SUCCESS (full path): Copied GPKG FC '%s' to '%s'",
                    full_path,
                    target_name)
                lg_sum.info("   📄 GPKG ➜ staged : %s", target_name)
                summary.log_staging("done")
                return

    # All strategies failed
    log.error("❌ All copy strategies failed for GPKG FC '%s'", input_fc_name)
    summary.log_staging("error")
    summary.log_error(input_fc_name, "All GPKG copy strategies failed")


@safe_arcpy_operation
def _attempt_copy_with_name(
        source_name: str,
        target_name: str,
        gdb_path: Path) -> bool:
    """🔄 Attempt to copy a feature class with a specific source name."""
    try:
        # First validate that the source exists
        if not arcpy.Exists(source_name):
            log.debug(
                "🔍 Source '%s' does not exist, skipping attempt",
                source_name)
            return False

        log.info(
            "📥 Copying GPKG FC ('%s') → GDB:/'%s'",
            source_name,
            target_name)
        with arcpy.EnvManager(overwriteOutput=True):
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=source_name,
                out_path=str(gdb_path),
                out_name=target_name
            )
        return True

    except arcpy.ExecuteError as arc_error:
        arcpy_messages: str = arcpy.GetMessages(2)
        log.debug("🔄 Copy attempt failed for '%s': %s. ArcPy Messages: %s",
                  source_name, arc_error, arcpy_messages)
        return False

    except Exception as generic_error:
        log.debug("🔄 Copy attempt failed for '%s' with unexpected error: %s",
                  source_name, generic_error)
        return False


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
            log.info(
                "⏭️ Skipping GPKG FC '%s' (normalized: '%s') as it's not in the include filter.",
                fc_name_listed_by_arcpy,
                comparable_fc_name)
            return
        else:
            log.info(
                "✅ GPKG FC '%s' (normalized: '%s') is in include filter. Proceeding.",
                fc_name_listed_by_arcpy,
                comparable_fc_name)

    if stem_for_output_naming != fc_name_listed_by_arcpy:
        log.info("Stripped 'main.' from '%s' → '%s' for output naming",
                 fc_name_listed_by_arcpy, stem_for_output_naming)

    base_name: str = generate_fc_name(authority, stem_for_output_naming)
    tgt_name: str = ensure_unique_name(base_name, used_names_set)

    copy_gpkg_feature_class(
        fc_name_listed_by_arcpy,
        tgt_name,
        gdb_path,
        summary)


def process_gpkg_contents(
    gpkg_file_path: Path,
    authority: str,
    gdb_path: Path,
    used_names_set: Set[str],
    summary: Summary,
    include_filter: Optional[List[str]] = None
) -> None:
    """📦 Copy GPKG contents to staging GDB with comprehensive error handling."""
    log.info(
        "📦 Processing GeoPackage: %s (Authority: '%s', Include Filter: %s)",
        gpkg_file_path.name,
        authority,
        include_filter or "None")

    if not gpkg_file_path.exists():
        log.error("❌ GeoPackage file does not exist: %s", gpkg_file_path)
        return

    # Use context manager for safe workspace management
    with arcpy_workspace(gpkg_file_path, overwrite_output=True):
        try:
            feature_classes_in_gpkg: List[str] = arcpy.ListFeatureClasses()
            if not feature_classes_in_gpkg:
                log.info(
                    "ℹ️ No feature classes found in GeoPackage: %s",
                    gpkg_file_path.name)
                return

            log.info(
                "Found %d feature classes in %s: %s",
                len(feature_classes_in_gpkg),
                gpkg_file_path.name,
                feature_classes_in_gpkg)

            # Normalize include filter for comparison BEFORE validation
            normalized_include_filter: Optional[Set[str]] = None
            if include_filter:
                normalized_include_filter = {_MAIN_RE.sub(
                    "", item).lower() for item in include_filter if item}
                log.info(
                    "Normalized include filter for %s: %s",
                    gpkg_file_path.name,
                    normalized_include_filter)

                # Pre-filter feature classes based on include filter
                if normalized_include_filter:
                    filtered_feature_classes = []
                    for fc_name in feature_classes_in_gpkg:
                        stem_for_comparison = _MAIN_RE.sub("", fc_name).lower()
                        if stem_for_comparison in normalized_include_filter:
                            filtered_feature_classes.append(fc_name)
                            log.info(
                                "✅ Including GPKG FC '%s' (matches filter)", fc_name)
                        else:
                            log.info(
                                "⏭️ Excluding GPKG FC '%s' (not in include filter)", fc_name)

                    feature_classes_in_gpkg = filtered_feature_classes
                    log.info(
                        "📦 After applying include filter: %d feature classes remain",
                        len(feature_classes_in_gpkg))

            if not feature_classes_in_gpkg:
                log.info(
                    "ℹ️ No feature classes match the include filter in GeoPackage: %s",
                    gpkg_file_path.name)
                return

            # Validate that feature classes actually exist and can be accessed
            valid_feature_classes: List[str] = []
            for fc_name in feature_classes_in_gpkg:
                if _validate_gpkg_feature_class(fc_name):
                    valid_feature_classes.append(fc_name)
                    log.debug("✅ Validated GPKG FC: %s", fc_name)
                else:
                    log.warning(
                        "⚠️ Skipping inaccessible GPKG FC: %s", fc_name)

            if not valid_feature_classes:
                log.warning(
                    "⚠️ No accessible feature classes found in GeoPackage: %s",
                    gpkg_file_path.name)
                return

            log.info("📦 Processing %d valid feature classes from %s",
                     len(valid_feature_classes), gpkg_file_path.name)

            for fc_name_listed_by_arcpy in valid_feature_classes:
                process_gpkg_feature_class(
                    fc_name_listed_by_arcpy,
                    authority,
                    gdb_path,
                    used_names_set,
                    summary,
                    normalized_include_filter)

        except Exception as gpkg_processing_error:
            log.error(
                "❌ Failed to list or process feature classes in GeoPackage '%s': %s",
                gpkg_file_path.name,
                gpkg_processing_error,
                exc_info=True)


def _validate_gpkg_feature_class(fc_name: str) -> bool:
    """🔍 Validate that a GPKG feature class can be accessed."""
    # Try multiple approaches to validate GPKG feature class existence
    candidates = [fc_name]

    # Also try stripped name if it has 'main.' prefix
    stripped_name = _MAIN_RE.sub("", fc_name)
    if stripped_name != fc_name:
        candidates.append(stripped_name)

    # Try with full workspace path
    current_workspace = arcpy.env.workspace  # type: ignore[attr-defined]
    if current_workspace:
        for candidate in [fc_name, stripped_name]:
            candidates.append(f"{current_workspace}\\{candidate}")

    for candidate in candidates:
        try:
            if arcpy.Exists(candidate):
                log.debug(
                    "🔍 GPKG FC validation SUCCESS: '%s' accessible as '%s'",
                    fc_name,
                    candidate)
                return True
            # Also try Describe as a secondary check
            desc = arcpy.Describe(candidate)
            if desc:
                log.debug(
                    "🔍 GPKG FC validation SUCCESS via Describe: '%s' accessible as '%s'",
                    fc_name,
                    candidate)
                return True
        except Exception:
            continue

    log.debug(
        "🔍 GPKG FC validation FAILED: '%s' not accessible via any method",
        fc_name)
    return False
