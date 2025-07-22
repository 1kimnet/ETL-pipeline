# etl/handlers/geoprocess.py (simplified and clean)
from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, Dict, List

import arcpy

log: Final = logging.getLogger("summary")


def geoprocess_staging_gdb(
    staging_gdb: Path | str,
    aoi_fc: Path | str,
    target_srid: int = 3010,  # Changed to SWEREF99 12 00
    pp_factor: str = "100",
) -> None:
    """🔄 In-place geoprocessing of staging.gdb: clip and project only.

    Args:
        staging_gdb: Path to staging.gdb to process in-place
        aoi_fc: Area of interest feature class for clipping
        target_srid: Target spatial reference ID (default: 3010 = SWEREF99 12 00)
        pp_factor: Parallel processing factor ("100" = all cores)

    Processes staging.gdb in-place: clip → project.
    """
    # Validate inputs
    staging_gdb_path = Path(staging_gdb)
    aoi_fc_path = Path(aoi_fc)

    if not staging_gdb_path.exists():
        raise FileNotFoundError(f"Staging GDB not found: {staging_gdb_path}")
    if not aoi_fc_path.exists():
        raise FileNotFoundError(f"AOI feature class not found: {aoi_fc_path}")

    log.info("🔄 Starting in-place geoprocessing of %s", staging_gdb_path.name)

    # Configure environment using EnvManager
    with arcpy.EnvManager(
        workspace=str(staging_gdb_path),
        outputCoordinateSystem=arcpy.SpatialReference(target_srid),
        overwriteOutput=True,
        parallelProcessingFactor=pp_factor
    ):
        # Get list of feature classes to process
        original_fcs = arcpy.ListFeatureClasses()
        if not original_fcs:
            log.warning("⚠️ No feature classes found in %s", staging_gdb_path)
            return

        log.info(
            "🔄 Processing %d feature classes: clip + project only",
            len(original_fcs))

        # Clip and project all FCs
        clip_and_project_fcs(original_fcs, aoi_fc_path)

        log.info("✅ Geoprocessing complete for %s", staging_gdb_path.name)


def clip_and_project_fcs(feature_classes: List[str], aoi_fc: Path) -> None:
    """🔄 Clip and project all feature classes in-place."""
    log.info("✂️ Clipping and projecting feature classes")

    processed_count = 0
    error_count = 0

    for fc_name in feature_classes:
        try:
            # Create temporary clipped version
            temp_clipped = f"in_memory\\{fc_name}_temp"

            # Clip (projection handled by environment)
            arcpy.analysis.PairwiseClip(fc_name, str(aoi_fc), temp_clipped)

            # Replace original with clipped version
            arcpy.management.Delete(fc_name)
            arcpy.management.CopyFeatures(temp_clipped, fc_name)

            # Clean up temp
            arcpy.management.Delete(temp_clipped)

            log.info("   ✂️ clipped & projected ➜ %s", fc_name)
            processed_count += 1

        except arcpy.ExecuteError:
            log.error(
                "   ❌ failed to process %s: %s",
                fc_name,
                arcpy.GetMessages(2))
            error_count += 1

    log.info(
        "📊 Clip/project complete: %d processed, %d errors",
        processed_count,
        error_count)


def create_naming_rules_from_config(config: Dict) -> Dict[str, Dict[str, str]]:
    """🔧 Placeholder function for future naming rules (currently unused)."""
    # Simplified - no longer used
    return {}
