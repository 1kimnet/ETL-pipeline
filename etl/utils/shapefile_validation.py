# etl/utils/validation.py
"""🔍 File validation utilities for ETL pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, List
from dataclasses import dataclass

log: Final = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ShapefileValidationResult:
    """Result of shapefile component validation with comprehensive error details."""
    is_valid: bool
    error_message: str
    missing_components: List[str]


def validate_shapefile_components(
        shp_file_path: Path) -> ShapefileValidationResult:
    """🔍 Validate that all required shapefile components exist.

    Args:
        shp_file_path: Path to the .shp file to validate.

    Returns:
        ShapefileValidationResult with validation status and details.
    """
    shp_stem: str = shp_file_path.stem
    shp_directory: Path = shp_file_path.parent

    required_extensions: List[str] = ['.shx', '.dbf']
    missing_components: List[str] = []

    log.debug(
        "🔍 Validating shapefile components for stem: '%s' in directory: %s",
        shp_stem,
        shp_directory)

    for ext in required_extensions:
        component_file: Path = shp_directory / f"{shp_stem}{ext}"
        log.debug("   Checking for component file: %s", component_file)

        if not component_file.exists():
            missing_components.append(ext)
            log.debug("   ❌ Missing component: %s", component_file)
        else:
            log.debug("   ✅ Found component: %s", component_file)

    if missing_components:
        error_msg: str = f"Missing required components: {', '.join(missing_components)}"
        return ShapefileValidationResult(False, error_msg, missing_components)

    log.debug(
        "✅ All shapefile components validated for: %s",
        shp_file_path.name)
    return ShapefileValidationResult(True, "All components present", [])


def find_alternative_shapefile(directory: Path) -> Path | None:
    """🔍 Find a valid alternative shapefile in the given directory.

    Args:
        directory: Directory to search for valid shapefiles.

    Returns:
        Path to a valid shapefile, or None if none found.
    """
    if not directory.exists():
        return None

    shp_files: List[Path] = list(directory.glob("*.shp"))
    log.debug(
        "🔍 Found %d shapefile(s) in directory %s",
        len(shp_files),
        directory)

    for shp_file in shp_files:
        log.debug("   Validating shapefile: %s", shp_file.name)
        validation_result: ShapefileValidationResult = validate_shapefile_components(
            shp_file)
        if validation_result.is_valid:
            log.info("✅ Found valid shapefile: %s", shp_file.name)
            return shp_file
        else:
            log.debug(
                "   ❌ Invalid shapefile %s: %s",
                shp_file.name,
                validation_result.error_message)

    log.warning("⚠️ No valid shapefiles found in directory: %s", directory)
    return None


def log_directory_contents(directory: Path, context: str) -> None:
    """🔍 Log directory contents for debugging purposes.

    Args:
        directory: Directory to list contents for.
        context: Context string for logging.
    """
    if not directory.exists():
        log.debug("   Directory does not exist for logging: %s", directory)
        return

    try:
        files: List[Path] = list(directory.iterdir())
        log.debug("   Directory contents (%s) for %s:", context, directory)
        for file_path in sorted(files):
            if file_path.is_file():
                log.debug(
                    "     📄 %s (%d bytes)",
                    file_path.name,
                    file_path.stat().st_size)
            elif file_path.is_dir():
                log.debug("     📁 %s/", file_path.name)
    except Exception as e:
        log.warning(
            "   ⚠️ Could not list directory contents for %s: %s",
            directory,
            e)
