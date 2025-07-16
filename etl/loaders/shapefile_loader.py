# etl/loaders/shapefile_loader.py
"""📐 Shapefile format loader."""

from __future__ import annotations

import logging
import shutil
import uuid
import zipfile
from pathlib import Path
from typing import Final, List, Optional

import arcpy

from etl.models import Source
from etl.utils import paths
from etl.utils.gdb_utils import ensure_unique_name
from etl.utils.naming import generate_fc_name, sanitize_for_arcgis_name

log: Final = logging.getLogger(__name__)


def _copy_to_temp_shapefile(source_path: Path, authority: str) -> tuple[Path, Path]:
    """Copy shapefile to a temporary, sanitized location if its name is invalid."""
    base_name = sanitize_for_arcgis_name(source_path.stem)
    generated_name = f"{authority.lower()}_{base_name}"

    temp_dir = paths.TEMP / f"shp_{uuid.uuid4().hex[:8]}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        out_shp = temp_dir / f"{generated_name}.shp"
        # Use arcpy.Copy_management as it robustly handles all shapefile sidecar files
        arcpy.management.Copy(in_data=str(source_path), out_data=str(out_shp))
        log.info("✅ Copied shapefile to temporary location: %s", out_shp)
        return out_shp, temp_dir
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


class ShapefileLoader:
    """🔌 Shapefile loader class."""

    def __init__(self, src: Source):
        self.src = src

    def load(self, used_names: set[str]) -> None:
        """Load shapefiles from the source's staging directory."""
        from etl.utils.naming import sanitize_for_filename

        # Calculate the staging directory path for this source
        source_staging_dir = (
            paths.STAGING / self.src.authority / sanitize_for_filename(self.src.name)
        )

        if not source_staging_dir.exists():
            log.warning(
                "No staging directory found for source '%s' at %s",
                self.src.name,
                source_staging_dir,
            )
            return

        # Find all files and directories to process in the staging directory
        items_to_process = []

        # Look for zip files first
        zip_files = list(source_staging_dir.glob("*.zip"))
        items_to_process.extend(zip_files)

        # Look for subdirectories (from extracted archives or multi-part downloads)
        subdirs = [p for p in source_staging_dir.iterdir() if p.is_dir()]
        items_to_process.extend(subdirs)

        # If no items found, look for shapefiles directly in the staging directory
        if not items_to_process:
            shapefiles = list(source_staging_dir.glob("*.shp"))
            if shapefiles:
                # Treat the staging directory itself as the item to process
                items_to_process.append(source_staging_dir)

        if not items_to_process:
            log.warning(
                "No items to process for source '%s' in %s",
                self.src.name,
                source_staging_dir,
            )
            return

        log.info(
            "Found %d item(s) to process for source '%s'",
            len(items_to_process),
            self.src.name,
        )

        for item_path in items_to_process:
            self._process_item(item_path, used_names)

    def _find_shapefiles(self, directory: Path) -> List[Path]:
        """Find all .shp files in a directory, including subdirectories."""
        return list(directory.rglob("*.shp"))

    def _process_item(self, item_path: Path, used_names: set[str]) -> None:
        """Process a single downloaded item (zip file or directory)."""
        item_dir = item_path
        temp_unzip_dir: Optional[Path] = None

        if zipfile.is_zipfile(item_path):
            temp_unzip_dir = (
                paths.TEMP / f"unzip_{item_path.stem}_{uuid.uuid4().hex[:8]}"
            )
            log.info("📦 Unzipping '%s' to '%s'", item_path.name, temp_unzip_dir)
            with zipfile.ZipFile(item_path, "r") as zip_ref:
                zip_ref.extractall(temp_unzip_dir)
            item_dir = temp_unzip_dir
        elif not item_path.is_dir():
            log.error(
                "❌ Item '%s' is not a directory or a zip file, skipping.", item_path
            )
            return

        shapefiles = self._find_shapefiles(item_dir)
        if not shapefiles:
            log.warning("⚠️ No shapefiles found in '%s'.", item_dir.name)
            return

        log.info(
            "📐 Found %d shapefile(s) in item dir '%s'.", len(shapefiles), item_dir.name
        )
        for shp_file in shapefiles:
            self.process_shapefile(shp_file, used_names)

        if temp_unzip_dir:
            shutil.rmtree(temp_unzip_dir, ignore_errors=True)

    def process_shapefile(self, shp_file_path: Path, used_names: set[str]) -> None:
        """Process a single shapefile."""
        working_path = shp_file_path
        temp_copy_dir: Optional[Path] = None

        try:
            if not arcpy.Exists(str(shp_file_path)):
                log.warning(
                    "Shapefile name '%s' incompatible with ArcGIS. Using temporary copy.",
                    shp_file_path.name,
                )
                working_path, temp_copy_dir = _copy_to_temp_shapefile(
                    shp_file_path, self.src.authority
                )

            fc_name_base = generate_fc_name(self.src.authority, working_path.stem)
            target_fc_name = ensure_unique_name(
                base_name=fc_name_base, used_names=used_names, max_length=60
            )

            log.info(
                "📥 Copying SHP ('%s') → GDB:/'%s' (Authority: '%s')",
                working_path.name,
                target_fc_name,
                self.src.authority,
            )

            with arcpy.EnvManager(overwriteOutput=True):
                arcpy.management.CopyFeatures(
                    in_features=str(working_path),
                    out_feature_class=str(paths.GDB / target_fc_name),
                )
            log.info(
                "✅ SUCCESS: Copied shapefile '%s' to '%s'",
                working_path.name,
                target_fc_name,
            )
        except arcpy.ExecuteError as arc_error:
            log.error(
                "❌ ArcPy error processing SHP %s: %s", working_path.name, arc_error
            )
        except (OSError, IOError, ValueError, RuntimeError) as processing_error:
            log.error(
                "❌ Error processing SHP %s: %s",
                working_path.name,
                processing_error,
                exc_info=True,
            )
        finally:
            if temp_copy_dir:
                shutil.rmtree(temp_copy_dir, ignore_errors=True)
