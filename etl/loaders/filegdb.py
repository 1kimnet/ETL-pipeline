"""Loader that consolidates all staged shapefiles into *staging.gdb* using ArcPy."""

import logging
import shutil
from pathlib import Path

import arcpy  # ArcGIS Pro / Server Python ships with this

from ..utils import paths, ensure_dirs


class ArcPyFileGDBLoader:  # noqa: D101
    def __init__(self, gdb_path: Path | None = None):
        ensure_dirs()
        self.gdb_path = gdb_path or paths.GDB

    # ---------------------------------------------------------------- public

    def load_from_staging(self, staging_root: Path) -> None:  # noqa: D401
        """Rebuild the GDB and copy every *.shp* in *staging_root*."""
        self._reset_gdb()
        shp_files = list(staging_root.rglob("*.shp"))
        if not shp_files:
            logging.warning("No shapefiles found in %s", staging_root)
            return

        for shp in shp_files:
            out_name = shp.stem  # keep original layer name
            logging.info("📥 Copying %s → %s", shp.relative_to(paths.ROOT), out_name)
            arcpy.management.CopyFeatures(shp, str(self.gdb_path / out_name))

    # ---------------------------------------------------------------- internals

    def _reset_gdb(self) -> None:
        if self.gdb_path.exists():
            logging.info("🗑️ Removing existing %s", self.gdb_path.name)
            shutil.rmtree(self.gdb_path)
        logging.info("🆕 Creating %s", self.gdb_path.name)
        arcpy.management.CreateFileGDB(self.gdb_path.parent, self.gdb_path.name)
