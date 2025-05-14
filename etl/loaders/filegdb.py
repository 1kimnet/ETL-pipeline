"""Loader that consolidates all staged shapefiles into *staging.gdb* using ArcPy.

This **updated** version adds:
* detailed ArcPy error capture with source file context
* duplicate‑safe, FGDB‑legal layer names
* `arcpy.env.overwriteOutput = True` so reruns don’t crash
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Set

import arcpy  # ArcGIS Pro / Server Python ships with this

from ..utils import paths, ensure_dirs


class ArcPyFileGDBLoader:  # noqa: D101
    """Build (or rebuild) *staging.gdb* from everything under *data/staging*."""

    def __init__(self, gdb_path: Path | None = None):
        ensure_dirs()
        self.gdb_path = gdb_path or paths.GDB

    # ---------------------------------------------------------------- public

    def load_from_staging(self, staging_root: Path) -> None:  # noqa: D401
        """Recreate the FileGDB and copy every `.shp` inside *staging_root*."""
        self._reset_gdb()

        shp_files = list(staging_root.rglob("*.shp"))
        if not shp_files:
            logging.warning("No shapefiles found in %s", staging_root)
            return

        arcpy.env.overwriteOutput = True  # type: ignore[attr-defined]
        arcpy.env.workspace = str(self.gdb_path)  # type: ignore[attr-defined]

        used_names: Set[str] = set()
        for shp in shp_files:
            tgt_name = self._safe_name(shp.stem, used_names)
            logging.info("📥 Copying %s → %s", shp.relative_to(paths.ROOT), tgt_name)
            try:
                arcpy.management.CopyFeatures(str(shp), tgt_name)
            except arcpy.ExecuteError:  # type: ignore[attr-defined]
                logging.error(
                    "❌ %s → %s failed: %s", shp.name, tgt_name, arcpy.GetMessages(2)
                )
                continue

    # ---------------------------------------------------------------- internals

    def _reset_gdb(self) -> None:
        """Delete and recreate the destination GDB fresh for this run.
        Any ArcPy error is logged with *full* tool messages then re‑raised so
        the caller can surface it (and Pipeline will catch & report).
        """
        try:
            if self.gdb_path.exists():
                logging.info("🗑️ Removing existing %s", self.gdb_path.name)
                shutil.rmtree(self.gdb_path)
            logging.info("🆕 Creating %s", self.gdb_path.name)
            arcpy.management.CreateFileGDB(self.gdb_path.parent, self.gdb_path.name)
        except arcpy.ExecuteError:  # type: ignore[attr-defined]
            msg = arcpy.GetMessages(2)
            logging.error("❌ CreateFileGDB failed: %s", msg)
            raise RuntimeError(msg) from None

    @staticmethod
    def _safe_name(stem: str, used: Set[str]) -> str:
        """Generate a unique, <= 60‑char FGDB layer name."""
        stem = stem[:55]  # leave room for suffix
        candidate = stem or "layer"
        idx = 1
        while candidate.lower() in (n.lower() for n in used):
            candidate = f"{stem}_{idx}"
            idx += 1
        used.add(candidate)
        return candidate
