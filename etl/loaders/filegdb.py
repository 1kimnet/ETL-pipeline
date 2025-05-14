"""Loader that consolidates all staged shapefiles into *staging.gdb* using ArcPy.

This **updated** version adds:
* detailed ArcPy error capture with source file context
* duplicate‑safe, FGDB‑legal layer names
* `arcpy.env.overwriteOutput = True` so reruns don’t crash
* Enhanced logging in _reset_gdb to pinpoint creation/deletion issues.
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
        self._reset_gdb() # This will now raise an error if it fails, stopping before CopyFeatures

        shp_files = list(staging_root.rglob("*.shp"))
        if not shp_files:
            logging.warning("⚠️ No shapefiles found in %s to load into GDB", staging_root)
            return

        logging.info("⚙️ Setting ArcPy environment for GDB loading...")
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
                    "❌ CopyFeatures failed for %s → %s: %s", shp.name, tgt_name, arcpy.GetMessages(2)
                )
                # Optionally, re-raise or continue based on project requirements
                # For now, it continues to allow other files to be processed.
                # raise # Uncomment to stop on first CopyFeatures error
                continue 
            except Exception as e:
                logging.error(
                    "❌ Unexpected error during CopyFeatures for %s → %s: %s", shp.name, tgt_name, e, exc_info=True
                )
                # raise # Uncomment to stop on first unexpected error
                continue


    # ---------------------------------------------------------------- internals

    def _reset_gdb(self) -> None:
        """
        Delete and recreate the destination GDB fresh for this run.
        Any ArcPy error is logged with *full* tool messages then re‑raised so
        the caller can surface it (and Pipeline will catch & report).
        Other exceptions during deletion are also logged and re-raised.
        """
        gdb_full_path = self.gdb_path.resolve() # Get absolute path for clarity
        logging.info("ℹ️ Target GDB path for reset: %s", gdb_full_path)

        if self.gdb_path.exists():
            logging.info("🗑️ Attempting to remove existing GDB: %s", gdb_full_path)
            try:
                shutil.rmtree(self.gdb_path)
                logging.info("✅ Successfully removed existing GDB: %s", gdb_full_path)
            except Exception as e:
                logging.error("❌ Failed to remove existing GDB '%s': %s", gdb_full_path, e, exc_info=True)
                raise RuntimeError(f"Failed to remove existing GDB '{gdb_full_path}': {e}") from e
        else:
            logging.info("ℹ️ GDB does not currently exist at %s, no removal needed.", gdb_full_path)

        # Ensure parent directory exists (though ensure_dirs should have handled paths.DATA)
        parent_dir = self.gdb_path.parent.resolve()
        if not parent_dir.exists():
            logging.info("🆕 Parent directory %s for GDB does not exist. Attempting to create.", parent_dir)
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
                logging.info("✅ Successfully created parent directory: %s", parent_dir)
            except Exception as e:
                logging.error("❌ Failed to create parent directory '%s' for GDB: %s", parent_dir, e, exc_info=True)
                raise RuntimeError(f"Failed to create parent directory '{parent_dir}' for GDB: {e}") from e
        
        logging.info("🆕 Attempting to create new FileGDB: %s in folder %s", self.gdb_path.name, self.gdb_path.parent.resolve())
        try:
            arcpy.management.CreateFileGDB(str(self.gdb_path.parent), self.gdb_path.name)
            logging.info("✅ Successfully created new GDB: %s", gdb_full_path)
        except arcpy.ExecuteError:  # type: ignore[attr-defined]
            msg = arcpy.GetMessages(2) # Get detailed ArcPy error messages
            logging.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s", gdb_full_path, msg)
            # Re-raise with the specific ArcPy message
            raise RuntimeError(f"CreateFileGDB failed for '{gdb_full_path}': {msg}") from None
        except Exception as e: # Catch any other unexpected errors during GDB creation
            logging.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s", gdb_full_path, e, exc_info=True)
            raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_full_path}': {e}") from e

    @staticmethod
    def _safe_name(stem: str, used: Set[str]) -> str:
        """Generate a unique, <= 60‑char FGDB layer name."""
        # FGDB layer names are max 160 characters, but feature class names within a FGDB are often recommended to be shorter.
        # ArcGIS itself might truncate or have issues with very long names. Let's aim for a reasonable limit.
        # Common practical limit for shapefiles is ~10 chars for field names, GDB is more generous.
        # Let's keep it well under any theoretical GDB limits for safety, e.g. 60 chars as a practical limit.
        stem = "".join(c if c.isalnum() or c == "_" else "_" for c in stem) # Sanitize
        
        if not stem: # Handle empty stem after sanitization
            stem = "layer"

        stem = stem[:55]  # leave room for suffix like "_1", "_2"
        
        candidate = stem
        idx = 1
        # Check against lowercased versions for case-insensitivity if needed, though FGDB names are case-sensitive
        # For simplicity here, direct check.
        while candidate in used: # FGDB names are case-sensitive, so direct check is fine.
            candidate = f"{stem}_{idx}"
            if len(candidate) > 60: # If suffix makes it too long, truncate stem further
                stem = stem[:-(len(candidate) - 60)]
                candidate = f"{stem}_{idx}" # Recalculate candidate
                if not stem: # Safety break if stem becomes empty
                    raise ValueError("Cannot generate a safe unique name under 60 chars.")
            idx += 1
        used.add(candidate)
        return candidate
