"""Loader that consolidates all staged shapefiles into *staging.gdb* using ArcPy.

This **updated** version:
* Uses a dedicated naming utility for cleaner code.
* Implements [AUTHORITY]_[SANITIZED_NAME] convention.
* Sanitizes Swedish characters (åäö).
* Includes detailed ArcPy error capture and enhanced logging.
* Sets `arcpy.env.overwriteOutput = True`.
"""

from __future__ import annotations

import logging
import shutil
import re
from pathlib import Path
from typing import Set, Final

import arcpy  # ArcGIS Pro / Server Python ships with this

from ..utils import paths, ensure_dirs
from ..utils.naming import (
    generate_base_feature_class_name,
    DEFAULT_MAX_FC_NAME_LENGTH,
)

_MAIN_RE: Final = re.compile(r"^main_?", re.IGNORECASE)
class ArcPyFileGDBLoader:  # noqa: D101
    """Build (or rebuild) *staging.gdb* from everything under *data/staging*."""

    def __init__(self, gdb_path: Path | None = None):
        ensure_dirs()
        self.gdb_path = gdb_path or paths.GDB

    # ---------------------------------------------------------------- public

    def load_from_staging(self, staging_root: Path) -> None:  # noqa: D401
        """Recreate the FileGDB and copy every `.shp` and `.gpkg` inside *staging_root*."""
        self._reset_gdb()

        used_names: Set[str] = set()

        # -- GeoPackages ---------------------------------------------------
        gpkg_files = list(staging_root.rglob("*.gpkg"))
        for gpkg in gpkg_files:
            authority = gpkg.relative_to(staging_root).parts[0]
            self._copy_gpkg_into_gdb(gpkg, authority, used_names)

        # -- Shapefiles ----------------------------------------------------
        shp_files = list(staging_root.rglob("*.shp"))
        if not shp_files:
            logging.warning("⚠️ No shapefiles found in %s to load into GDB", staging_root)
            return
        arcpy.env.overwriteOutput = True  # type: ignore[attr-defined]
        arcpy.env.workspace = str(self.gdb_path)  # type: ignore[attr-defined]

        used_names: Set[str] = set()
        for shp in shp_files:
            tgt_name = "unknown_target" # Initialize for robust error logging
            authority = "unknown_authority" # Initialize authority for robust error logging
            try:
                # Extract authority from the path structure: staging_root/AUTHORITY/dataset_name/file.shp
                relative_path = shp.relative_to(staging_root)
                if not relative_path.parts:
                    logging.error("❌ Could not determine authority for %s, skipping.", shp)
                    continue
                authority = relative_path.parts[0]

                # Generate the base name using the utility function
                # _ensure_unique_name will now primarily handle uniqueness and final length.
                # Pass the desired final max length to generate_base_feature_class_name
                # so it can reserve space for suffixes correctly.
                base_name = generate_base_feature_class_name(
                    shp.stem, 
                    authority, 
                    max_length=DEFAULT_MAX_FC_NAME_LENGTH # Use constant from naming.py
                ) 
                tgt_name = self._ensure_unique_name(
                    base_name, 
                    used_names, 
                    max_length=DEFAULT_MAX_FC_NAME_LENGTH # Use constant from naming.py
                )
                
                logging.info("📥 Copying %s → %s (Original Stem: %s, Authority: %s, Base Name: %s)",
                             shp.relative_to(paths.ROOT), tgt_name, shp.stem, authority, base_name)
                arcpy.management.CopyFeatures(str(shp), tgt_name)
            except arcpy.ExecuteError:  # type: ignore[attr-defined]
                logging.error(
                    "❌ CopyFeatures failed for %s → %s: %s", shp.name, tgt_name, arcpy.GetMessages(2)
                )
                continue # Continue with the next shapefile
            except ValueError as ve: # Catch naming errors from generate_base_feature_class_name or _ensure_unique_name
                logging.error("❌ Naming error for original stem '%s' (authority '%s'): %s", shp.stem, authority, ve)
                continue # Continue with the next shapefile
            except Exception as e: # Catch any other unexpected errors
                logging.error(
                    "❌ Unexpected error processing %s (target %s): %s", shp.name, tgt_name, e, exc_info=True
                )
                continue # Continue with the next shapefile


    # ---------------------------------------------------------------- internals

    def _reset_gdb(self) -> None:
        """
        Delete and recreate the destination GDB fresh for this run.
        Logs detailed messages and re-raises critical errors.
        """
        gdb_full_path = self.gdb_path.resolve() 
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
        except arcpy.ExecuteError: 
            msg = arcpy.GetMessages(2) 
            logging.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s", gdb_full_path, msg)
            raise RuntimeError(f"CreateFileGDB failed for '{gdb_full_path}': {msg}") from None
        except Exception as e: 
            logging.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s", gdb_full_path, e, exc_info=True)
            raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_full_path}': {e}") from e
    
    def _copy_gpkg_into_gdb(
        self,
        gpkg: Path,
        authority: str,
        used_names: Set[str],
    ) -> None:
        """🔄 Copy every layer in *gpkg* into *self.gdb_path* with cleaned names."""
        arcpy.env.workspace = str(gpkg)  # type: ignore[attr-defined]

        for fc in arcpy.ListFeatureClasses():  # type: ignore[attr-defined]
            cleaned_stem = _MAIN_RE.sub("", fc)
            base = generate_base_feature_class_name(
                cleaned_stem,
                authority,
                max_length=DEFAULT_MAX_FC_NAME_LENGTH,
            )
            tgt_name = self._ensure_unique_name(base, used_names)
            logging.info("📥 %s/%s → %s", gpkg.name, fc, tgt_name)
            arcpy.conversion.FeatureClassToFeatureClass(
                fc, str(self.gdb_path), tgt_name
            )  # type: ignore[attr-defined]

    @staticmethod
    def _ensure_unique_name(base_name: str, used_names: Set[str], max_length: int = DEFAULT_MAX_FC_NAME_LENGTH) -> str:
        """
        Ensures the generated feature class name is unique by appending a suffix (_1, _2, etc.)
        if necessary. Also ensures the final name does not exceed max_length.

        Args:
            base_name: The proposed base name (already sanitized, prefixed, and length-adjusted
                       by generate_base_feature_class_name to reserve space for suffixes).
            used_names: A set of already used names in the current GDB context (case-insensitive check).
            max_length: The absolute maximum length for the final feature class name.

        Returns:
            A unique feature class name.

        Raises:
            ValueError: If a unique name cannot be generated within the length constraints.
        """
        candidate = base_name
        
        # The base_name from generate_base_feature_class_name should already be truncated 
        # to max_length - SUFFIX_RESERVATION_LENGTH.
        # This _ensure_unique_name function primarily handles adding suffixes and ensuring
        # the *final* name with suffix fits max_length.

        if len(candidate) > max_length: # Should ideally not happen if base_name was prepared correctly
            logging.warning(
                "⚠️ Base name '%s' (length %d) exceeds max_length %d even before adding suffix. Truncating.",
                candidate, len(candidate), max_length
            )
            candidate = candidate[:max_length]
            if not candidate:
                 raise ValueError(f"Base name '{base_name}' became empty after initial truncation to {max_length} chars.")
        
        final_candidate = candidate # Start with the (potentially truncated) base name
        idx = 1
        # Perform a case-insensitive check for used names for broader compatibility,
        # even though FGDB names are technically case-sensitive.
        while final_candidate.lower() in (n.lower() for n in used_names):
            suffix = f"_{idx}"
            
            # Check if the original candidate (which is base_name or truncated base_name) plus suffix is too long
            if len(candidate) + len(suffix) > max_length:
                # If so, we need to shorten 'candidate' to make space for this suffix
                # This means the original base_name was too long to begin with, even with suffix reservation.
                chars_to_remove = (len(candidate) + len(suffix)) - max_length
                current_stem_part = candidate # This is the part we are trying to fit
                
                if chars_to_remove >= len(current_stem_part): # Cannot make it fit
                    raise ValueError(
                        f"Cannot generate a unique name for base '{base_name}' (stem part '{current_stem_part}') "
                        f"under {max_length} chars with suffix '{suffix}'. Too many characters to remove."
                    )
                
                truncated_stem_part = current_stem_part[:-chars_to_remove]
                final_candidate = f"{truncated_stem_part}{suffix}"

                if not truncated_stem_part: # If the stem part became empty
                     raise ValueError(
                        f"Cannot generate a unique name for base '{base_name}'. "
                        f"Stem part became empty after trying to fit suffix '{suffix}' within {max_length} chars."
                    )
            else:
                # The original candidate (base_name or truncated base_name) has enough space for this suffix
                final_candidate = f"{candidate}{suffix}"
            
            idx += 1
            if idx > 9999: # Safety break to prevent infinite loops
                raise ValueError(f"Could not find a unique name for base '{base_name}' after {idx-1} attempts within {max_length} chars.")

        used_names.add(final_candidate)
        return final_candidate
