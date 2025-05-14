"""Loader that consolidates all staged data (Shapefiles and GeoPackages) 
into *staging.gdb* using ArcPy.

This **updated** version:
* Fixes GPKG input feature class name for FeatureClassToFeatureClass.
* Adds # type: ignore[attr-defined] for arcpy.env.workspace assignments.
* Corrects `used_names` scope to ensure uniqueness across all sources.
* Adds specific error logging for GeoPackage feature class conversion.
* Improves arcpy.env.workspace management.
* Uses a dedicated naming utility.
* Implements [AUTHORITY]_[SANITIZED_NAME] convention.
* Sanitizes Swedish characters (åäö).
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

# Regex to find "main." prefix, case-insensitive
_MAIN_RE: Final = re.compile(r"^main\.", re.IGNORECASE) 

class ArcPyFileGDBLoader:  # noqa: D101
    """Build (or rebuild) *staging.gdb* from everything under *data/staging*."""

    def __init__(self, gdb_path: Path | None = None):
        ensure_dirs()
        self.gdb_path = gdb_path or paths.GDB

    # ---------------------------------------------------------------- public

    def load_from_staging(self, staging_root: Path) -> None:  # noqa: D401
        """Recreate the FileGDB and copy every `.shp` and `.gpkg` content from *staging_root*."""
        self._reset_gdb()

        used_names_in_staging_gdb: Set[str] = set()
        
        arcpy.env.overwriteOutput = True  # type: ignore[attr-defined]
        original_workspace = arcpy.env.workspace # type: ignore[attr-defined] 

        # -- GeoPackages ---------------------------------------------------
        gpkg_files = list(staging_root.rglob("*.gpkg"))
        if gpkg_files:
            logging.info("ℹ️ Found %d GeoPackage(s) to process.", len(gpkg_files))
            for gpkg_file_path in gpkg_files:
                authority = "UNKNOWN_AUTHORITY"
                try:
                    relative_gpkg_path = gpkg_file_path.relative_to(staging_root)
                    if not relative_gpkg_path.parts:
                        logging.error(
                            "❌ Could not determine authority for GeoPackage %s, skipping.", 
                            gpkg_file_path
                        )
                        continue
                    authority = relative_gpkg_path.parts[0]
                    self._copy_gpkg_contents_to_staging_gdb(gpkg_file_path, authority, used_names_in_staging_gdb)
                except Exception as e: 
                    logging.error(
                        "❌ Unexpected error processing GeoPackage file %s (authority '%s'): %s",
                        gpkg_file_path.name, authority, e, exc_info=True
                    )
        else:
            logging.info("ℹ️ No GeoPackages found in staging area.")

        # -- Shapefiles ----------------------------------------------------
        arcpy.env.workspace = str(self.gdb_path) # type: ignore[attr-defined]
        shp_files = list(staging_root.rglob("*.shp"))
        if shp_files:
            logging.info("ℹ️ Found %d shapefile(s) to process.", len(shp_files))
            for shp_file_path in shp_files:
                tgt_name = f"unknown_target_for_{shp_file_path.name}" 
                authority = "UNKNOWN_AUTHORITY"
                try:
                    relative_shp_path = shp_file_path.relative_to(staging_root)
                    if not relative_shp_path.parts:
                        logging.error(
                            "❌ Could not determine authority for Shapefile %s, skipping.",
                            shp_file_path
                        )
                        continue
                    authority = relative_shp_path.parts[0]

                    base_name = generate_base_feature_class_name(
                        shp_file_path.stem, 
                        authority, 
                        max_length=DEFAULT_MAX_FC_NAME_LENGTH
                    ) 
                    tgt_name = self._ensure_unique_name(
                        base_name, 
                        used_names_in_staging_gdb, 
                        max_length=DEFAULT_MAX_FC_NAME_LENGTH
                    )
                    
                    logging.info("📥 Copying SHP %s → %s (Original Stem: %s, Authority: %s, Base Name: %s)",
                                 shp_file_path.relative_to(paths.ROOT), tgt_name, shp_file_path.stem, authority, base_name)
                    arcpy.management.CopyFeatures(str(shp_file_path), tgt_name)
                except arcpy.ExecuteError:
                    logging.error(
                        "❌ arcpy.management.CopyFeatures failed for SHP %s → %s: %s", 
                        shp_file_path.name, tgt_name, arcpy.GetMessages(2),
                        exc_info=True 
                    )
                except ValueError as ve: 
                    logging.error("❌ Naming error for SHP original stem '%s' (authority '%s'): %s", 
                                  shp_file_path.stem, authority, ve, exc_info=True) 
                except Exception as e: 
                    logging.error(
                        "❌ Unexpected error processing SHP %s (target '%s'): %s", 
                        shp_file_path.name, tgt_name, e, exc_info=True
                    )
        else:
            logging.info("ℹ️ No shapefiles found in staging area.")

        if not gpkg_files and not shp_files:
            logging.warning("⚠️ No GeoPackages or shapefiles found in %s to load into GDB", staging_root)

        arcpy.env.workspace = original_workspace # type: ignore[attr-defined]
        logging.info("✅ GDB loading stage complete. Workspace restored to: %s", arcpy.env.workspace) # type: ignore[attr-defined]


    # ---------------------------------------------------------------- internals

    def _reset_gdb(self) -> None:
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
            logging.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s", gdb_full_path, msg, exc_info=True) 
            raise RuntimeError(f"CreateFileGDB failed for '{gdb_full_path}': {msg}") from None
        except Exception as e: 
            logging.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s", gdb_full_path, e, exc_info=True)
            raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_full_path}': {e}") from e
    
    def _copy_gpkg_contents_to_staging_gdb(
        self,
        gpkg_file_path: Path,
        authority: str,
        used_names_in_staging_gdb: Set[str], 
    ) -> None:
        """Copies all feature classes from a GeoPackage into the main staging FileGDB."""
        logging.info("📦 Processing GeoPackage: %s (Authority: %s) [V3 LOG]", 
                     gpkg_file_path.relative_to(paths.ROOT), authority)
        
        gpkg_workspace = str(gpkg_file_path)
        current_arc_workspace = arcpy.env.workspace # type: ignore[attr-defined]
        arcpy.env.workspace = gpkg_workspace # type: ignore[attr-defined]

        try:
            feature_classes_in_gpkg = arcpy.ListFeatureClasses()
            if not feature_classes_in_gpkg:
                logging.info("    ℹ️ No feature classes found in GeoPackage: %s [V3 LOG]", gpkg_file_path.name)
                return

            logging.info("    Found %d feature classes in %s: %s [V3 LOG]", 
                         len(feature_classes_in_gpkg), gpkg_file_path.name, feature_classes_in_gpkg)

            for fc_name_in_gpkg in feature_classes_in_gpkg: # This name might be "main.layername" or just "layername"
                tgt_name = f"unknown_target_for_gpkg_fc_{fc_name_in_gpkg}"
                # The actual name to use for FeatureClassToFeatureClass input when workspace is the GPKG
                # should be the name *without* "main." if ListFeatureClasses returns it with the prefix.
                # If ListFeatureClasses already returns it without the prefix, this sub() does nothing.
                input_fc_name_for_arcpy = _MAIN_RE.sub("", fc_name_in_gpkg)

                try:
                    # For generating the *output* name, we use the potentially prefixed name from ListFeatureClasses
                    # and then clean it. The generate_base_feature_class_name handles stripping "main." if it's there
                    # as part of its general sanitization or if _MAIN_RE in that function catches it.
                    # However, to be explicit for output naming, let's ensure we use the stem after our _MAIN_RE.
                    stem_for_output_naming = _MAIN_RE.sub("", fc_name_in_gpkg)
                    if stem_for_output_naming != fc_name_in_gpkg:
                         logging.info("    Stripped 'main.' prefix from '%s' → '%s' for output naming [V3 LOG]", 
                                      fc_name_in_gpkg, stem_for_output_naming)
                    else:
                        logging.info("    No 'main.' prefix found on '%s' for output naming [V3 LOG]", fc_name_in_gpkg)


                    base_name = generate_base_feature_class_name(
                        stem_for_output_naming, # Use the stem explicitly cleaned here for output
                        authority,
                        max_length=DEFAULT_MAX_FC_NAME_LENGTH,
                    )
                    tgt_name = self._ensure_unique_name(
                        base_name, 
                        used_names_in_staging_gdb, 
                        max_length=DEFAULT_MAX_FC_NAME_LENGTH
                        )
                    
                    logging.info("    [V3 LOG] Attempting GPKG FC copy: GPKG_WORKSPACE:/'%s' (Original listed name: '%s') → STAGING_GDB:/'%s'", 
                                 input_fc_name_for_arcpy, fc_name_in_gpkg, tgt_name)
                    
                    arcpy.conversion.FeatureClassToFeatureClass(
                        in_features=input_fc_name_for_arcpy, # USE THE CLEANED NAME FOR INPUT
                        out_path=str(self.gdb_path), 
                        out_name=tgt_name
                    )
                    logging.info("        [V3 LOG] ✅ Successfully copied GPKG FC '%s' (as '%s') to '%s'", 
                                 fc_name_in_gpkg, input_fc_name_for_arcpy, tgt_name)

                except arcpy.ExecuteError:
                    arcpy_messages = arcpy.GetMessages(2) 
                    logging.error(
                        "    [V3 LOG] ❌ arcpy.conversion.FeatureClassToFeatureClass ExecuteError for GPKG FC (input as '%s', original listed: '%s', from %s) → '%s'. ArcPy Messages: %s",
                        input_fc_name_for_arcpy, fc_name_in_gpkg, gpkg_file_path.name, tgt_name, arcpy_messages,
                        exc_info=True 
                    )
                except ValueError as ve: 
                    logging.error("    [V3 LOG] ❌ Naming error for GPKG FC (original listed: '%s', from %s, authority '%s'): %s",
                                  fc_name_in_gpkg, gpkg_file_path.name, authority, ve,
                                  exc_info=True 
                                  )
                except Exception as e: 
                    logging.error(
                        "    [V3 LOG] ❌ Unexpected error processing GPKG FC (original listed: '%s', from %s, target '%s'): %s",
                        fc_name_in_gpkg, gpkg_file_path.name, tgt_name, e,
                        exc_info=True 
                    )
        except Exception as e: 
            logging.error("❌ Failed to list or process feature classes in GeoPackage '%s': %s [V3 LOG]", 
                          gpkg_file_path.name, e, exc_info=True)
        finally:
            arcpy.env.workspace = current_arc_workspace # type: ignore[attr-defined]
            logging.debug("    Restored workspace after GPKG %s to: %s [V3 LOG]", gpkg_file_path.name, arcpy.env.workspace) # type: ignore[attr-defined]

    @staticmethod
    def _ensure_unique_name(base_name: str, used_names: Set[str], max_length: int = DEFAULT_MAX_FC_NAME_LENGTH) -> str:
        candidate = base_name
        
        if len(candidate) > max_length: 
            logging.warning(
                "⚠️ Base name '%s' (length %d) exceeds max_length %d even before adding suffix. Truncating.",
                candidate, len(candidate), max_length
            )
            candidate = candidate[:max_length]
            if not candidate:
                 raise ValueError(f"Base name '{base_name}' became empty after initial truncation to {max_length} chars.")
        
        final_candidate = candidate 
        idx = 1
        while final_candidate.lower() in (n.lower() for n in used_names):
            suffix = f"_{idx}"
            
            if len(candidate) + len(suffix) > max_length:
                chars_to_remove = (len(candidate) + len(suffix)) - max_length
                current_stem_part = candidate 
                
                if chars_to_remove >= len(current_stem_part): 
                    raise ValueError(
                        f"Cannot generate a unique name for base '{base_name}' (stem part '{current_stem_part}') "
                        f"under {max_length} chars with suffix '{suffix}'. Too many characters to remove."
                    )
                
                truncated_stem_part = current_stem_part[:-chars_to_remove]
                final_candidate = f"{truncated_stem_part}{suffix}"

                if not truncated_stem_part: 
                     raise ValueError(
                        f"Cannot generate a unique name for base '{base_name}'. "
                        f"Stem part became empty after trying to fit suffix '{suffix}' within {max_length} chars."
                    )
            else:
                final_candidate = f"{candidate}{suffix}"
            
            idx += 1
            if idx > 9999: 
                raise ValueError(f"Could not find a unique name for base '{base_name}' after {idx-1} attempts within {max_length} chars.")

        used_names.add(final_candidate)
        return final_candidate
