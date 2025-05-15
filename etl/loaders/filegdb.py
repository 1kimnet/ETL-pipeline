"""Loader that consolidates all staged data (Shapefiles and GeoPackages) 
into *staging.gdb* using ArcPy.

This **updated** version (V5.2):
* Fixes staging path logic for multi-part shapefile collections (e.g., NVV).
* Adds specific debug logging to _process_single_shapefile for authority.
* Iterates through Source objects to apply include filters for GPKGs.
* Uses a `staged_data_type` in Source model to determine processing.
* Implements a two-attempt strategy for GPKG input feature class names.
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
from typing import Set, Final, List, Optional 

import arcpy  # ArcGIS Pro / Server Python ships with this

from ..utils import paths, ensure_dirs
from ..utils.naming import (
    generate_base_feature_class_name,
    DEFAULT_MAX_FC_NAME_LENGTH,
    sanitize_for_filename 
)
from ..models import Source 

_MAIN_RE: Final = re.compile(r"^main\.", re.IGNORECASE) 

class ArcPyFileGDBLoader:
    """Build (or rebuild) *staging.gdb* from everything under *data/staging*."""

    def __init__(self, gdb_path: Optional[Path] = None, sources_yaml_path: Optional[Path] = None):
        ensure_dirs()
        self.gdb_path: Path = gdb_path or paths.GDB
        self.sources: List[Source] = []
        if sources_yaml_path:
            self.sources = Source.load_all(sources_yaml_path)
            if not self.sources and Path(sources_yaml_path).exists(): # Check if file exists but parsing failed or was empty
                 logging.warning(f"⚠️ Source.load_all returned empty list from existing file {sources_yaml_path}. Check YAML format. Loader will use fallback globbing. [V5.2 LOG]")
            elif not Path(sources_yaml_path).exists():
                 logging.error(f"❌ sources_yaml_path {sources_yaml_path} does not exist. Loader will use fallback globbing. [V5.2 LOG]")
        elif not sources_yaml_path: 
            logging.warning("⚠️ sources_yaml_path not provided to ArcPyFileGDBLoader. Include filters for GPKGs will not be applied, and specific source staging paths may not be found. Loader will use fallback globbing. [V5.2 LOG]")


    def load_from_staging(self, staging_root: Path) -> None:
        """Recreate the FileGDB and copy content based on Source configurations."""
        self._reset_gdb()

        used_names_in_staging_gdb: Set[str] = set()
        
        arcpy.env.overwriteOutput = True  # type: ignore[attr-defined]
        original_workspace: Optional[str] = arcpy.env.workspace # type: ignore[attr-defined] 

        if not self.sources: 
            logging.warning("⚠️ No Source configurations loaded for GDB loader. Attempting to glob all .shp and .gpkg files from root staging directory without specific include filters. [V5.2 LOG]")
            self._glob_and_load_shapefiles(staging_root, used_names_in_staging_gdb)
            self._glob_and_load_geopackages(staging_root, used_names_in_staging_gdb, None) 
        else:
            for source in self.sources:
                if not source.enabled:
                    logging.info("⏭️ Source '%s' is disabled, skipping loading. [V5.2 LOG]", source.name)
                    continue
                
                logging.info("ℹ️ Processing source for loading: '%s' (Source.Authority from YAML: '%s') [V5.2 LOG]", 
                             source.name, source.authority)

                if source.staged_data_type == "gpkg":
                    # For GPKG, the handler stages it as: staging_root/AUTHORITY/sanitized_source_name/sanitized_source_name.gpkg
                    sanitized_source_name_for_path: str = sanitize_for_filename(source.name)
                    source_staging_dir: Path = staging_root / source.authority / sanitized_source_name_for_path
                    expected_gpkg_filename: str = sanitized_source_name_for_path + ".gpkg" 
                    gpkg_file_path: Path = source_staging_dir / expected_gpkg_filename
                    
                    logging.debug("    Looking for GPKG for source '%s' at: %s [V5.2 LOG]", source.name, gpkg_file_path)
                    
                    processed_gpkg = False
                    if gpkg_file_path.is_file():
                        self._copy_gpkg_contents_to_staging_gdb(
                            gpkg_file_path, 
                            source.authority, 
                            used_names_in_staging_gdb,
                            source.include 
                        )
                        processed_gpkg = True
                    else:
                        # Fallback for GPKG: check if any .gpkg exists directly under source_staging_dir
                        gpkg_files_in_dir = list(source_staging_dir.glob("*.gpkg"))
                        if gpkg_files_in_dir:
                            if len(gpkg_files_in_dir) > 1:
                                logging.warning(f"    Found multiple GPKG files in {source_staging_dir}, processing only the first: {gpkg_files_in_dir[0].name} [V5.2 LOG]")
                            self._copy_gpkg_contents_to_staging_gdb(
                                gpkg_files_in_dir[0],
                                source.authority, 
                                used_names_in_staging_gdb,
                                source.include
                            )
                            processed_gpkg = True
                    if not processed_gpkg:
                        logging.warning(f"    No GPKG file found for source '{source.name}' in dir {source_staging_dir} [V5.2 LOG]")
                
                elif source.staged_data_type == "shapefile_collection" or not source.staged_data_type: 
                    # This handles both single ZIPs (like FM) and multi-ZIPs (like NVV)
                    arcpy.env.workspace = str(self.gdb_path) # type: ignore[attr-defined]
                    
                    if source.include: # Multi-part source, e.g., NVV
                        logging.info("    Processing multi-part shapefile collection for '%s'. [V5.2 LOG]", source.name)
                        found_any_shp_for_source = False
                        for included_item_stem in source.include:
                            sanitized_item_stem: str = sanitize_for_filename(included_item_stem)
                            # Handler creates: staging_root / AUTHORITY / sanitized_item_stem / extracted_files...
                            item_staging_dir: Path = staging_root / source.authority / sanitized_item_stem
                            
                            if not item_staging_dir.exists():
                                logging.warning(f"        ⚠️ Staging subdirectory not found for included item '{included_item_stem}': {item_staging_dir} [V5.2 LOG]")
                                continue

                            shp_files_in_item_dir = list(item_staging_dir.rglob("*.shp"))
                            if shp_files_in_item_dir:
                                logging.info("        Found %d shapefile(s) in item dir '%s'. [V5.2 LOG]", len(shp_files_in_item_dir), item_staging_dir.name)
                                for shp_file_path in shp_files_in_item_dir:
                                    logging.debug("    [DEBUG V5.2] Calling _process_single_shapefile for '%s' with authority: '%s' (from multi-part source '%s')", 
                                                  shp_file_path.name, source.authority, source.name)
                                    self._process_single_shapefile(shp_file_path, source.authority, used_names_in_staging_gdb)
                                    found_any_shp_for_source = True
                            else:
                                logging.info("        No shapefiles found for included item '%s' in %s. [V5.2 LOG]", included_item_stem, item_staging_dir)
                        if not found_any_shp_for_source:
                             logging.warning("    No shapefiles processed for multi-part source '%s'. [V5.2 LOG]", source.name)
                    else: # Single shapefile collection (e.g., from one ZIP like FM or LST)
                        # Handler stages as: staging_root / AUTHORITY / sanitized_source_name / extracted_files...
                        sanitized_source_name_for_path: str = sanitize_for_filename(source.name)
                        source_staging_dir: Path = staging_root / source.authority / sanitized_source_name_for_path
                        
                        if not source_staging_dir.exists():
                            logging.warning(f"    ⚠️ Staging directory not found for source '{source.name}': {source_staging_dir} [V5.2 LOG]")
                            continue

                        shp_files_in_dir = list(source_staging_dir.rglob("*.shp")) 
                        if shp_files_in_dir:
                            logging.info("    Found %d shapefile(s) for single source '%s'. [V5.2 LOG]", len(shp_files_in_dir), source.name)
                            for shp_file_path in shp_files_in_dir:
                                logging.debug("    [DEBUG V5.2] Calling _process_single_shapefile for '%s' with authority: '%s' (from single source '%s')", 
                                              shp_file_path.name, source.authority, source.name)
                                self._process_single_shapefile(shp_file_path, source.authority, used_names_in_staging_gdb)
                        else:
                            logging.info("    No shapefiles found for source '%s' in %s. [V5.2 LOG]", source.name, source_staging_dir)
                else:
                    logging.warning(f"🤷 Unknown staged_data_type '{source.staged_data_type}' for source '{source.name}'. Skipping. [V5.2 LOG]")

        arcpy.env.workspace = original_workspace # type: ignore[attr-defined]
        logging.info("✅ GDB loading stage complete. Workspace restored to: %s [V5.2 LOG]", arcpy.env.workspace) # type: ignore[attr-defined]

    def _glob_and_load_shapefiles(self, staging_root: Path, used_names_set: Set[str]):
        """Fallback: glob and load all shapefiles if no sources are defined for loader."""
        arcpy.env.workspace = str(self.gdb_path) # type: ignore[attr-defined]
        shp_files = list(staging_root.rglob("*.shp"))
        if shp_files:
            logging.info("Fallback: Found %d shapefile(s) to process via globbing. [V5.2 LOG]", len(shp_files))
            for shp_file_path in shp_files:
                derived_authority_from_path: str 
                try:
                    path_parts = shp_file_path.relative_to(staging_root).parts
                    if len(path_parts) > 1: 
                        derived_authority_from_path = path_parts[0]
                    else: 
                        derived_authority_from_path = "UNKNOWN_GLOB_AUTH_PATH"
                except (IndexError, ValueError): 
                    derived_authority_from_path = "UNKNOWN_GLOB_AUTH_EXC"
                logging.debug("    [DEBUG V5.2 Fallback] Calling _process_single_shapefile for '%s' with derived authority: '%s'", shp_file_path.name, derived_authority_from_path)
                self._process_single_shapefile(shp_file_path, derived_authority_from_path, used_names_set)
        else:
            logging.info("Fallback: No shapefiles found in staging area via globbing. [V5.2 LOG]")
            
    def _glob_and_load_geopackages(self, staging_root: Path, used_names_set: Set[str], include_filter: Optional[List[str]]):
        """Fallback: glob and load all geopackages if no sources are defined for loader."""
        gpkg_files = list(staging_root.rglob("*.gpkg"))
        if gpkg_files:
            logging.info("Fallback: Found %d GeoPackage(s) to process via globbing. [V5.2 LOG]", len(gpkg_files))
            for gpkg_file_path in gpkg_files:
                derived_authority_from_path: str
                try:
                    path_parts = gpkg_file_path.relative_to(staging_root).parts
                    if len(path_parts) > 1:
                        derived_authority_from_path = path_parts[0]
                    else:
                        derived_authority_from_path = "UNKNOWN_GLOB_AUTH_PATH"
                except (IndexError, ValueError):
                    derived_authority_from_path = "UNKNOWN_GLOB_AUTH_EXC"
                self._copy_gpkg_contents_to_staging_gdb(gpkg_file_path, derived_authority_from_path, used_names_set, include_filter)
        else:
            logging.info("Fallback: No GeoPackages found in staging area via globbing. [V5.2 LOG]")

    def _process_single_shapefile(self, shp_file_path: Path, authority: str, used_names_in_staging_gdb: Set[str]):
        """Helper to process a single shapefile."""
        logging.debug("    [DEBUG V5.2 _process_single_shapefile] Received authority: '%s' for SHP: %s", authority, shp_file_path.name)
        tgt_name = f"unknown_target_for_{shp_file_path.name}"
        try:
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
            
            logging.info("    📥 Copying SHP %s → %s (Original Stem: %s, Authority used for naming: '%s', Base Name: '%s') [V5.2 LOG]",
                         shp_file_path.relative_to(paths.ROOT), tgt_name, shp_file_path.stem, authority, base_name)
            arcpy.management.CopyFeatures(str(shp_file_path), tgt_name)
        except arcpy.ExecuteError:
            logging.error(
                "    ❌ arcpy.management.CopyFeatures failed for SHP %s → %s: %s [V5.2 LOG]", 
                shp_file_path.name, tgt_name, arcpy.GetMessages(2),
                exc_info=True 
            )
        except ValueError as ve: 
            logging.error("    ❌ Naming error for SHP original stem '%s' (authority '%s'): %s [V5.2 LOG]", 
                          shp_file_path.stem, authority, ve, exc_info=True) 
        except Exception as e: 
            logging.error(
                "    ❌ Unexpected error processing SHP %s (target '%s'): %s [V5.2 LOG]", 
                shp_file_path.name, tgt_name, e, exc_info=True
            )

    def _reset_gdb(self) -> None:
        gdb_full_path = self.gdb_path.resolve() 
        logging.info("ℹ️ Target GDB path for reset: %s [V5.2 LOG]", gdb_full_path)

        if self.gdb_path.exists():
            logging.info("🗑️ Attempting to remove existing GDB: %s [V5.2 LOG]", gdb_full_path)
            try:
                shutil.rmtree(self.gdb_path)
                logging.info("✅ Successfully removed existing GDB: %s [V5.2 LOG]", gdb_full_path)
            except Exception as e:
                logging.error("❌ Failed to remove existing GDB '%s': %s [V5.2 LOG]", gdb_full_path, e, exc_info=True)
                raise RuntimeError(f"Failed to remove existing GDB '{gdb_full_path}': {e}") from e
        else:
            logging.info("ℹ️ GDB does not currently exist at %s, no removal needed. [V5.2 LOG]", gdb_full_path)

        parent_dir = self.gdb_path.parent.resolve()
        if not parent_dir.exists():
            logging.info("🆕 Parent directory %s for GDB does not exist. Attempting to create. [V5.2 LOG]", parent_dir)
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
                logging.info("✅ Successfully created parent directory: %s [V5.2 LOG]", parent_dir)
            except Exception as e:
                logging.error("❌ Failed to create parent directory '%s' for GDB: %s [V5.2 LOG]", parent_dir, e, exc_info=True)
                raise RuntimeError(f"Failed to create parent directory '{parent_dir}' for GDB: {e}") from e
        
        logging.info("🆕 Attempting to create new FileGDB: %s in folder %s [V5.2 LOG]", self.gdb_path.name, self.gdb_path.parent.resolve())
        try:
            arcpy.management.CreateFileGDB(str(self.gdb_path.parent), self.gdb_path.name)
            logging.info("✅ Successfully created new GDB: %s [V5.2 LOG]", gdb_full_path)
        except arcpy.ExecuteError: 
            msg = arcpy.GetMessages(2) 
            logging.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s [V5.2 LOG]", gdb_full_path, msg, exc_info=True) 
            raise RuntimeError(f"CreateFileGDB failed for '{gdb_full_path}': {msg}") from None
        except Exception as e: 
            logging.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s [V5.2 LOG]", gdb_full_path, e, exc_info=True)
            raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_full_path}': {e}") from e
    
    def _copy_gpkg_contents_to_staging_gdb(
        self,
        gpkg_file_path: Path,
        authority: str, 
        used_names_in_staging_gdb: Set[str], 
        include_filter: Optional[List[str]] = None 
    ) -> None:
        """Copies feature classes from a GeoPackage into the main staging FileGDB,
           optionally filtering by the include_filter list."""
        logging.info("📦 Processing GeoPackage: %s (Authority for naming: '%s', Include Filter: %s) [V5.2 LOG]", 
                     gpkg_file_path.relative_to(paths.ROOT), authority, 
                     include_filter if include_filter else "None")
        
        gpkg_workspace: str = str(gpkg_file_path)
        current_arc_workspace: Optional[str] = arcpy.env.workspace # type: ignore[attr-defined]
        arcpy.env.workspace = gpkg_workspace # type: ignore[attr-defined]

        try:
            feature_classes_in_gpkg: List[str] = arcpy.ListFeatureClasses()
            if not feature_classes_in_gpkg:
                logging.info("    ℹ️ No feature classes found in GeoPackage: %s [V5.2 LOG]", gpkg_file_path.name)
                return

            logging.info("    Found %d feature classes in %s: %s [V5.2 LOG]", 
                         len(feature_classes_in_gpkg), gpkg_file_path.name, feature_classes_in_gpkg)

            normalized_include_filter: Optional[Set[str]] = None
            if include_filter: 
                normalized_include_filter = {_MAIN_RE.sub("", item).lower() for item in include_filter if item}
                logging.info("    Normalized include filter for %s: %s [V5.2 LOG]", gpkg_file_path.name, normalized_include_filter)


            for fc_name_listed_by_arcpy in feature_classes_in_gpkg: 
                tgt_name: str = f"unknown_target_for_gpkg_fc_{fc_name_listed_by_arcpy}"
                
                stem_for_output_naming: str = _MAIN_RE.sub("", fc_name_listed_by_arcpy)

                if normalized_include_filter:
                    comparable_fc_name: str = stem_for_output_naming.lower()
                    if comparable_fc_name not in normalized_include_filter:
                        logging.info("    ⏭️ Skipping GPKG FC '%s' (normalized: '%s') as it's not in the include filter. [V5.2 LOG]",
                                     fc_name_listed_by_arcpy, comparable_fc_name)
                        continue 
                    else:
                        logging.info("    ✅ GPKG FC '%s' (normalized: '%s') is in include filter. Proceeding. [V5.2 LOG]",
                                     fc_name_listed_by_arcpy, comparable_fc_name)
                
                if stem_for_output_naming != fc_name_listed_by_arcpy:
                     logging.info("    Stripped 'main.' from '%s' → '%s' for output naming [V5.2 LOG]", 
                                  fc_name_listed_by_arcpy, stem_for_output_naming)

                base_name: str = generate_base_feature_class_name(
                    stem_for_output_naming, 
                    authority, 
                    max_length=DEFAULT_MAX_FC_NAME_LENGTH,
                )
                tgt_name = self._ensure_unique_name(
                    base_name, 
                    used_names_in_staging_gdb, 
                    max_length=DEFAULT_MAX_FC_NAME_LENGTH
                )
                
                input_fc_name_attempt1: str = fc_name_listed_by_arcpy
                copied_successfully: bool = False
                
                logging.info("    [V5.2 LOG] Attempt 1: GPKG FC copy using input '%s' (listed name) → STAGING_GDB:/'%s'", 
                             input_fc_name_attempt1, tgt_name)
                try:
                    arcpy.conversion.FeatureClassToFeatureClass(
                        in_features=input_fc_name_attempt1,
                        out_path=str(self.gdb_path), 
                        out_name=tgt_name
                    )
                    logging.info("        [V5.2 LOG] ✅ Attempt 1 SUCCESS: Copied GPKG FC '%s' to '%s'", 
                                 input_fc_name_attempt1, tgt_name)
                    copied_successfully = True
                except arcpy.ExecuteError as e1:
                    arcpy_messages_e1: str = arcpy.GetMessages(2)
                    logging.warning(
                        "    [V5.2 LOG] ⚠️ Attempt 1 FAILED for input '%s': %s. ArcPy Messages: %s",
                        input_fc_name_attempt1, e1, arcpy_messages_e1
                    )
                    if "000732" in arcpy_messages_e1 and _MAIN_RE.match(fc_name_listed_by_arcpy):
                        input_fc_name_attempt2: str = _MAIN_RE.sub("", fc_name_listed_by_arcpy)
                        if input_fc_name_attempt2 != input_fc_name_attempt1: 
                            logging.info("    [V5.2 LOG] Attempt 2: GPKG FC copy using input '%s' (stripped name) → STAGING_GDB:/'%s'", 
                                         input_fc_name_attempt2, tgt_name)
                            try:
                                arcpy.conversion.FeatureClassToFeatureClass(
                                    in_features=input_fc_name_attempt2,
                                    out_path=str(self.gdb_path), 
                                    out_name=tgt_name
                                )
                                logging.info("        [V5.2 LOG] ✅ Attempt 2 SUCCESS: Copied GPKG FC '%s' (listed as '%s') to '%s'", 
                                             input_fc_name_attempt2, fc_name_listed_by_arcpy, tgt_name)
                                copied_successfully = True
                            except arcpy.ExecuteError as e2:
                                arcpy_messages_e2: str = arcpy.GetMessages(2)
                                logging.error(
                                    "    [V5.2 LOG] ❌ Attempt 2 FAILED for input '%s': %s. ArcPy Messages: %s",
                                    input_fc_name_attempt2, e2, arcpy_messages_e2, exc_info=True
                                )
                            except Exception as e_generic2: 
                                logging.error(
                                    "    [V5.2 LOG] ❌ Unexpected error on Attempt 2 for input '%s': %s",
                                    input_fc_name_attempt2, e_generic2, exc_info=True
                                )
                        else: 
                            logging.error(
                                "    [V5.2 LOG] ❌ Attempt 1 (input '%s') failed with 000732, and stripping 'main.' made no change or was not applicable. Final error for this FC. Original ArcPy Messages: %s",
                                input_fc_name_attempt1, arcpy_messages_e1, exc_info=True 
                            )
                    else: 
                        logging.error(
                            "    [V5.2 LOG] ❌ Attempt 1 (input '%s') failed (not 000732 or no 'main.' prefix), and not retrying with stripped name. Final error for this FC. ArcPy Messages: %s",
                            input_fc_name_attempt1, arcpy_messages_e1, exc_info=True 
                        )
                except ValueError as ve: 
                    logging.error("    [V5.2 LOG] ❌ Naming error for GPKG FC (original listed: '%s', from %s, authority '%s'): %s",
                                  fc_name_listed_by_arcpy, gpkg_file_path.name, authority, ve,
                                  exc_info=True 
                                  )
                except Exception as e_generic1: 
                    logging.error(
                        "    [V5.2 LOG] ❌ Unexpected error on Attempt 1 for input '%s': %s",
                        input_fc_name_attempt1, e_generic1, exc_info=True
                    )

                if not copied_successfully:
                    logging.error("    [V5.2 LOG] ❗ Ultimately FAILED to copy GPKG FC '%s' from %s to staging GDB.",
                                  fc_name_listed_by_arcpy, gpkg_file_path.name)

        except Exception as e: 
            logging.error("❌ Failed to list or process feature classes in GeoPackage '%s': %s [V5.2 LOG]", 
                          gpkg_file_path.name, e, exc_info=True)
        finally:
            arcpy.env.workspace = current_arc_workspace # type: ignore[attr-defined]
            logging.debug("    Restored workspace after GPKG %s to: %s [V5.2 LOG]", gpkg_file_path.name, arcpy.env.workspace) # type: ignore[attr-defined]

    @staticmethod
    def _ensure_unique_name(base_name: str, used_names: Set[str], max_length: int = DEFAULT_MAX_FC_NAME_LENGTH) -> str:
        candidate: str = base_name
        
        if len(candidate) > max_length: 
            logging.warning(
                "⚠️ Base name '%s' (length %d) exceeds max_length %d even before adding suffix. Truncating. [V5.2 LOG]",
                candidate, len(candidate), max_length
            )
            candidate = candidate[:max_length]
            if not candidate:
                 raise ValueError(f"Base name '{base_name}' became empty after initial truncation to {max_length} chars.")
        
        final_candidate: str = candidate 
        idx: int = 1
        while final_candidate.lower() in (n.lower() for n in used_names):
            suffix: str = f"_{idx}"
            
            if len(candidate) + len(suffix) > max_length:
                chars_to_remove: int = (len(candidate) + len(suffix)) - max_length
                current_stem_part: str = candidate 
                
                if chars_to_remove >= len(current_stem_part): 
                    raise ValueError(
                        f"Cannot generate a unique name for base '{base_name}' (stem part '{current_stem_part}') "
                        f"under {max_length} chars with suffix '{suffix}'. Too many characters to remove."
                    )
                
                truncated_stem_part: str = current_stem_part[:-chars_to_remove]
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
