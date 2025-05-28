# etl/loaders/filegdb.py
"""
Complete FileGDB loader with corrected shapefile validation and comprehensive error handling.

This module consolidates all staged data (Shapefiles, GeoPackages, and GeoJSON)
into staging.gdb using ArcPy with a refactored naming system and robust validation.
"""

from __future__ import annotations

import logging
import shutil
import re
from pathlib import Path
from typing import Set, Final, List, Optional
from dataclasses import dataclass

import arcpy  # ArcGIS Pro / Server Python ships with this

from ..utils import paths, ensure_dirs
from ..utils.naming import sanitize_for_filename, generate_fc_name
from ..models import Source

log: Final[logging.Logger] = logging.getLogger(__name__)

_MAIN_RE: Final[re.Pattern[str]] = re.compile(r"^main\.", re.IGNORECASE)


@dataclass(frozen=True)
class ShapefileValidationResult:
    """Result of shapefile component validation with comprehensive error details."""
    is_valid: bool
    error_message: str
    missing_components: List[str]


class ArcPyFileGDBLoader:
    """Build (or rebuild) staging.gdb from everything under data/staging with comprehensive validation."""

    def __init__(
        self, 
        gdb_path: Optional[Path] = None, 
        sources_yaml_path: Optional[Path] = None
    ) -> None:
        """Initialize the FileGDB loader with optional custom paths."""
        ensure_dirs()
        self.gdb_path: Path = gdb_path or paths.GDB
        self.sources: List[Source] = []
        
        if sources_yaml_path:
            self._load_sources_configuration(sources_yaml_path)
        else:
            log.warning("⚠️ sources_yaml_path not provided to ArcPyFileGDBLoader. "
                       "Include filters for GPKGs and GeoJSON specific loading will not be applied optimally. "
                       "Loader will use fallback globbing.")

    def _load_sources_configuration(self, sources_yaml_path: Path) -> None:
        """Load and validate sources configuration from YAML file."""
        try:
            self.sources = Source.load_all(sources_yaml_path)
            if not self.sources and sources_yaml_path.exists():
                log.warning("⚠️ Source.load_all returned empty list from existing file %s. "
                           "Check YAML format. Loader will use fallback globbing.", sources_yaml_path)
            elif not sources_yaml_path.exists():
                log.error("❌ sources_yaml_path %s does not exist. "
                         "Loader will use fallback globbing.", sources_yaml_path)
        except Exception as exc:
            log.error("❌ Failed to load sources configuration: %s", exc, exc_info=True)
            self.sources = []

    def load_from_staging(self, staging_root: Path) -> None:
        """Recreate the FileGDB and copy content based on Source configurations."""
        self._reset_gdb()

        used_names_in_staging_gdb: Set[str] = set()
        arcpy.env.overwriteOutput = True  # type: ignore[attr-defined]
        original_global_workspace: Optional[str] = arcpy.env.workspace  # type: ignore[attr-defined]

        try:
            if not self.sources:
                self._perform_fallback_globbing(staging_root, used_names_in_staging_gdb)
            else:
                self._process_configured_sources(staging_root, used_names_in_staging_gdb)
        finally:
            arcpy.env.workspace = original_global_workspace  # type: ignore[attr-defined]
            log.info("✅ GDB loading stage complete. Workspace restored to: %s", 
                    arcpy.env.workspace)  # type: ignore[attr-defined]

    def _perform_fallback_globbing(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """Perform fallback globbing when no source configuration is available."""
        log.warning("⚠️ No Source configurations loaded for GDB loader. "
                   "Attempting to glob all .shp, .gpkg, and .geojson files from root staging directory "
                   "without specific include filters.")
        
        self._glob_and_load_shapefiles(staging_root, used_names_set)
        self._glob_and_load_geopackages(staging_root, used_names_set, None)
        self._glob_and_load_geojsonfiles(staging_root, used_names_set)

    def _process_configured_sources(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """Process sources based on their configuration."""
        for source in self.sources:
            if not source.enabled:
                log.info("⏭️ Source '%s' is disabled, skipping loading.", source.name)
                continue

            normalized_data_type: Optional[str] = self._normalize_staged_data_type(source.staged_data_type)
            
            log.info("🔄 Processing source for loading: '%s' (Authority: '%s', Type: '%s')",
                     source.name, source.authority, normalized_data_type)

            sanitized_source_name_for_path: str = sanitize_for_filename(source.name)
            source_staging_dir: Path = staging_root / source.authority / sanitized_source_name_for_path

            try:
                self._process_source_by_type(
                    source, normalized_data_type, source_staging_dir, staging_root, used_names_set
                )
            except Exception as exc:
                log.error("❌ Failed to process source '%s': %s", source.name, exc, exc_info=True)

    def _process_source_by_type(
        self,
        source: Source,
        normalized_data_type: Optional[str],
        source_staging_dir: Path,
        staging_root: Path,
        used_names_set: Set[str]
    ) -> None:
        """Process a source based on its normalized data type."""
        if normalized_data_type == "gpkg":
            self._handle_gpkg_source(source, source_staging_dir, used_names_set)
        elif normalized_data_type in ("geojson", "json"):
            self._handle_json_geojson_source(source, source_staging_dir, used_names_set)
        elif normalized_data_type == "shapefile_collection" or not normalized_data_type:
            self._handle_shapefile_source(source, source_staging_dir, staging_root, used_names_set)
        else:
            log.warning("🤷 Unknown or unhandled staged_data_type '%s' for source '%s'. Skipping.",
                       normalized_data_type, source.name)

    def _normalize_staged_data_type(self, staged_data_type: Optional[str]) -> Optional[str]:
        """Normalize staged_data_type values to handle common variations."""
        if not staged_data_type:
            return staged_data_type
        
        normalized: str = staged_data_type.lower().strip()
        
        # Handle common variations - keep json and geojson separate for file detection
        if normalized in ["geopackage", "gpkg"]:
            return "gpkg"
        elif normalized in ["shapefile", "shp", "shapefile_collection"]:
            return "shapefile_collection"
        
        # Return json/geojson as-is since we handle both file types
        return normalized

    def _handle_gpkg_source(self, source: Source, source_staging_dir: Path, used_names_set: Set[str]) -> None:
        """Handle GPKG source loading with comprehensive error checking."""
        if not source_staging_dir.exists():
            log.warning("⚠️ Staging directory not found for GPKG source '%s': %s",
                       source.name, source_staging_dir)
            return

        sanitized_source_name_for_path: str = sanitize_for_filename(source.name)
        expected_gpkg_filename: str = f"{sanitized_source_name_for_path}.gpkg"
        gpkg_file_path: Path = source_staging_dir / expected_gpkg_filename
        
        log.debug("Looking for GPKG for source '%s' at: %s", source.name, gpkg_file_path)
        
        if gpkg_file_path.is_file():
            self._copy_gpkg_contents_to_staging_gdb(
                gpkg_file_path, source.authority, used_names_set, source.include
            )
        else:
            self._handle_alternative_gpkg_files(source, source_staging_dir, used_names_set)

    def _handle_alternative_gpkg_files(
        self, source: Source, source_staging_dir: Path, used_names_set: Set[str]
    ) -> None:
        """Handle cases where expected GPKG filename doesn't match."""
        gpkg_files_in_dir: List[Path] = list(source_staging_dir.glob("*.gpkg"))
        
        if gpkg_files_in_dir:
            if len(gpkg_files_in_dir) > 1:
                log.warning("Found multiple GPKG files in %s, processing only the first: %s",
                           source_staging_dir, gpkg_files_in_dir[0].name)
            
            self._copy_gpkg_contents_to_staging_gdb(
                gpkg_files_in_dir[0], source.authority, used_names_set, source.include
            )
        else:
            log.warning("No GPKG file found for source '%s' in dir %s", source.name, source_staging_dir)

    def _handle_json_geojson_source(self, source: Source, source_staging_dir: Path, used_names_set: Set[str]) -> None:
        """Handle both GeoJSON (.geojson) and JSON (.json) source loading with comprehensive error checking."""
        if not source_staging_dir.exists():
            log.warning("⚠️ Staging directory not found for JSON/GeoJSON source '%s': %s",
                       source.name, source_staging_dir)
            return
        
        # Look for both .geojson and .json files
        geojson_files: List[Path] = list(source_staging_dir.glob("*.geojson"))
        json_files: List[Path] = list(source_staging_dir.glob("*.json"))
        all_json_files: List[Path] = geojson_files + json_files
        
        if all_json_files:
            log.info("Found %d JSON/GeoJSON file(s) for source '%s' in dir %s.",
                     len(all_json_files), source.name, source_staging_dir.name)
            for json_file_path in all_json_files:
                self._process_json_geojson_file(json_file_path, source.authority, used_names_set)
        else:
            log.info("No JSON/GeoJSON files found for source '%s' in %s.", source.name, source_staging_dir)

    def _handle_shapefile_source(
        self, 
        source: Source, 
        source_staging_dir: Path, 
        staging_root: Path, 
        used_names_set: Set[str]
    ) -> None:
        """Handle shapefile collection source loading with comprehensive error checking."""
        if source.include and source.type == "file":
            self._process_multi_part_shapefile_collection(source, staging_root, used_names_set)
        else:
            self._process_single_shapefile_source(source, source_staging_dir, used_names_set)

    def _process_multi_part_shapefile_collection(
        self, source: Source, staging_root: Path, used_names_set: Set[str]
    ) -> None:
        """Process multi-part shapefile collections based on include list."""
        log.info("Processing multi-part shapefile collection for '%s'.", source.name)
        found_any_shp_for_source: bool = False
        
        for included_item_stem in source.include:
            sanitized_item_stem: str = sanitize_for_filename(included_item_stem)
            item_staging_dir: Path = staging_root / source.authority / sanitized_item_stem
            
            if not item_staging_dir.exists():
                log.warning("⚠️ Staging subdirectory not found for included item '%s': %s",
                           included_item_stem, item_staging_dir)
                continue
            
            shp_files_in_item_dir: List[Path] = list(item_staging_dir.rglob("*.shp"))
            if shp_files_in_item_dir:
                log.info("Found %d shapefile(s) in item dir '%s'.",
                         len(shp_files_in_item_dir), item_staging_dir.name)
                for shp_file_path in shp_files_in_item_dir:
                    self._process_single_shapefile(shp_file_path, source.authority, used_names_set)
                    found_any_shp_for_source = True
            else:
                log.info("No shapefiles found for included item '%s' in %s.",
                         included_item_stem, item_staging_dir)
        
        if not found_any_shp_for_source:
            log.warning("No shapefiles processed for multi-part source '%s'.", source.name)

    def _process_single_shapefile_source(
        self, source: Source, source_staging_dir: Path, used_names_set: Set[str]
    ) -> None:
        """Process a single shapefile source."""
        if not source_staging_dir.exists():
            log.warning("⚠️ Staging directory not found for source '%s': %s",
                       source.name, source_staging_dir)
            return
        
        shp_files_in_dir: List[Path] = list(source_staging_dir.rglob("*.shp"))
        if shp_files_in_dir:
            log.info("Found %d shapefile(s) for single source '%s' in dir %s.",
                     len(shp_files_in_dir), source.name, source_staging_dir.name)
            for shp_file_path in shp_files_in_dir:
                self._process_single_shapefile(shp_file_path, source.authority, used_names_set)
        else:
            log.info("No shapefiles found for source '%s' in %s.", source.name, source_staging_dir)

    def _process_json_geojson_file(self, json_file_path: Path, authority: str, used_names_set: Set[str]) -> None:
        """Process a single JSON/GeoJSON file using JSONToFeatures with comprehensive error handling."""
        log.debug("[DEBUG] Processing JSON/GeoJSON - Authority: '%s' for file: %s", authority, json_file_path.name)
        
        if not json_file_path.exists():
            log.error("❌ JSON/GeoJSON file does not exist: %s", json_file_path)
            return
        
        # Initialize variables before try block to ensure they're always bound
        tgt_name: str = "UNKNOWN"
        out_fc_full_path: str = "UNKNOWN_PATH"
        
        try:
            input_json_full_path: str = str(json_file_path.resolve())
            base_name: str = generate_fc_name(authority, json_file_path.stem)
            tgt_name = self._ensure_unique_name(base_name, used_names_set)
            out_fc_full_path = str(self.gdb_path / tgt_name)
            
            log.info("📥 Converting JSON/GeoJSON ('%s') → GDB:/'%s' (Authority: '%s')",
                     json_file_path.name, tgt_name, authority)
            
            arcpy.conversion.JSONToFeatures(
                in_json_file=input_json_full_path, 
                out_features=out_fc_full_path
            )
            log.info("✅ SUCCESS: Converted JSON/GeoJSON '%s' to '%s'", json_file_path.name, tgt_name)
            
        except arcpy.ExecuteError as arc_error:
            arcpy_messages: str = arcpy.GetMessages(2)
            log.error("❌ arcpy.conversion.JSONToFeatures failed for JSON/GeoJSON %s → %s: %s. ArcPy Messages: %s",
                      json_file_path.name, tgt_name, arc_error, arcpy_messages, exc_info=True)
        except Exception as generic_error:
            log.error("❌ Unexpected error processing JSON/GeoJSON %s → %s: %s",
                      json_file_path.name, out_fc_full_path, generic_error, exc_info=True)

    def _process_single_shapefile(self, shp_file_path: Path, authority: str, used_names_set: Set[str]) -> None:
        """Process a single shapefile into the GDB with comprehensive error handling and validation."""
        log.debug("[DEBUG] Processing shapefile - Authority: '%s' for file: %s", authority, shp_file_path.name)
        original_workspace: Optional[str] = arcpy.env.workspace  # type: ignore[attr-defined]
        
        if not shp_file_path.exists():
            log.error("❌ Shapefile does not exist: %s", shp_file_path)
            return
        
        # Initialize variables before try block to ensure they're always bound
        tgt_name: str = "UNKNOWN"
        out_fc_full_path: str = "UNKNOWN_PATH"
        
        # Enhanced shapefile validation with corrected path logic
        validation_result: ShapefileValidationResult = self._validate_shapefile_components(shp_file_path)
        
        if not validation_result.is_valid:
            log.warning("⚠️ Shapefile validation failed for %s: %s", 
                       shp_file_path.name, validation_result.error_message)
            
            # List what files are actually present for debugging
            self._log_directory_contents(shp_file_path.parent, "shapefile validation failed")
            
            # Try to find alternative shapefiles in the same directory
            alternative_shp: Optional[Path] = self._find_alternative_shapefile(shp_file_path.parent)
            if alternative_shp and alternative_shp != shp_file_path:
                log.info("🔄 Found alternative shapefile: %s", alternative_shp.name)
                self._process_single_shapefile(alternative_shp, authority, used_names_set)
                return
            else:
                log.error("❌ No valid alternative shapefiles found in directory: %s", shp_file_path.parent)
                return
        
        try:
            # Set workspace to the directory containing the shapefile
            shp_directory: str = str(shp_file_path.parent)
            arcpy.env.workspace = shp_directory  # type: ignore[attr-defined]
            
            # Use just the filename (without path) for ArcPy input
            input_shp_name: str = shp_file_path.name
            
            base_name: str = generate_fc_name(authority, shp_file_path.stem)
            tgt_name = self._ensure_unique_name(base_name, used_names_set)
            out_fc_full_path = str(self.gdb_path / tgt_name)
            
            log.info("📥 Copying SHP ('%s') → GDB:/'%s' (Authority: '%s')",
                     shp_file_path.name, tgt_name, authority)
            
            arcpy.management.CopyFeatures(
                in_features=input_shp_name,  # Use filename only
                out_feature_class=out_fc_full_path
            )
            log.info("✅ SUCCESS: Copied shapefile '%s' to '%s'", shp_file_path.name, tgt_name)
            
        except arcpy.ExecuteError as arc_error:
            arcpy_messages: str = arcpy.GetMessages(2)
            log.error("❌ arcpy.management.CopyFeatures failed for SHP %s → %s: %s. ArcPy Messages: %s",
                      shp_file_path.name, tgt_name, arcpy_messages, exc_info=True)
            
            # Try alternative approach with full path if workspace method fails
            if "000732" in arcpy_messages:  # Dataset does not exist error
                self._retry_shapefile_with_full_path(shp_file_path, out_fc_full_path, tgt_name)
                    
        except Exception as generic_error:
            log.error("❌ Unexpected error processing SHP %s → %s: %s", 
                     shp_file_path.name, out_fc_full_path, generic_error, exc_info=True)
        finally:
            arcpy.env.workspace = original_workspace  # type: ignore[attr-defined]

    def _retry_shapefile_with_full_path(self, shp_file_path: Path, out_fc_full_path: str, tgt_name: str) -> None:
        """Retry shapefile processing with full path approach."""
        log.info("🔄 Retrying with full path approach for shapefile: %s", shp_file_path.name)
        try:
            input_shp_full_path: str = str(shp_file_path.resolve())
            arcpy.management.CopyFeatures(
                in_features=input_shp_full_path,
                out_feature_class=out_fc_full_path
            )
            log.info("✅ SUCCESS (retry): Copied shapefile '%s' to '%s'", shp_file_path.name, tgt_name)
        except arcpy.ExecuteError as retry_arc_error:
            log.error("❌ Retry also failed for SHP %s → %s: %s", 
                     shp_file_path.name, tgt_name, arcpy.GetMessages(2), exc_info=True)
        except Exception as retry_generic_error:
            log.error("❌ Unexpected error on retry for SHP %s → %s: %s", 
                     shp_file_path.name, out_fc_full_path, retry_generic_error, exc_info=True)

    def _validate_shapefile_components(self, shp_file_path: Path) -> ShapefileValidationResult:
        """Validate that all required shapefile components exist with corrected path logic."""
        # The key fix: use the full filename stem, not just the base path
        # For 'lst.vbk_vindkraftverk.shp' -> stem is 'lst.vbk_vindkraftverk'
        shp_stem: str = shp_file_path.stem  # This gets 'lst.vbk_vindkraftverk'
        shp_directory: Path = shp_file_path.parent
        
        # Required components for a valid shapefile
        required_extensions: List[str] = ['.shx', '.dbf']
        missing_components: List[str] = []
        
        log.debug("Validating shapefile components for stem: '%s' in directory: %s", 
                 shp_stem, shp_directory)
        
        for ext in required_extensions:
            # Correctly construct the component file path
            component_file: Path = shp_directory / f"{shp_stem}{ext}"
            log.debug("Checking for component file: %s", component_file)
            
            if not component_file.exists():
                missing_components.append(ext)
                log.debug("Missing component: %s", component_file)
            else:
                log.debug("Found component: %s", component_file)
        
        if missing_components:
            error_msg: str = f"Missing required components: {', '.join(missing_components)}"
            return ShapefileValidationResult(False, error_msg, missing_components)
        
        log.debug("✅ All shapefile components validated for: %s", shp_file_path.name)
        return ShapefileValidationResult(True, "All components present", [])

    def _find_alternative_shapefile(self, directory: Path) -> Optional[Path]:
        """Find a valid alternative shapefile in the given directory."""
        if not directory.exists():
            return None
        
        # Look for all .shp files in the directory
        shp_files: List[Path] = list(directory.glob("*.shp"))
        
        log.debug("Found %d shapefile(s) in directory %s", len(shp_files), directory)
        
        for shp_file in shp_files:
            log.debug("Validating shapefile: %s", shp_file.name)
            validation_result: ShapefileValidationResult = self._validate_shapefile_components(shp_file)
            if validation_result.is_valid:
                log.info("✅ Found valid shapefile: %s", shp_file.name)
                return shp_file
            else:
                log.debug("Invalid shapefile %s: %s", shp_file.name, validation_result.error_message)
        
        log.warning("⚠️ No valid shapefiles found in directory: %s", directory)
        return None

    def _log_directory_contents(self, directory: Path, context: str) -> None:
        """Log directory contents for debugging purposes."""
        if not directory.exists():
            log.debug("Directory does not exist for logging: %s", directory)
            return
        
        try:
            files: List[Path] = list(directory.iterdir())
            log.debug("Directory contents (%s) for %s:", context, directory)
            for file_path in sorted(files):
                if file_path.is_file():
                    log.debug("  📄 %s (%d bytes)", file_path.name, file_path.stat().st_size)
                elif file_path.is_dir():
                    log.debug("  📁 %s/", file_path.name)
        except Exception as e:
            log.warning("Could not list directory contents for %s: %s", directory, e)

    def _reset_gdb(self) -> None:
        """Reset (delete and recreate) the target GDB with comprehensive error handling."""
        gdb_full_path: Path = self.gdb_path.resolve()
        log.info("🔄 Target GDB path for reset: %s", gdb_full_path)
        
        if self.gdb_path.exists():
            self._remove_existing_gdb(gdb_full_path)
        else:
            log.info("ℹ️ GDB does not currently exist at %s, no removal needed.", gdb_full_path)
        
        self._ensure_parent_directory_exists()
        self._create_new_gdb(gdb_full_path)

    def _remove_existing_gdb(self, gdb_full_path: Path) -> None:
        """Remove existing GDB with error handling."""
        log.info("🗑️ Attempting to remove existing GDB: %s", gdb_full_path)
        try:
            shutil.rmtree(self.gdb_path)
            log.info("✅ Successfully removed existing GDB: %s", gdb_full_path)
        except Exception as removal_error:
            log.error("❌ Failed to remove existing GDB '%s': %s", gdb_full_path, removal_error, exc_info=True)
            raise RuntimeError(f"Failed to remove existing GDB '{gdb_full_path}': {removal_error}") from removal_error

    def _ensure_parent_directory_exists(self) -> None:
        """Ensure the parent directory for the GDB exists."""
        parent_dir: Path = self.gdb_path.parent.resolve()
        if not parent_dir.exists():
            log.info("🆕 Parent directory %s for GDB does not exist. Attempting to create.", parent_dir)
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
                log.info("✅ Successfully created parent directory: %s", parent_dir)
            except Exception as dir_creation_error:
                log.error("❌ Failed to create parent directory '%s' for GDB: %s", 
                         parent_dir, dir_creation_error, exc_info=True)
                raise RuntimeError(f"Failed to create parent directory '{parent_dir}' for GDB: {dir_creation_error}") from dir_creation_error

    def _create_new_gdb(self, gdb_full_path: Path) -> None:
        """Create a new FileGDB with error handling."""
        log.info("🆕 Attempting to create new FileGDB: %s in folder %s",
                 self.gdb_path.name, self.gdb_path.parent.resolve())
        try:
            arcpy.management.CreateFileGDB(str(self.gdb_path.parent), self.gdb_path.name)
            log.info("✅ Successfully created new GDB: %s", gdb_full_path)
        except arcpy.ExecuteError as gdb_creation_error:
            msg: str = arcpy.GetMessages(2)
            log.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s", gdb_full_path, msg, exc_info=True)
            raise RuntimeError(f"CreateFileGDB failed for '{gdb_full_path}': {msg}") from None
        except Exception as unexpected_gdb_error:
            log.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s",
                      gdb_full_path, unexpected_gdb_error, exc_info=True)
            raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_full_path}': {unexpected_gdb_error}") from unexpected_gdb_error

    def _copy_gpkg_contents_to_staging_gdb(
        self, 
        gpkg_file_path: Path, 
        authority: str, 
        used_names_set: Set[str], 
        include_filter: Optional[List[str]] = None
    ) -> None:
        """Copy GPKG contents to staging GDB with comprehensive error handling."""
        log.info("📦 Processing GeoPackage: %s (Authority: '%s', Include Filter: %s)",
                 gpkg_file_path.relative_to(paths.ROOT), authority, include_filter or "None")
        
        if not gpkg_file_path.exists():
            log.error("❌ GeoPackage file does not exist: %s", gpkg_file_path)
            return
        
        gpkg_workspace: str = str(gpkg_file_path)
        current_arc_workspace: Optional[str] = arcpy.env.workspace  # type: ignore[attr-defined]
        
        try:
            arcpy.env.workspace = gpkg_workspace  # type: ignore[attr-defined]
            log.debug("Temporarily set workspace to GPKG: %s", gpkg_workspace)
            
            feature_classes_in_gpkg: List[str] = arcpy.ListFeatureClasses()
            if not feature_classes_in_gpkg:
                log.info("ℹ️ No feature classes found in GeoPackage: %s", gpkg_file_path.name)
                return
            
            log.info("Found %d feature classes in %s: %s",
                     len(feature_classes_in_gpkg), gpkg_file_path.name, feature_classes_in_gpkg)
            
            normalized_include_filter: Optional[Set[str]] = None
            if include_filter:
                normalized_include_filter = {_MAIN_RE.sub("", item).lower() for item in include_filter if item}
                log.info("Normalized include filter for %s: %s", gpkg_file_path.name, normalized_include_filter)
            
            for fc_name_listed_by_arcpy in feature_classes_in_gpkg:
                self._process_gpkg_feature_class(
                    fc_name_listed_by_arcpy, authority, used_names_set, normalized_include_filter
                )
                
        except Exception as gpkg_processing_error:
            log.error("❌ Failed to list or process feature classes in GeoPackage '%s': %s",
                      gpkg_file_path.name, gpkg_processing_error, exc_info=True)
        finally:
            arcpy.env.workspace = current_arc_workspace  # type: ignore[attr-defined]
            log.debug("Restored workspace after GPKG %s to: %s", gpkg_file_path.name, arcpy.env.workspace)  # type: ignore[attr-defined]

    def _process_gpkg_feature_class(
        self,
        fc_name_listed_by_arcpy: str,
        authority: str,
        used_names_set: Set[str],
        normalized_include_filter: Optional[Set[str]]
    ) -> None:
        """Process a single feature class from GPKG."""
        stem_for_output_naming: str = _MAIN_RE.sub("", fc_name_listed_by_arcpy)
        
        if normalized_include_filter:
            comparable_fc_name: str = stem_for_output_naming.lower()
            if comparable_fc_name not in normalized_include_filter:
                log.info("⏭️ Skipping GPKG FC '%s' (normalized: '%s') as it's not in the include filter.",
                         fc_name_listed_by_arcpy, comparable_fc_name)
                return
            else:
                log.info("✅ GPKG FC '%s' (normalized: '%s') is in include filter. Proceeding.",
                         fc_name_listed_by_arcpy, comparable_fc_name)
        
        if stem_for_output_naming != fc_name_listed_by_arcpy:
            log.info("Stripped 'main.' from '%s' → '%s' for output naming",
                     fc_name_listed_by_arcpy, stem_for_output_naming)
        
        base_name: str = generate_fc_name(authority, stem_for_output_naming)
        tgt_name: str = self._ensure_unique_name(base_name, used_names_set)
        
        self._copy_gpkg_feature_class(fc_name_listed_by_arcpy, tgt_name)

    def _copy_gpkg_feature_class(self, input_fc_name: str, target_name: str) -> None:
        """Copy a single feature class from GPKG to GDB with retry logic and comprehensive error handling."""
        copied_successfully: bool = False
        
        log.info("Attempt 1: GPKG FC copy using input '%s' (listed name) → STAGING_GDB:/'%s'",
                 input_fc_name, target_name)
        
        try:
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=input_fc_name, 
                out_path=str(self.gdb_path), 
                out_name=target_name
            )
            log.info("✅ Attempt 1 SUCCESS: Copied GPKG FC '%s' to '%s'", input_fc_name, target_name)
            copied_successfully = True
            
        except arcpy.ExecuteError as attempt1_error:
            arcpy_messages_e1: str = arcpy.GetMessages(2)
            log.warning("⚠️ Attempt 1 FAILED for input '%s': %s. ArcPy Messages: %s",
                       input_fc_name, attempt1_error, arcpy_messages_e1)
            
            if "000732" in arcpy_messages_e1 and _MAIN_RE.match(input_fc_name):
                copied_successfully = self._retry_gpkg_with_stripped_name(input_fc_name, target_name)
        except Exception as attempt1_generic_error:
            log.error("❌ Unexpected error on Attempt 1 for input '%s': %s",
                      input_fc_name, attempt1_generic_error, exc_info=True)
        
        if not copied_successfully:
            log.error("❗ Ultimately FAILED to copy GPKG FC '%s' to staging GDB.", input_fc_name)

    def _retry_gpkg_with_stripped_name(self, input_fc_name: str, target_name: str) -> bool:
        """Retry GPKG feature class copy with stripped name."""
        input_fc_name_attempt2: str = _MAIN_RE.sub("", input_fc_name)
        if input_fc_name_attempt2 != input_fc_name:
            log.info("Attempt 2: GPKG FC copy using input '%s' (stripped name) → STAGING_GDB:/'%s'",
                     input_fc_name_attempt2, target_name)
            try:
                arcpy.conversion.FeatureClassToFeatureClass(
                    in_features=input_fc_name_attempt2, 
                    out_path=str(self.gdb_path), 
                    out_name=target_name
                )
                log.info("✅ Attempt 2 SUCCESS: Copied GPKG FC '%s' (listed as '%s') to '%s'",
                         input_fc_name_attempt2, input_fc_name, target_name)
                return True
            except arcpy.ExecuteError as attempt2_error:
                arcpy_messages_e2: str = arcpy.GetMessages(2)
                log.error("❌ Attempt 2 FAILED for input '%s': %s. ArcPy Messages: %s",
                          input_fc_name_attempt2, attempt2_error, arcpy_messages_e2, exc_info=True)
            except Exception as attempt2_generic_error:
                log.error("❌ Unexpected error on Attempt 2 for input '%s': %s",
                          input_fc_name_attempt2, attempt2_generic_error, exc_info=True)
        return False

    def _glob_and_load_shapefiles(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """Fallback: glob and load all shapefiles if no sources are defined for loader."""
        shp_files: List[Path] = list(staging_root.rglob("*.shp"))
        if shp_files:
            log.info("🔍 Fallback: Found %d shapefile(s) to process via globbing.", len(shp_files))
            for shp_file_path in shp_files:
                derived_authority_from_path: str = self._derive_authority_from_path(shp_file_path, staging_root)
                self._process_single_shapefile(shp_file_path, derived_authority_from_path, used_names_set)
        else:
            log.info("🔍 Fallback: No shapefiles found in staging area via globbing.")

    def _glob_and_load_geopackages(self, staging_root: Path, used_names_set: Set[str], 
                                  include_filter: Optional[List[str]]) -> None:
        """Fallback: glob and load all geopackages if no sources are defined for loader."""
        gpkg_files: List[Path] = list(staging_root.rglob("*.gpkg"))
        if gpkg_files:
            log.info("🔍 Fallback: Found %d GeoPackage(s) to process via globbing.", len(gpkg_files))
            for gpkg_file_path in gpkg_files:
                derived_authority_from_path: str = self._derive_authority_from_path(gpkg_file_path, staging_root)
                self._copy_gpkg_contents_to_staging_gdb(
                    gpkg_file_path, derived_authority_from_path, used_names_set, include_filter
                )
        else:
            log.info("🔍 Fallback: No GeoPackages found in staging area via globbing.")

    def _glob_and_load_geojsonfiles(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """Fallback: glob and load all GeoJSON and JSON files if no sources are defined for loader."""
        geojson_files: List[Path] = list(staging_root.rglob("*.geojson"))
        json_files: List[Path] = list(staging_root.rglob("*.json"))
        all_json_files: List[Path] = geojson_files + json_files
        
        if all_json_files:
            log.info("🔍 Fallback: Found %d JSON/GeoJSON file(s) to process via globbing.", len(all_json_files))
            for json_file_path in all_json_files:
                derived_authority_from_path: str = self._derive_authority_from_path(json_file_path, staging_root)
                self._process_json_geojson_file(json_file_path, derived_authority_from_path, used_names_set)
        else:
            log.info("🔍 Fallback: No JSON/GeoJSON files found in staging area via globbing.")

    def _derive_authority_from_path(self, file_path: Path, staging_root: Path) -> str:
        """Helper to derive authority from file path structure."""
        try:
            path_parts: tuple[str, ...] = file_path.relative_to(staging_root).parts
            return path_parts[0] if len(path_parts) > 1 else "UNKNOWN_GLOB_AUTH"
        except (IndexError, ValueError):
            return "UNKNOWN_GLOB_AUTH_EXC"

    @staticmethod
    def _ensure_unique_name(base_name: str, used_names: Set[str], max_length: int = 64) -> str:
        """Ensure the name is unique within the GDB, with simplified logic."""
        candidate: str = base_name[:max_length]  # Truncate to max length first
        
        if not candidate:
            raise ValueError(f"Base name '{base_name}' resulted in empty string after truncation")
        
        final_candidate: str = candidate
        idx: int = 1
        
        while final_candidate.lower() in (n.lower() for n in used_names):
            suffix: str = f"_{idx}"
            # Calculate available space for the base part
            available_length: int = max_length - len(suffix)
            
            if available_length <= 0:
                raise ValueError(f"Cannot generate unique name for '{base_name}' within {max_length} characters")
            
            truncated_base: str = candidate[:available_length]
            final_candidate = f"{truncated_base}{suffix}"
            idx += 1
            
            if idx > 9999:
                raise ValueError(f"Could not find unique name for '{base_name}' after {idx-1} attempts")
        
        used_names.add(final_candidate)
        return final_candidate