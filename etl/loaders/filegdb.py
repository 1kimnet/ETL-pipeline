# etl/loaders/filegdb.py
"""🗄️ Main FileGDB loader coordinator - clean architecture."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, List, Optional, Set

import arcpy

from ..models import Source
from ..utils import paths, ensure_dirs
from ..utils.naming import sanitize_for_filename
from ..utils.gdb_utils import ensure_unique_name, reset_gdb
from ..utils.path_utils import derive_authority_from_path
from ..utils.run_summary import Summary
from .geojson_loader import process_geojson_file
from .gpkg_loader import process_gpkg_contents
from .shapefile_loader import process_shapefile

log: Final = logging.getLogger(__name__)


class ArcPyFileGDBLoader:
    """🗄️ Main coordinator for building staging.gdb from staged data."""

    def __init__(
        self, 
        summary: Summary,
        gdb_path: Optional[Path] = None, 
        sources_yaml_path: Optional[Path] = None,
    ) -> None:
        """🔧 Initialize the FileGDB loader."""
        ensure_dirs()
        self.gdb_path: Path = gdb_path or paths.GDB
        self.sources: List[Source] = []
        self.summary = summary

        if sources_yaml_path:
            self._load_sources_configuration(sources_yaml_path)
        else:
            log.warning("⚠️ sources_yaml_path not provided. Using fallback globbing.")

    def _load_sources_configuration(self, sources_yaml_path: Path) -> None:
        """📋 Load and validate sources configuration from YAML file."""
        try:
            self.sources = Source.load_all(sources_yaml_path)
            if not self.sources and sources_yaml_path.exists():
                log.warning("⚠️ Source.load_all returned empty list. Check YAML format.")
            elif not sources_yaml_path.exists():
                log.warning("⚠️ Sources YAML file does not exist: %s", sources_yaml_path)
        except Exception as exc:
            log.error("❌ Failed to load sources from %s: %s", sources_yaml_path, exc, exc_info=True)

    def run(self) -> None:
        """🔄 Execute the complete FileGDB loading process."""
        log.info("🔄 Starting FileGDB loading process")
        
        # Ensure GDB exists
        self._ensure_gdb_exists()
        
        # Track used names for uniqueness
        used_names_set: Set[str] = set()
        staging_root = paths.STAGING
        
        if self.sources:
            log.info("📋 Processing %d configured sources", len(self.sources))
            self._process_configured_sources(staging_root, used_names_set)
        else:
            log.warning("⚠️ No sources configured. Using fallback globbing.")
            self._perform_fallback_globbing(staging_root, used_names_set)
        
        log.info("✅ FileGDB loading process completed")

    def _ensure_gdb_exists(self) -> None:
        """🗄️ Ensure the target GDB exists."""
        if not self.gdb_path.exists():
            log.info("🆕 Creating new FileGDB: %s", self.gdb_path)
            try:
                arcpy.management.CreateFileGDB(
                    str(self.gdb_path.parent), 
                    self.gdb_path.name
                )
                log.info("✅ Successfully created GDB: %s", self.gdb_path)
            except Exception as e:
                log.error("❌ Failed to create GDB: %s", e, exc_info=True)
                raise

    def _process_configured_sources(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """📋 Process sources based on their configuration."""
        for source in self.sources:
            if not source.enabled:
                log.info("⏭️ Skipping disabled source: %s", source.name)
                continue
                
            try:
                log.info("🔄 Processing source: %s (Authority: %s)", source.name, source.authority)
                self._process_single_source(source, staging_root, used_names_set)
            except Exception as exc:
                log.error("❌ Failed to process source '%s': %s", source.name, exc, exc_info=True)

    def _process_single_source(self, source: Source, staging_root: Path, used_names_set: Set[str]) -> None:
        """🔄 Process a single source based on its type."""
        source_staging_dir = staging_root / source.authority / sanitize_for_filename(source.name)
        normalized_data_type = self._normalize_staged_data_type(source.staged_data_type)
        
        log.info("🔍 Processing source '%s': staged_data_type='%s', normalized='%s'", 
                 source.name, source.staged_data_type, normalized_data_type)
        
        if normalized_data_type == "gpkg":
            # ALWAYS use GPKG loader when configured as GPKG, regardless of other files present
            log.info("📦 Source '%s' configured for GPKG - using GPKG loader exclusively", source.name)
            self._handle_gpkg_source(source, source_staging_dir, used_names_set)
        elif normalized_data_type in ("geojson", "json"):
            self._handle_geojson_source(source, source_staging_dir, used_names_set)
        elif normalized_data_type == "shapefile_collection":
            log.info("📐 Handling shapefile collection source '%s'", source.name)
            self._handle_shapefile_source(source, source_staging_dir, staging_root, used_names_set)
        elif not normalized_data_type:
            # When no staged_data_type is specified, fall back to shapefile processing
            self._handle_shapefile_source(source, source_staging_dir, staging_root, used_names_set)
        else:
            log.warning("🤷 Unknown staged_data_type '%s' for source '%s'. Skipping.",
                       normalized_data_type, source.name)

    def _normalize_staged_data_type(self, staged_data_type: Optional[str]) -> Optional[str]:
        """🔧 Normalize staged_data_type values to handle common variations."""
        if not staged_data_type:
            return staged_data_type
        
        normalized = staged_data_type.lower().strip()
        
        if normalized in ["geopackage", "gpkg"]:
            return "gpkg"
        elif normalized in ["shapefile", "shp", "shapefile_collection"]:
            return "shapefile_collection"
        
        return normalized

    def _handle_gpkg_source(self, source: Source, source_staging_dir: Path, used_names_set: Set[str]) -> None:
        """📦 Handle GPKG source loading."""
        if not source_staging_dir.exists():
            log.warning("⚠️ Staging directory not found for GPKG source '%s': %s",
                       source.name, source_staging_dir)
            return

        expected_gpkg_path = source_staging_dir / f"{sanitize_for_filename(source.name)}.gpkg"
        
        if expected_gpkg_path.exists():
            process_gpkg_contents(
                expected_gpkg_path, source.authority, self.gdb_path, 
                used_names_set, self.summary, source.include
            )
        else:
            # Look for any GPKG files in the directory
            gpkg_files = list(source_staging_dir.glob("*.gpkg"))
            if gpkg_files:
                log.info("📦 Found %d GPKG file(s) for source '%s'", len(gpkg_files), source.name)
                for gpkg_file in gpkg_files:
                    process_gpkg_contents(
                        gpkg_file, source.authority, self.gdb_path,
                        used_names_set, self.summary, source.include
                    )
            else:
                log.warning("⚠️ No GPKG file found for source '%s' in dir %s", source.name, source_staging_dir)

    def _handle_geojson_source(self, source: Source, source_staging_dir: Path, used_names_set: Set[str]) -> None:
        """🌍 Handle JSON/GeoJSON source loading."""
        if not source_staging_dir.exists():
            log.warning("⚠️ Staging directory not found for JSON/GeoJSON source '%s': %s",
                       source.name, source_staging_dir)
            return

        geojson_files = list(source_staging_dir.glob("*.geojson"))
        json_files = list(source_staging_dir.glob("*.json"))
        all_json_files = geojson_files + json_files
        
        if all_json_files:
            log.info("🌍 Found %d JSON/GeoJSON file(s) for source '%s'.",
                     len(all_json_files), source.name)
            for json_file_path in all_json_files:
                process_geojson_file(
                    json_file_path, source.authority, self.gdb_path,
                    used_names_set, self.summary
                )
        else:
            log.info("ℹ️ No JSON/GeoJSON files found for source '%s'.", source.name)

    def _handle_shapefile_source(
        self, 
        source: Source, 
        source_staging_dir: Path, 
        staging_root: Path, 
        used_names_set: Set[str]
    ) -> None:
        """📐 Handle shapefile collection source loading."""
        # Process shapefiles based on configuration
        if source.include and source.type == "file":
            self._process_multi_part_shapefile_collection(source, staging_root, used_names_set)
        else:
            self._process_single_shapefile_source(source, source_staging_dir, used_names_set)

    def _process_multi_part_shapefile_collection(
        self, source: Source, staging_root: Path, used_names_set: Set[str]
    ) -> None:
        """📐 Process multi-part shapefile collections based on include list."""
        log.info("📐 Processing multi-part shapefile collection for '%s'.", source.name)
        
        for included_item_stem in source.include or []:
            sanitized_item_stem = sanitize_for_filename(included_item_stem)
            item_staging_dir = staging_root / source.authority / sanitized_item_stem
            
            if not item_staging_dir.exists():
                log.warning("⚠️ Staging subdirectory not found for item '%s': %s",
                           included_item_stem, item_staging_dir)
                continue
            
            shp_files_in_item_dir = list(item_staging_dir.rglob("*.shp"))
            if shp_files_in_item_dir:
                log.info("📐 Found %d shapefile(s) in item dir '%s'.",
                         len(shp_files_in_item_dir), item_staging_dir.name)
                for shp_file_path in shp_files_in_item_dir:
                    process_shapefile(
                        shp_file_path, source.authority, self.gdb_path,
                        used_names_set, self.summary
                    )

    def _process_single_shapefile_source(
        self, source: Source, source_staging_dir: Path, used_names_set: Set[str]
    ) -> None:
        """📐 Process a single shapefile source."""
        if not source_staging_dir.exists():
            log.warning("⚠️ Staging directory not found for source '%s': %s",
                       source.name, source_staging_dir)
            return
        
        shp_files_in_dir = list(source_staging_dir.rglob("*.shp"))
        if shp_files_in_dir:
            log.info("📐 Found %d shapefile(s) for source '%s'.",
                     len(shp_files_in_dir), source.name)
            for shp_file_path in shp_files_in_dir:
                process_shapefile(
                    shp_file_path, source.authority, self.gdb_path,
                    used_names_set, self.summary
                )
        else:
            log.info("ℹ️ No shapefiles found for source '%s'.", source.name)

    def _perform_fallback_globbing(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """🔍 Perform fallback globbing when no source configuration is available."""
        log.warning("⚠️ No Source configurations loaded. Using fallback globbing.")
        
        self._glob_and_load_shapefiles(staging_root, used_names_set)
        self._glob_and_load_geopackages(staging_root, used_names_set)
        self._glob_and_load_geojsonfiles(staging_root, used_names_set)

    def _glob_and_load_shapefiles(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """📐 Fallback: glob and load all shapefiles."""
        shp_files = list(staging_root.rglob("*.shp"))
        if shp_files:
            log.info("🔍 Fallback: Found %d shapefile(s) to process.", len(shp_files))
            for shp_file_path in shp_files:
                derived_authority = derive_authority_from_path(shp_file_path, staging_root)
                process_shapefile(
                    shp_file_path, derived_authority, self.gdb_path,
                    used_names_set, self.summary
                )
        else:
            log.info("🔍 Fallback: No shapefiles found.")

    def _glob_and_load_geopackages(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """📦 Fallback: glob and load all geopackages."""
        gpkg_files = list(staging_root.rglob("*.gpkg"))
        if gpkg_files:
            log.info("🔍 Fallback: Found %d GeoPackage(s) to process.", len(gpkg_files))
            for gpkg_file_path in gpkg_files:
                derived_authority = derive_authority_from_path(gpkg_file_path, staging_root)
                process_gpkg_contents(
                    gpkg_file_path, derived_authority, self.gdb_path, 
                    used_names_set, self.summary, None
                )
        else:
            log.info("🔍 Fallback: No GeoPackages found.")

    def _glob_and_load_geojsonfiles(self, staging_root: Path, used_names_set: Set[str]) -> None:
        """🌍 Fallback: glob and load all GeoJSON and JSON files."""
        geojson_files = list(staging_root.rglob("*.geojson"))
        json_files = list(staging_root.rglob("*.json"))
        all_json_files = geojson_files + json_files
        
        if all_json_files:
            log.info("🔍 Fallback: Found %d JSON/GeoJSON file(s) to process.", len(all_json_files))
            for json_file_path in all_json_files:
                derived_authority = derive_authority_from_path(json_file_path, staging_root)
                process_geojson_file(
                    json_file_path, derived_authority, self.gdb_path,
                    used_names_set, self.summary
                )
        else:
            log.info("🔍 Fallback: No JSON/GeoJSON files found.")

    def load_from_staging(self, staging_root: Path) -> None:
        """🔄 Compatibility method that mimics the old interface."""
        log.info("🔄 Starting FileGDB loading from staging directory: %s", staging_root)
        
        # Reset GDB to start fresh
        reset_gdb(self.gdb_path)
        
        # Track used names for uniqueness  
        used_names_set: Set[str] = set()
        
        if self.sources:
            log.info("📋 Processing %d configured sources", len(self.sources))
            self._process_configured_sources(staging_root, used_names_set)
        else:
            log.warning("⚠️ No sources configured. Using fallback globbing.")
            self._perform_fallback_globbing(staging_root, used_names_set)
        
        log.info("✅ FileGDB loading from staging completed")


# Maintain compatibility with existing code that expects a different interface
def load_from_staging(
    staging_root: Path, 
    gdb_path: Optional[Path] = None, 
    sources_yaml_path: Optional[Path] = None,
    summary: Optional[Summary] = None
) -> None:
    """🔄 Compatibility function for existing code that uses the old interface."""
    from ..utils.run_summary import Summary as DefaultSummary
    
    if summary is None:
        summary = DefaultSummary()
    
    loader = ArcPyFileGDBLoader(
        summary=summary,
        gdb_path=gdb_path,
        sources_yaml_path=sources_yaml_path
    )
    
    # Use reset_gdb to clear existing content
    if gdb_path:
        reset_gdb(gdb_path)
    else:
        reset_gdb(paths.GDB)
    
    # Track used names for uniqueness
    used_names_set: Set[str] = set()
    
    if loader.sources:
        loader._process_configured_sources(staging_root, used_names_set)
    else:
        loader._perform_fallback_globbing(staging_root, used_names_set)
