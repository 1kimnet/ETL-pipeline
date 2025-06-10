"""SDE loader with mapping support for ETL pipeline.

This module provides SDE loading functionality that integrates with the
mapping system to handle custom output naming and dataset organization.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Dict, Any

import arcpy

from ..exceptions import GeospatialError, LicenseError
from ..mapping import MappingManager, OutputMapping
from ..models import Source
from ..spatial import get_spatial_operations

log = logging.getLogger(__name__)


class SDELoader:
    """Loads data to SDE geodatabase with mapping support."""
    
    def __init__(
        self,
        sde_connection: str,
        mapping_manager: Optional[MappingManager] = None,
        global_config: Optional[Dict[str, Any]] = None
    ):
        self.sde_connection = sde_connection
        self.mapping_manager = mapping_manager
        self.global_config = global_config or {}
        self.spatial_ops = get_spatial_operations()
        
        # Validate SDE connection
        if not self._validate_sde_connection():
            raise LicenseError(f"Cannot connect to SDE: {sde_connection}")
        
        log.info("🏛️  SDE loader initialized: %s", sde_connection)
    
    def _validate_sde_connection(self) -> bool:
        """Validate SDE connection is accessible."""
        try:
            # Test connection by listing datasets
            arcpy.env.workspace = self.sde_connection
            arcpy.ListDatasets()
            return True
        except Exception as e:
            log.error("❌ SDE connection validation failed: %s", e)
            return False
    
    def load_feature_class(
        self,
        source: Source,
        staging_fc_path: str,
        staging_fc_name: str
    ) -> str:
        """Load feature class from staging to SDE using mappings."""
        try:
            # Get output mapping (explicit or default)
            if self.mapping_manager:
                mapping = self.mapping_manager.get_output_mapping(source, staging_fc_name)
            else:
                # Fallback to simple naming if no mapping manager
                mapping = OutputMapping(
                    staging_fc=staging_fc_name,
                    sde_fc=f"{source.authority}_{staging_fc_name}",
                    sde_dataset=f"Underlag_{source.authority}",
                    schema="GNG"
                )
            
            log.info("📍 Using mapping: %s -> %s.%s", 
                    mapping.staging_fc, mapping.sde_dataset, mapping.sde_fc)
            
            # Ensure target dataset exists
            dataset_path = self._ensure_dataset_exists(mapping)
            
            # Get full target path
            target_fc_path = self._get_target_fc_path(mapping)
            
            # Perform the load operation
            return self._load_to_sde(staging_fc_path, target_fc_path, mapping, source)
            
        except Exception as e:
            raise GeospatialError(
                f"Failed to load {staging_fc_name} to SDE: {e}",
                operation="sde_load"
            ) from e
    
    def _ensure_dataset_exists(self, mapping: OutputMapping) -> str:
        """Ensure target SDE dataset exists."""
        if not self.mapping_manager:
            return ""
        
        dataset_path = self.mapping_manager.get_dataset_path(mapping, self.sde_connection)
        
        try:
            # Check if dataset exists
            if not arcpy.Exists(dataset_path):
                if self.mapping_manager.settings.create_missing_datasets:
                    log.info("📂 Creating SDE dataset: %s", dataset_path)
                    
                    # Create feature dataset with spatial reference
                    sr = arcpy.SpatialReference(3006)  # SWEREF99 TM as default
                    arcpy.management.CreateFeatureDataset(
                        self.sde_connection,
                        mapping.sde_dataset,
                        sr
                    )
                    
                    log.info("✅ Created SDE dataset: %s", dataset_path)
                else:
                    log.warning("⚠️ Dataset does not exist and auto-creation is disabled: %s", dataset_path)
            
            return dataset_path
            
        except Exception as e:
            log.error("❌ Failed to ensure dataset exists: %s", e)
            if self.mapping_manager.settings.skip_unmappable_sources:
                raise GeospatialError(f"Dataset creation failed: {e}", operation="dataset_creation") from e
            return ""
    
    def _get_target_fc_path(self, mapping: OutputMapping) -> str:
        """Get full target feature class path."""
        if self.mapping_manager:
            return self.mapping_manager.get_full_sde_path(mapping, self.sde_connection)
        else:
            return f"{self.sde_connection}\\{mapping.sde_dataset}\\{mapping.sde_fc}"
    
    def _load_to_sde(
        self, 
        source_fc: str, 
        target_fc: str, 
        mapping: OutputMapping,
        source: Source
    ) -> str:
        """Perform the actual load operation to SDE."""
        load_strategy = self.global_config.get("sde_load_strategy", "truncate_and_load")
        
        try:
            if load_strategy == "truncate_and_load":
                return self._truncate_and_load(source_fc, target_fc, mapping)
            elif load_strategy == "replace":
                return self._replace_feature_class(source_fc, target_fc, mapping)
            elif load_strategy == "append":
                return self._append_to_feature_class(source_fc, target_fc, mapping)
            else:
                raise GeospatialError(f"Unknown load strategy: {load_strategy}", operation="sde_load")
                
        except Exception as e:
            log.error("❌ SDE load operation failed: %s", e)
            raise
    
    def _truncate_and_load(self, source_fc: str, target_fc: str, mapping: OutputMapping) -> str:
        """Truncate existing data and load new data."""
        log.info("🗑️  Truncating and loading: %s", target_fc)
        
        # Check if target exists
        if arcpy.Exists(target_fc):
            # Truncate existing data
            arcpy.management.TruncateTable(target_fc)
            log.debug("Truncated existing data from: %s", target_fc)
        else:
            # Create new feature class
            log.debug("Creating new feature class: %s", target_fc)
            
        # Append data from source
        arcpy.management.Append(
            source_fc,
            target_fc,
            "NO_TEST"  # Skip field mapping validation for speed
        )
        
        # Get record count for verification
        record_count = int(arcpy.management.GetCount(target_fc)[0])
        log.info("✅ Loaded %d records to %s", record_count, mapping.sde_fc)
        
        return target_fc
    
    def _replace_feature_class(self, source_fc: str, target_fc: str, mapping: OutputMapping) -> str:
        """Replace entire feature class."""
        log.info("🔄 Replacing feature class: %s", target_fc)
        
        # Delete existing if it exists
        if arcpy.Exists(target_fc):
            arcpy.management.Delete(target_fc)
            log.debug("Deleted existing feature class: %s", target_fc)
        
        # Copy source to target
        arcpy.management.CopyFeatures(source_fc, target_fc)
        
        # Get record count for verification
        record_count = int(arcpy.management.GetCount(target_fc)[0])
        log.info("✅ Replaced with %d records: %s", record_count, mapping.sde_fc)
        
        return target_fc
    
    def _append_to_feature_class(self, source_fc: str, target_fc: str, mapping: OutputMapping) -> str:
        """Append data to existing feature class."""
        log.info("➕ Appending to feature class: %s", target_fc)
        
        # Get initial record count
        initial_count = 0
        if arcpy.Exists(target_fc):
            initial_count = int(arcpy.management.GetCount(target_fc)[0])
        else:
            # Create target if it doesn't exist
            arcpy.management.CopyFeatures(source_fc, target_fc)
            final_count = int(arcpy.management.GetCount(target_fc)[0])
            log.info("✅ Created and loaded %d records to %s", final_count, mapping.sde_fc)
            return target_fc
        
        # Append data
        arcpy.management.Append(source_fc, target_fc, "NO_TEST")
        
        # Get final record count
        final_count = int(arcpy.management.GetCount(target_fc)[0])
        added_count = final_count - initial_count
        
        log.info("✅ Appended %d records to %s (total: %d)", 
                added_count, mapping.sde_fc, final_count)
        
        return target_fc
    
    def get_load_statistics(self) -> Dict[str, Any]:
        """Get statistics about SDE loading operations."""
        # This would be enhanced to track actual statistics
        return {
            "sde_connection": self.sde_connection,
            "mapping_manager_enabled": self.mapping_manager is not None,
            "load_strategy": self.global_config.get("sde_load_strategy", "truncate_and_load")
        }
    
    def validate_target_schema(self, mapping: OutputMapping) -> bool:
        """Validate that target schema/dataset naming is valid."""
        if self.mapping_manager:
            issues = self.mapping_manager.validate_mapping(mapping)
            if issues:
                log.warning("⚠️ Mapping validation issues for %s: %s", 
                           mapping.staging_fc, "; ".join(issues))
                return False
        
        return True