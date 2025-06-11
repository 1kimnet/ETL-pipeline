"""Output mapping management for ETL pipeline.

This module handles the mapping between staging feature classes and production
SDE datasets/feature classes. It provides flexible naming and organization
while maintaining backward compatibility with default naming logic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml

from .exceptions import ConfigurationError, ValidationError
from .models import Source
from .utils.naming import sanitize_for_arcgis_name

log = logging.getLogger(__name__)


@dataclass
class OutputMapping:
    """Mapping between staging feature class and SDE output."""
    staging_fc: str
    sde_fc: str
    sde_dataset: str
    description: Optional[str] = None
    enabled: bool = True
    schema: Optional[str] = None
    
    def __post_init__(self):
        """Validate mapping configuration."""
        if not self.staging_fc.strip():
            raise ValidationError("staging_fc cannot be empty")
        if not self.sde_fc.strip():
            raise ValidationError("sde_fc cannot be empty")
        if not self.sde_dataset.strip():
            raise ValidationError("sde_dataset cannot be empty")


@dataclass
class MappingSettings:
    """Global settings for output mappings."""
    default_schema: str = "GNG"
    default_dataset_pattern: str = "Underlag_{authority}"
    default_fc_pattern: str = "{authority}_{source_name}"
    validate_datasets: bool = True
    create_missing_datasets: bool = True
    skip_unmappable_sources: bool = False


class MappingManager:
    """Manages output mappings between staging and production."""
    
    def __init__(self, mappings_file: Optional[Path] = None):
        self.mappings_file = mappings_file
        self.mappings: Dict[str, OutputMapping] = {}
        self.settings = MappingSettings()
        
        if mappings_file:
            self.load_mappings(mappings_file)
        
        log.info("🗺️  Mapping manager initialized with %d mappings", len(self.mappings))
    
    def load_mappings(self, mappings_file: Path) -> None:
        """🔄 Load mappings from YAML file.
        
        Args:
            mappings_file: Path to the mappings YAML file.
            
        Raises:
            ConfigurationError: If the file cannot be loaded or parsed.
        """
        if not mappings_file.exists():
            log.info("📋 No mappings file found at %s, using defaults only", mappings_file)
            return
            
        try:
            with mappings_file.open('r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                
            # Handle empty file or missing mappings section
            if not content:
                log.info("📋 Empty mappings file, using defaults only")
                return
                
            # Load settings if present
            if 'settings' in content and content['settings']:
                self.settings = MappingSettings(**content['settings'])
                log.info("⚙️  Loaded mapping settings: %s", self.settings)
            
            # Load individual mappings if present
            mappings_data = content.get('mappings', [])
            if not mappings_data:
                log.info("📋 No mappings defined, using defaults only")
                return
                
            for mapping_data in mappings_data:
                try:
                    mapping = OutputMapping(**mapping_data)
                    self.mappings[mapping.staging_fc] = mapping
                    log.debug("📌 Loaded mapping: %s → %s.%s", 
                             mapping.staging_fc, mapping.sde_dataset, mapping.sde_fc)
                except TypeError as e:
                    log.warning("⚠️  Skipping invalid mapping %s: %s", mapping_data, e)
                    continue
                
            log.info("✅ Loaded %d mappings from %s", len(self.mappings), mappings_file)
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in mappings file {mappings_file}: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load mappings from {mappings_file}: {e}") from e
    
    def get_output_mapping(self, source: Source, staging_fc_name: str) -> OutputMapping:
        """Get output mapping for a source, falling back to default logic if not mapped."""
        # Check for explicit mapping first
        if staging_fc_name in self.mappings:
            mapping = self.mappings[staging_fc_name]
            if mapping.enabled:
                log.debug("📍 Using explicit mapping for %s -> %s.%s", 
                         staging_fc_name, mapping.sde_dataset, mapping.sde_fc)
                return mapping
            else:
                log.debug("⏭️ Mapping for %s is disabled, using default logic", staging_fc_name)
        
        # Fall back to default naming logic
        return self._create_default_mapping(source, staging_fc_name)
    
    def _create_default_mapping(self, source: Source, staging_fc_name: str) -> OutputMapping:
        """Create default mapping using naming patterns."""
        # Generate default SDE dataset name
        sde_dataset = self.settings.default_dataset_pattern.format(
            authority=source.authority,
            source_name=sanitize_for_arcgis_name(source.name)
        )
        
        # Generate default SDE feature class name
        sde_fc = self.settings.default_fc_pattern.format(
            authority=source.authority,
            source_name=sanitize_for_arcgis_name(source.name),
            staging_fc=staging_fc_name
        )
        
        # Ensure ArcGIS naming compatibility
        sde_dataset = sanitize_for_arcgis_name(sde_dataset)
        sde_fc = sanitize_for_arcgis_name(sde_fc)
        
        log.debug("🔧 Generated default mapping for %s -> %s.%s", 
                 staging_fc_name, sde_dataset, sde_fc)
        
        return OutputMapping(
            staging_fc=staging_fc_name,
            sde_fc=sde_fc,
            sde_dataset=sde_dataset,
            description=f"Auto-generated mapping for {source.name}",
            schema=self.settings.default_schema
        )
    
    def get_full_sde_path(self, mapping: OutputMapping, sde_connection: str) -> str:
        """Get full SDE path for a mapping."""
        if mapping.schema:
            return f"{sde_connection}\\{mapping.schema}.{mapping.sde_dataset}\\{mapping.sde_fc}"
        else:
            return f"{sde_connection}\\{mapping.sde_dataset}\\{mapping.sde_fc}"
    
    def get_dataset_path(self, mapping: OutputMapping, sde_connection: str) -> str:
        """Get SDE dataset path for a mapping."""
        if mapping.schema:
            return f"{sde_connection}\\{mapping.schema}.{mapping.sde_dataset}"
        else:
            return f"{sde_connection}\\{mapping.sde_dataset}"
    
    def get_mappings_for_dataset(self, dataset_name: str) -> List[OutputMapping]:
        """Get all mappings that target a specific dataset."""
        return [mapping for mapping in self.mappings.values() 
                if mapping.sde_dataset == dataset_name and mapping.enabled]
    
    def get_all_target_datasets(self) -> List[str]:
        """Get list of all target SDE datasets from mappings."""
        datasets = set()
        for mapping in self.mappings.values():
            if mapping.enabled:
                if mapping.schema:
                    datasets.add(f"{mapping.schema}.{mapping.sde_dataset}")
                else:
                    datasets.add(mapping.sde_dataset)
        return list(datasets)
    
    def validate_mapping(self, mapping: OutputMapping) -> List[str]:
        """Validate a mapping configuration and return any issues."""
        issues = []
        
        # Check naming conventions
        if not mapping.staging_fc.replace("_", "").replace("-", "").isalnum():
            issues.append(f"Staging FC name '{mapping.staging_fc}' contains invalid characters")
        
        if not mapping.sde_fc.replace("_", "").isalnum():
            issues.append(f"SDE FC name '{mapping.sde_fc}' contains invalid characters")
        
        if not mapping.sde_dataset.replace("_", "").isalnum():
            issues.append(f"SDE dataset name '{mapping.sde_dataset}' contains invalid characters")
        
        # Check length limits (ArcGIS SDE limits)
        if len(mapping.sde_fc) > 128:
            issues.append(f"SDE FC name '{mapping.sde_fc}' exceeds 128 character limit")
        
        if len(mapping.sde_dataset) > 128:
            issues.append(f"SDE dataset name '{mapping.sde_dataset}' exceeds 128 character limit")
        
        return issues
    
    def validate_all_mappings(self) -> Dict[str, List[str]]:
        """Validate all mappings and return issues by staging FC name."""
        all_issues = {}
        
        for staging_fc, mapping in self.mappings.items():
            issues = self.validate_mapping(mapping)
            if issues:
                all_issues[staging_fc] = issues
        
        return all_issues
    
    def get_mapping_statistics(self) -> Dict[str, Any]:
        """Get statistics about the current mappings."""
        enabled_mappings = [m for m in self.mappings.values() if m.enabled]
        disabled_mappings = [m for m in self.mappings.values() if not m.enabled]
        
        datasets = set(m.sde_dataset for m in enabled_mappings)
        schemas = set(m.schema for m in enabled_mappings if m.schema)
        
        return {
            'total_mappings': len(self.mappings),
            'enabled_mappings': len(enabled_mappings),
            'disabled_mappings': len(disabled_mappings),
            'unique_datasets': len(datasets),
            'unique_schemas': len(schemas),
            'datasets': list(datasets),
            'schemas': list(schemas)
        }
    
    def add_mapping(self, mapping: OutputMapping) -> None:
        """Add a new mapping."""
        validation_issues = self.validate_mapping(mapping)
        if validation_issues:
            raise ValidationError(f"Invalid mapping: {'; '.join(validation_issues)}")
        
        self.mappings[mapping.staging_fc] = mapping
        log.info("➕ Added mapping: %s -> %s.%s", 
                mapping.staging_fc, mapping.sde_dataset, mapping.sde_fc)
    
    def remove_mapping(self, staging_fc: str) -> bool:
        """Remove a mapping by staging FC name."""
        if staging_fc in self.mappings:
            del self.mappings[staging_fc]
            log.info("➖ Removed mapping for: %s", staging_fc)
            return True
        return False
    
    def save_mappings(self, output_file: Optional[Path] = None) -> None:
        """Save current mappings to file."""
        output_file = output_file or self.mappings_file
        
        if not output_file:
            raise ConfigurationError("No output file specified for saving mappings")
        
        # Prepare data for serialization
        mappings_data = []
        for mapping in self.mappings.values():
            mapping_dict: Dict[str, Any] = {
                'staging_fc': mapping.staging_fc,
                'sde_fc': mapping.sde_fc,
                'sde_dataset': mapping.sde_dataset
            }
            
            if mapping.description:
                mapping_dict['description'] = mapping.description
            if not mapping.enabled:
                mapping_dict['enabled'] = mapping.enabled
            if mapping.schema != self.settings.default_schema:
                mapping_dict['schema'] = mapping.schema
            
            mappings_data.append(mapping_dict)
        
        # Prepare settings data
        settings_data = {
            'default_schema': self.settings.default_schema,
            'default_dataset_pattern': self.settings.default_dataset_pattern,
            'default_fc_pattern': self.settings.default_fc_pattern,
            'validate_datasets': self.settings.validate_datasets,
            'create_missing_datasets': self.settings.create_missing_datasets,
            'skip_unmappable_sources': self.settings.skip_unmappable_sources
        }
        
        output_data = {
            'settings': settings_data,
            'mappings': mappings_data
        }
        
        try:
            with output_file.open('w', encoding='utf-8') as f:
                yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)
            
            log.info("💾 Saved %d mappings to %s", len(self.mappings), output_file)
            
        except Exception as e:
            raise ConfigurationError(f"Failed to save mappings to {output_file}: {e}") from e


# Global mapping manager instance
_mapping_manager: Optional[MappingManager] = None


def get_mapping_manager(mappings_file: Optional[Path] = None) -> MappingManager:
    """Get global mapping manager instance."""
    global _mapping_manager
    
    if _mapping_manager is None:
        # Try to find mappings file if not provided
        if mappings_file is None:
            default_paths = [
                Path("config/mappings.yaml"),
                Path("mappings.yaml"),
                Path.cwd() / "config" / "mappings.yaml"
            ]
            
            for path in default_paths:
                if path.exists():
                    mappings_file = path
                    break
        
        _mapping_manager = MappingManager(mappings_file)
    
    return _mapping_manager


def load_mappings_from_config(config: Dict[str, Any]) -> MappingManager:
    """Load mappings based on configuration."""
    mappings_file = None
    
    if 'mappings_file' in config:
        mappings_file = Path(config['mappings_file'])
    
    return MappingManager(mappings_file)