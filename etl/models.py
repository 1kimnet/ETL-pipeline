"""Domain models and SDE loading logic for the ETL pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Final, List, Optional, Tuple

import yaml

from .utils.naming import sanitize_for_arcgis_name

log: Final = logging.getLogger(__name__)


def _parse_include(include_value: str | List[str] | None) -> List[str]:
    """Parse include field from YAML into a list of strings."""
    if include_value is None:
        return []
    
    if isinstance(include_value, list):
        return include_value
    
    if isinstance(include_value, str):
        # Handle semicolon-separated strings
        if ";" in include_value:
            return [item.strip() for item in include_value.split(";") if item.strip()]
        return [include_value]
    
    return []


@dataclass(slots=True, frozen=True)
class AppConfig:
    """Application configuration settings."""
    
    sde_dataset_pattern: str


@dataclass(slots=True, frozen=True)
class Source:
    """Represents a single data source configuration."""

    name: str
    authority: str
    type: str = "file"
    url: str = ""
    staged_data_type: Optional[str] = None
    download_format: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    include: List[str] = field(default_factory=list)

    @classmethod
    def load_all(cls, yaml_path: Path | str) -> List[Source]:
        """🔄 Load all sources from a YAML configuration file.

        Args:
            yaml_path: Path to the YAML configuration file.

        Returns:
            List of Source objects loaded from the file.
        """
        yaml_path = Path(yaml_path)
        
        if not yaml_path.exists():
            log.warning("⚠️ Sources YAML file not found: %s", yaml_path)
            return []

        try:
            with yaml_path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            log.error("❌ Failed to parse YAML file: %s", exc)
            return []
        except Exception as exc:
            log.error("❌ Failed to read YAML file: %s", exc)
            return []

        if not data:
            log.info("📄 Empty YAML file: %s", yaml_path)
            return []

        sources_data = data.get("sources", [])
        if not isinstance(sources_data, list):
            log.warning("⚠️ 'sources' key is not a list in YAML file: %s", yaml_path)
            return []

        sources: List[Source] = []
        for source_data in sources_data:
            try:
                # Parse the include field properly
                include_parsed = _parse_include(source_data.get("include"))
                
                source = cls(
                    name=source_data["name"],
                    authority=source_data["authority"],
                    type=source_data.get("type", "file"),
                    url=source_data.get("url", ""),
                    staged_data_type=source_data.get("staged_data_type"),
                    download_format=source_data.get("download_format"),
                    raw=source_data.get("raw", {}),
                    enabled=source_data.get("enabled", True),
                    include=include_parsed,
                )
                sources.append(source)
                
            except KeyError as exc:
                log.warning(
                    "⚠️ Skipping source with missing field: %s. Source data: %s",
                    exc,
                    source_data,
                )
                continue
            except Exception as exc:
                log.warning(
                    "⚠️ Skipping source due to error: %s. Source data: %s",
                    exc,
                    source_data,
                )
                continue

        log.info("✅ Loaded %d sources from %s", len(sources), yaml_path)
        return sources


class SdeLoader:
    """🔌 Manages the connection and loading process to an SDE database."""

    def __init__(self, config: AppConfig, sources: List[Source]):
        """
        Initialize the SDE loader.

        Args:
            config: The application's global configuration.
            sources: A list of all configured Source objects.        """
        self.sde_dataset_pattern: str = config.sde_dataset_pattern
        self.sources = sources

    def _find_source_for_fc(self, staging_fc_name: str) -> Optional[Source]:
        """Find the original Source object for a staging feature class."""
        log.info("🔍 Looking for source for FC: %s", staging_fc_name)
        log.info("🔍 Available sources: %d enabled sources", len([s for s in self.sources if s.enabled]))
        
        for source in self.sources:
            if not source.enabled:
                continue

            log.info(
                "🔍 Checking source: name='%s', authority='%s', type='%s'",
                source.name,
                source.authority,
                source.type,
            )

            # Method 1: Check if the staging fc name could have been generated from this source
            sanitized_source_name = sanitize_for_arcgis_name(source.name)
            log.info(
                "🔍 Sanitized source name: '%s' vs staging FC: '%s'",
                sanitized_source_name,
                staging_fc_name,
            )
            
            if sanitized_source_name in staging_fc_name:
                log.info(
                    "🔍 ✅ Source matched by name: '%s' ('%s' in '%s')",
                    source.name,
                    sanitized_source_name,
                    staging_fc_name,
                )
                return source

            # Method 2: For REST API sources with multiple layers, the feature class name 
            # might be "{sanitized_source_name}_{layer_suffix}_{geometry_type}"
            if source.type in ("rest_api", "ogc_api") and source.raw.get("layer_ids"):
                # Check if the fc name starts with the sanitized source name
                if staging_fc_name.lower().startswith(sanitized_source_name.lower()):
                    log.info(
                        "🔍 ✅ Source matched by REST API pattern: '%s' (starts with '%s')",
                        source.name,
                        sanitized_source_name,
                    )
                    return source

            # Method 3: Check if the feature class name starts with the authority
            # Expected pattern: "{authority}_{source_name}_{geometry_type}"
            authority_prefix = source.authority.lower()
            if staging_fc_name.lower().startswith(f"{authority_prefix}_"):
                # Additional check: see if the source name appears after the authority
                remaining_name = staging_fc_name[len(authority_prefix) + 1:]
                if sanitized_source_name.replace("_", "").lower() in remaining_name.replace("_", "").lower():
                    log.info(
                        "🔍 ✅ Source matched by authority prefix: '%s' (authority: %s)",
                        source.name,
                        source.authority,
                    )
                    return source

            # Method 4: Check against shapefile names inside a source's items
            if source.staged_data_type == "shapefile_collection":
                # This logic assumes the staging_fc_name is derived from the shapefile's stem
                # e.g., 'TILLTRADESFORBUD.shp' -> 'tilltradesforbud'
                for item in source.include:
                    if sanitize_for_arcgis_name(item) in staging_fc_name:
                        log.info(
                            "🔍 ✅ Source matched by shapefile item: '%s' (item: %s)",
                            source.name,
                            item,
                        )
                        return source

        log.warning("⚠️ No source found for staging fc: %s", staging_fc_name)
        
        # Debug: Log all available sources for troubleshooting
        log.info("🔍 Available sources for debugging:")
        for source in self.sources:
            if source.enabled:
                sanitized = sanitize_for_arcgis_name(source.name)
                log.info(
                    "🔍   - %s (authority: %s, sanitized: %s, type: %s)",
                    source.name,
                    source.authority,
                    sanitized,
                    source.type,
                )
        
        return None

    def _map_to_sde(self, staging_fc_name: str) -> Optional[Tuple[str, str]]:
        """Map a staging feature class to its target SDE dataset and feature class name."""
        source = self._find_source_for_fc(staging_fc_name)
        if not source:
            log.warning("⚠️ Could not find a source for staging fc: %s", staging_fc_name)
            # Fallback: try to extract authority from the beginning of the fc name
            # This assumes the pattern is "{authority}_{rest_of_name}"
            parts = staging_fc_name.split("_", 1)
            if len(parts) >= 2 and len(parts[0]) <= 5:  # Authorities are typically short
                authority = parts[0].upper()
            else:
                authority = "UNKNOWN"
            dataset_name = self.sde_dataset_pattern.format(authority=authority)
            return dataset_name, sanitize_for_arcgis_name(staging_fc_name)

        # Use the source's authority to create the dataset name
        dataset_name = self.sde_dataset_pattern.format(authority=source.authority)

        # The final feature class name is the sanitized staging name
        final_fc_name = sanitize_for_arcgis_name(staging_fc_name)

        log.info(
            "🔧 SDE mapping: '%s' → dataset='%s', fc='%s' (source: %s)",
            staging_fc_name,
            dataset_name,
            final_fc_name,
            source.name,
        )

        return dataset_name, final_fc_name

    def load_to_sde(self, staging_gdb: Path):
        """Loads all feature classes from the staging GDB into the SDE."""
        # This is a placeholder for the method that would orchestrate the loading.
        # It would list feature classes in staging_gdb and call _map_to_sde for each.
        log.info("Starting SDE load from: %s", staging_gdb)
        # ... implementation needed here ...