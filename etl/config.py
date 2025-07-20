"""Configuration management and validation for ETL pipeline.

This module provides structured configuration management using dataclasses
instead of Pydantic (to maintain ArcGIS compatibility). Includes validation,
environment-specific configuration, and schema checking.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Type, get_type_hints
import yaml

from .exceptions import ConfigurationError, ValidationError

log = logging.getLogger(__name__)


@dataclass
class LoggingConfig:
    """Configuration for logging settings."""
    level: str = "INFO"
    summary_file: str = "etl_summary.log"
    debug_file: str = "etl_debug.log"
    console_level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    max_file_size_mb: int = 10
    backup_count: int = 5
    
    def __post_init__(self):
        """Validate logging configuration."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            raise ValidationError(f"Invalid log level: {self.level}. Must be one of {valid_levels}")
        if self.console_level.upper() not in valid_levels:
            raise ValidationError(f"Invalid console log level: {self.console_level}. Must be one of {valid_levels}")


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 300.0
    timeout: int = 30
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    
    def __post_init__(self):
        """Validate retry configuration."""
        if self.max_attempts < 1:
            raise ValidationError("max_attempts must be at least 1")
        if self.base_delay < 0:
            raise ValidationError("base_delay must be non-negative")
        if self.backoff_factor < 1:
            raise ValidationError("backoff_factor must be at least 1")
        if self.timeout < 1:
            raise ValidationError("timeout must be at least 1 second")


@dataclass
class PathsConfig:
    """Configuration for file paths."""
    download: str = "downloads"
    staging: str = "staging"
    output: str = "output"
    temp: str = "temp"
    logs: str = "logs"
    
    def __post_init__(self):
        """Validate and normalize paths."""
        # Convert relative paths to absolute
        for field_name in ["download", "staging", "output", "temp", "logs"]:
            path_value = getattr(self, field_name)
            normalized_path = str(Path(path_value).resolve())
            setattr(self, field_name, normalized_path)


@dataclass
class ProcessingConfig:
    """Configuration for data processing."""
    chunk_size: int = 1000
    parallel_workers: int = 2
    memory_limit_mb: int = 1024
    enable_caching: bool = True
    cache_ttl_hours: int = 24
    
    def __post_init__(self):
        """Validate processing configuration."""
        if self.chunk_size < 1:
            raise ValidationError("chunk_size must be at least 1")
        if self.parallel_workers < 1:
            raise ValidationError("parallel_workers must be at least 1")
        if self.memory_limit_mb < 128:
            raise ValidationError("memory_limit_mb must be at least 128MB")


@dataclass
class ValidationConfig:
    """Configuration for data validation."""
    strict_mode: bool = False
    schema_validation: bool = True
    geometry_validation: bool = True
    attribute_validation: bool = True
    coordinate_system_validation: bool = True
    max_validation_errors: int = 100
    
    def __post_init__(self):
        """Validate validation configuration."""
        if self.max_validation_errors < 1:
            raise ValidationError("max_validation_errors must be at least 1")


@dataclass
class SecurityConfig:
    """Configuration for security settings."""
    enable_ssl_verification: bool = True
    trusted_hosts: List[str] = field(default_factory=list)
    max_file_size_mb: int = 1024
    allowed_file_types: List[str] = field(default_factory=lambda: [
        ".zip", ".gpkg", ".shp", ".geojson", ".json", ".gdb"
    ])
    
    def __post_init__(self):
        """Validate security configuration."""
        if self.max_file_size_mb < 1:
            raise ValidationError("max_file_size_mb must be at least 1MB")


@dataclass 
class DatabaseConfig:
    """Configuration for database connections."""
    connection_string: Optional[str] = None
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    echo_sql: bool = False
    
    def __post_init__(self):
        """Validate database configuration."""
        if self.pool_size < 1:
            raise ValidationError("pool_size must be at least 1")
        if self.max_overflow < 0:
            raise ValidationError("max_overflow must be non-negative")


@dataclass
class GlobalConfig:
    """Main configuration container for the ETL pipeline."""
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    # Environment-specific settings
    environment: str = "development"
    debug: bool = False
    
    def __post_init__(self):
        """Validate global configuration."""
        valid_environments = ["development", "staging", "production"]
        if self.environment not in valid_environments:
            raise ValidationError(f"Invalid environment: {self.environment}. Must be one of {valid_environments}")


@dataclass
class SourceConfig:
    """Enhanced source configuration with validation."""
    name: str
    authority: str
    type: str = "file"
    url: str = ""
    enabled: bool = True
    download_format: Optional[str] = None
    staged_data_type: Optional[str] = None
    include: List[str] = field(default_factory=list)
    
    # Additional validation fields
    timeout: Optional[int] = None
    retry_attempts: Optional[int] = None
    priority: int = 10
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate source configuration."""
        if not self.name.strip():
            raise ValidationError("Source name cannot be empty")
        if not self.authority.strip():
            raise ValidationError("Source authority cannot be empty")
        
        valid_types = ["file", "rest_api", "ogc_api", "atom_feed", "database"]
        if self.type not in valid_types:
            raise ValidationError(f"Invalid source type: {self.type}. Must be one of {valid_types}")
        
        if self.enabled and not self.url.strip():
            raise ValidationError(f"Enabled source '{self.name}' must have a URL")
        
        if self.priority < 1 or self.priority > 100:
            raise ValidationError("Source priority must be between 1 and 100")
        
        if self.timeout is not None and self.timeout < 1:
            raise ValidationError("Source timeout must be at least 1 second")
        
        if self.retry_attempts is not None and self.retry_attempts < 0:
            raise ValidationError("Source retry_attempts must be non-negative")


class ConfigManager:
    """Manages configuration loading, validation, and environment-specific settings."""
    
    def __init__(self, environment: Optional[str] = None):
        self.environment = environment or os.getenv("ETL_ENVIRONMENT", "development")
        self._config_cache: Dict[str, Any] = {}
    
    def load_global_config(self, config_path: Optional[Path] = None) -> GlobalConfig:
        """Load and validate global configuration."""
        if config_path is None:
            config_path = self._find_config_file("config.yaml")
        
        try:
            config_dict = self._load_yaml_file(config_path)
            
            # Apply environment-specific overrides
            config_dict = self._apply_environment_overrides(config_dict)
            
            # Create configuration object with validation
            return self._create_global_config(config_dict)
            
        except Exception as e:
            if isinstance(e, (ConfigurationError, ValidationError)):
                raise
            raise ConfigurationError(
                f"Failed to load configuration from {config_path}: {e}",
                config_file=str(config_path)
            ) from e
    
    def load_sources_config(self, sources_path: Optional[Path] = None) -> List[SourceConfig]:
        """Load and validate sources configuration."""
        if sources_path is None:
            sources_path = self._find_config_file("sources.yaml")
        
        try:
            sources_dict = self._load_yaml_file(sources_path)
            
            if "sources" not in sources_dict:
                raise ConfigurationError(
                    f"Configuration file {sources_path} missing 'sources' key",
                    config_file=str(sources_path)
                )
            
            sources = []
            for i, source_data in enumerate(sources_dict["sources"]):
                try:
                    source_config = self._create_source_config(source_data)
                    sources.append(source_config)
                except ValidationError as e:
                    raise ValidationError(
                        f"Source {i + 1} validation failed: {e}",
                        field_name=f"sources[{i}]"
                    ) from e
            
            log.info("✅ Loaded %d source configurations", len(sources))
            return sources
            
        except Exception as e:
            if isinstance(e, (ConfigurationError, ValidationError)):
                raise
            raise ConfigurationError(
                f"Failed to load sources from {sources_path}: {e}",
                config_file=str(sources_path)
            ) from e
    
    def validate_configuration(self, config: GlobalConfig) -> List[str]:
        """Validate configuration and return list of warnings."""
        warnings = []
        
        # Check path accessibility
        for path_name in ["download", "staging", "output", "temp", "logs"]:
            path_value = getattr(config.paths, path_name)
            path_obj = Path(path_value)
            
            if not path_obj.parent.exists():
                warnings.append(f"Parent directory for {path_name} path does not exist: {path_obj.parent}")
        
        # Check memory settings
        if config.processing.memory_limit_mb > 8192:  # 8GB
            warnings.append("Memory limit is set very high (>8GB). Consider reducing for better stability.")
        
        # Check retry settings
        if config.retry.max_attempts > 10:
            warnings.append("Max retry attempts is very high (>10). This may cause long delays.")
        
        # Environment-specific warnings
        if config.environment == "production":
            if config.debug:
                warnings.append("Debug mode is enabled in production environment")
            if config.logging.level == "DEBUG":
                warnings.append("Debug logging is enabled in production environment")
        
        return warnings
    
    def _find_config_file(self, filename: str) -> Path:
        """Find configuration file in standard locations."""
        """Configuration management and validation for ETL pipeline.

        This module provides structured configuration management using dataclasses
        instead of Pydantic (to maintain ArcGIS compatibility). Includes validation,
        environment-specific configuration, and schema checking.
        """
        from __future__ import annotations

        import logging
        import os
        import platform
        from dataclasses import dataclass, field, fields
        from pathlib import Path
        from typing import Any, Dict, List, Optional, Union, Type, get_type_hints
        import yaml

        from .exceptions import ConfigurationError, ValidationError

        log = logging.getLogger(__name__)

        # ... rest of the code remains unchanged ...

            def _find_config_file(self, filename: str) -> Path:
                """Find configuration file in standard locations."""
                search_paths = [
                    Path.cwd() / "config" / filename,
                    Path.cwd() / filename,
                    Path.home() / ".etl" / filename,
                ]
                
                # Add Unix-specific path only on non-Windows systems
                if platform.system() != "Windows":
                    search_paths.append(Path("/etc/etl") / filename)
                
                for path in search_paths:
                    if path.exists():
                        return path
                
                raise ConfigurationError(f"Configuration file '{filename}' not found in any of: {search_paths}")
        search_paths = [
            Path.cwd() / "config" / filename,
            Path.cwd() / filename,
            Path.home() / ".etl" / filename,
        ]
        
        # Add Unix-specific path only on non-Windows systems
        if platform.system() != "Windows":
            search_paths.append(Path("/etc/etl") / filename)
        
        for path in search_paths:
            if path.exists():
                return path
        
        raise ConfigurationError(f"Configuration file '{filename}' not found in any of: {search_paths}")
    
    def _load_yaml_file(self, path: Path) -> Dict[str, Any]:
        """Load YAML file with error handling."""
        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {path}")
        
        try:
            with path.open("r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
            
            if content is None:
                return {}
            
            if not isinstance(content, dict):
                raise ConfigurationError(f"Configuration file must contain a YAML dictionary: {path}")
            
            return content
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}") from e
    
    def _apply_environment_overrides(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment-specific configuration overrides."""
        # Look for environment-specific section
        env_section = f"environments.{self.environment}"
        if env_section in config_dict:
            env_overrides = config_dict[env_section]
            config_dict = self._deep_merge(config_dict, env_overrides)
        
        # Apply environment variables
        config_dict = self._apply_environment_variables(config_dict)
        
        # Set environment in config
        config_dict["environment"] = self.environment
        
        return config_dict
    
    def _apply_environment_variables(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variable overrides."""
        env_mappings = {
            "ETL_LOG_LEVEL": ("logging", "level"),
            "ETL_DEBUG": ("debug",),
            "ETL_DOWNLOAD_PATH": ("paths", "download"),
            "ETL_STAGING_PATH": ("paths", "staging"),
            "ETL_OUTPUT_PATH": ("paths", "output"),
            "ETL_DB_CONNECTION": ("database", "connection_string"),
            "ETL_MAX_WORKERS": ("processing", "parallel_workers"),
            "ETL_MEMORY_LIMIT": ("processing", "memory_limit_mb"),
        }
        
        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Navigate to the config section
                current = config_dict
                for key in config_path[:-1]:
                    current = current.setdefault(key, {})
                
                # Set the value with appropriate type conversion
                key = config_path[-1]
                if key in ["debug"]:
                    current[key] = env_value.lower() in ("true", "1", "yes", "on")
                elif key in ["parallel_workers", "memory_limit_mb"]:
                    current[key] = int(env_value)
                else:
                    current[key] = env_value
        
        return config_dict
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _create_global_config(self, config_dict: Dict[str, Any]) -> GlobalConfig:
        """Create GlobalConfig from dictionary with validation."""
        # Create nested config objects
        config_sections = {}
        
        if "logging" in config_dict:
            config_sections["logging"] = self._create_dataclass_from_dict(
                LoggingConfig, config_dict["logging"]
            )
        
        if "retry" in config_dict:
            config_sections["retry"] = self._create_dataclass_from_dict(
                RetryConfig, config_dict["retry"]
            )
        
        if "paths" in config_dict:
            config_sections["paths"] = self._create_dataclass_from_dict(
                PathsConfig, config_dict["paths"]
            )
        
        if "processing" in config_dict:
            config_sections["processing"] = self._create_dataclass_from_dict(
                ProcessingConfig, config_dict["processing"]
            )
        
        if "validation" in config_dict:
            config_sections["validation"] = self._create_dataclass_from_dict(
                ValidationConfig, config_dict["validation"]
            )
        
        if "security" in config_dict:
            config_sections["security"] = self._create_dataclass_from_dict(
                SecurityConfig, config_dict["security"]
            )
        
        if "database" in config_dict:
            config_sections["database"] = self._create_dataclass_from_dict(
                DatabaseConfig, config_dict["database"]
            )
        
        # Add top-level fields
        if "environment" in config_dict:
            config_sections["environment"] = config_dict["environment"]
        if "debug" in config_dict:
            config_sections["debug"] = config_dict["debug"]
        
        return GlobalConfig(**config_sections)
    
    def _create_source_config(self, source_dict: Dict[str, Any]) -> SourceConfig:
        """Create SourceConfig from dictionary with validation."""
        return self._create_dataclass_from_dict(SourceConfig, source_dict)
    
    def _create_dataclass_from_dict(self, cls: Type, data: Dict[str, Any]):
        """Create dataclass instance from dictionary, handling type conversion."""
        field_types = get_type_hints(cls)
        kwargs = {}
        
        for field_info in fields(cls):
            field_name = field_info.name
            if field_name in data:
                field_type = field_types.get(field_name)
                value = data[field_name]
                
                # Handle type conversion
                if field_type and hasattr(field_type, '__origin__'):
                    # Handle List types
                    if field_type.__origin__ is list:
                        if not isinstance(value, list):
                            if isinstance(value, str):
                                # Split string into list
                                value = [item.strip() for item in value.split(',') if item.strip()]
                            else:
                                value = [value]
                
                kwargs[field_name] = value
        
        return cls(**kwargs)