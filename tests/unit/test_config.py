"""Unit tests for etl.config module."""
import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from etl.config import (
    GlobalConfig,
    LoggingConfig,
    RetryConfig,
    PathsConfig,
    ProcessingConfig,
    ValidationConfig,
    SecurityConfig,
    DatabaseConfig,
    SourceConfig,
    ConfigManager
)
from etl.exceptions import ConfigurationError, ValidationError


class TestLoggingConfig:
    """Test LoggingConfig validation."""

    @pytest.mark.unit
    def test_valid_logging_config(self):
        config = LoggingConfig(level="INFO", console_level="WARNING")
        assert config.level == "INFO"
        assert config.console_level == "WARNING"

    @pytest.mark.unit
    def test_invalid_log_level(self):
        with pytest.raises(ValidationError, match="Invalid log level"):
            LoggingConfig(level="INVALID")

    @pytest.mark.unit
    def test_invalid_console_level(self):
        with pytest.raises(ValidationError, match="Invalid console log level"):
            LoggingConfig(console_level="INVALID")


class TestRetryConfig:
    """Test RetryConfig validation."""

    @pytest.mark.unit
    def test_valid_retry_config(self):
        config = RetryConfig(max_attempts=5, base_delay=2.0)
        assert config.max_attempts == 5
        assert config.base_delay == 2.0

    @pytest.mark.unit
    def test_invalid_max_attempts(self):
        with pytest.raises(ValidationError, match="max_attempts must be at least 1"):
            RetryConfig(max_attempts=0)

    @pytest.mark.unit
    def test_invalid_base_delay(self):
        with pytest.raises(ValidationError, match="base_delay must be non-negative"):
            RetryConfig(base_delay=-1.0)

    @pytest.mark.unit
    def test_invalid_backoff_factor(self):
        with pytest.raises(ValidationError, match="backoff_factor must be at least 1"):
            RetryConfig(backoff_factor=0.5)

    @pytest.mark.unit
    def test_invalid_timeout(self):
        with pytest.raises(ValidationError, match="timeout must be at least 1 second"):
            RetryConfig(timeout=0)


class TestPathsConfig:
    """Test PathsConfig validation and normalization."""

    @pytest.mark.unit
    def test_paths_normalization(self):
        config = PathsConfig(download="./downloads", staging="../staging")
        # Paths should be converted to absolute paths
        assert Path(config.download).is_absolute()
        assert Path(config.staging).is_absolute()


class TestProcessingConfig:
    """Test ProcessingConfig validation."""

    @pytest.mark.unit
    def test_valid_processing_config(self):
        config = ProcessingConfig(chunk_size=500, parallel_workers=4)
        assert config.chunk_size == 500
        assert config.parallel_workers == 4

    @pytest.mark.unit
    def test_invalid_chunk_size(self):
        with pytest.raises(ValidationError, match="chunk_size must be at least 1"):
            ProcessingConfig(chunk_size=0)

    @pytest.mark.unit
    def test_invalid_parallel_workers(self):
        with pytest.raises(ValidationError, match="parallel_workers must be at least 1"):
            ProcessingConfig(parallel_workers=0)

    @pytest.mark.unit
    def test_invalid_memory_limit(self):
        with pytest.raises(ValidationError, match="memory_limit_mb must be at least 128MB"):
            ProcessingConfig(memory_limit_mb=64)


class TestValidationConfig:
    """Test ValidationConfig validation."""

    @pytest.mark.unit
    def test_valid_validation_config(self):
        config = ValidationConfig(strict_mode=True, max_validation_errors=50)
        assert config.strict_mode is True
        assert config.max_validation_errors == 50

    @pytest.mark.unit
    def test_invalid_max_validation_errors(self):
        with pytest.raises(ValidationError, match="max_validation_errors must be at least 1"):
            ValidationConfig(max_validation_errors=0)


class TestSecurityConfig:
    """Test SecurityConfig validation."""

    @pytest.mark.unit
    def test_valid_security_config(self):
        config = SecurityConfig(
            enable_ssl_verification=True,
            trusted_hosts=["example.com"],
            max_file_size_mb=512
        )
        assert config.enable_ssl_verification is True
        assert "example.com" in config.trusted_hosts
        assert config.max_file_size_mb == 512

    @pytest.mark.unit
    def test_invalid_max_file_size(self):
        with pytest.raises(ValidationError, match="max_file_size_mb must be at least 1MB"):
            SecurityConfig(max_file_size_mb=0)


class TestDatabaseConfig:
    """Test DatabaseConfig validation."""

    @pytest.mark.unit
    def test_valid_database_config(self):
        config = DatabaseConfig(pool_size=10, max_overflow=5)
        assert config.pool_size == 10
        assert config.max_overflow == 5

    @pytest.mark.unit
    def test_invalid_pool_size(self):
        with pytest.raises(ValidationError, match="pool_size must be at least 1"):
            DatabaseConfig(pool_size=0)

    @pytest.mark.unit
    def test_invalid_max_overflow(self):
        with pytest.raises(ValidationError, match="max_overflow must be non-negative"):
            DatabaseConfig(max_overflow=-1)


class TestGlobalConfig:
    """Test GlobalConfig validation."""

    @pytest.mark.unit
    def test_valid_global_config(self):
        config = GlobalConfig(environment="production", debug=False)
        assert config.environment == "production"
        assert config.debug is False

    @pytest.mark.unit
    def test_invalid_environment(self):
        with pytest.raises(ValidationError, match="Invalid environment"):
            GlobalConfig(environment="invalid")


class TestSourceConfig:
    """Test SourceConfig validation."""

    @pytest.mark.unit
    def test_valid_source_config(self):
        config = SourceConfig(
            name="Test Source",
            authority="TEST",
            type="rest_api",
            url="https://example.com/api",
            enabled=True
        )
        assert config.name == "Test Source"
        assert config.authority == "TEST"
        assert config.type == "rest_api"
        assert config.enabled is True

    @pytest.mark.unit
    def test_empty_name(self):
        with pytest.raises(ValidationError, match="Source name cannot be empty"):
            SourceConfig(name="", authority="TEST")

    @pytest.mark.unit
    def test_empty_authority(self):
        with pytest.raises(ValidationError, match="Source authority cannot be empty"):
            SourceConfig(name="Test", authority="")

    @pytest.mark.unit
    def test_invalid_type(self):
        with pytest.raises(ValidationError, match="Invalid source type"):
            SourceConfig(name="Test", authority="TEST", type="invalid_type")

    @pytest.mark.unit
    def test_enabled_source_without_url(self):
        with pytest.raises(ValidationError, match="Enabled source .* must have a URL"):
            SourceConfig(name="Test", authority="TEST", enabled=True, url="")

    @pytest.mark.unit
    def test_invalid_priority(self):
        with pytest.raises(ValidationError, match="Source priority must be between 1 and 100"):
            SourceConfig(name="Test", authority="TEST", priority=0)

    @pytest.mark.unit
    def test_invalid_timeout(self):
        with pytest.raises(ValidationError, match="Source timeout must be at least 1 second"):
            SourceConfig(name="Test", authority="TEST", timeout=0)

    @pytest.mark.unit
    def test_invalid_retry_attempts(self):
        with pytest.raises(ValidationError, match="Source retry_attempts must be non-negative"):
            SourceConfig(name="Test", authority="TEST", retry_attempts=-1)


class TestConfigManager:
    """Test ConfigManager functionality."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create temporary directory with config files."""
        temp_dir = Path(tempfile.mkdtemp())

        # Create config directory structure
        config_dir = temp_dir / "config"
        config_dir.mkdir()

        # Create basic config file
        config_file = config_dir / "config.yaml"
        config_content = """
environment: "development"
logging:
  level: "DEBUG"
retry:
  max_attempts: 2
"""
        config_file.write_text(config_content)

        # Create sources file
        sources_file = config_dir / "sources.yaml"
        sources_content = """
sources:
  - name: "Test Source"
    authority: "TEST"
    type: "rest_api"
    url: "https://example.com"
    enabled: true
"""
        sources_file.write_text(sources_content)

        yield temp_dir

        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.unit
    def test_config_manager_initialization(self):
        manager = ConfigManager(environment="development")
        assert manager.environment == "development"

    @pytest.mark.unit
    def test_environment_from_env_var(self):
        with patch.dict(os.environ, {"ETL_ENVIRONMENT": "production"}):
            manager = ConfigManager()
            assert manager.environment == "production"

    @pytest.mark.unit
    def test_load_global_config(self, temp_config_dir):
        config_file = temp_config_dir / "config" / "config.yaml"
        manager = ConfigManager()

        config = manager.load_global_config(config_file)
        assert isinstance(config, GlobalConfig)
        assert config.environment == "development"
        assert config.logging.level == "DEBUG"
        assert config.retry.max_attempts == 2

    @pytest.mark.unit
    def test_load_sources_config(self, temp_config_dir):
        sources_file = temp_config_dir / "config" / "sources.yaml"
        manager = ConfigManager()

        sources = manager.load_sources_config(sources_file)
        assert len(sources) == 1
        assert isinstance(sources[0], SourceConfig)
        assert sources[0].name == "Test Source"
        assert sources[0].authority == "TEST"

    @pytest.mark.unit
    def test_load_nonexistent_config(self):
        manager = ConfigManager()

        with pytest.raises(ConfigurationError, match="Configuration file .* not found"):
            manager.load_global_config(Path("nonexistent.yaml"))

    @pytest.mark.unit
    def test_load_invalid_yaml(self, temp_config_dir):
        invalid_file = temp_config_dir / "invalid.yaml"
        invalid_file.write_text("invalid: yaml: content: [")

        manager = ConfigManager()

        with pytest.raises(ConfigurationError, match="Invalid YAML"):
            manager.load_global_config(invalid_file)

    @pytest.mark.unit
    def test_sources_missing_sources_key(self, temp_config_dir):
        invalid_sources = temp_config_dir / "invalid_sources.yaml"
        invalid_sources.write_text("other_key: value")

        manager = ConfigManager()

        with pytest.raises(ConfigurationError, match="missing 'sources' key"):
            manager.load_sources_config(invalid_sources)

    @pytest.mark.unit
    def test_validate_configuration_warnings(self, temp_config_dir):
        config_file = temp_config_dir / "config" / "config.yaml"
        manager = ConfigManager()

        config = manager.load_global_config(config_file)
        warnings = manager.validate_configuration(config)

        # Should return a list (may be empty)
        assert isinstance(warnings, list)

    @pytest.mark.unit
    def test_environment_variable_overrides(self, temp_config_dir):
        config_file = temp_config_dir / "config" / "config.yaml"
        manager = ConfigManager()

        with patch.dict(os.environ, {
            "ETL_LOG_LEVEL": "ERROR",
            "ETL_DEBUG": "true",
            "ETL_MAX_WORKERS": "8"
        }):
            config = manager.load_global_config(config_file)

            assert config.logging.level == "ERROR"
            assert config.debug is True
            assert config.processing.parallel_workers == 8
