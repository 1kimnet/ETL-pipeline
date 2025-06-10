"""Unit tests for etl.exceptions module."""
import pytest

from etl.exceptions import (
    ETLError,
    ConfigurationError,
    ValidationError,
    SourceError,
    SourceUnavailableError,
    SourceNotFoundError,
    AuthenticationError,
    RateLimitError,
    NetworkError,
    ConnectionError,
    TimeoutError,
    HTTPError,
    DataError,
    DataFormatError,
    DataQualityError,
    TransformationError,
    GeospatialError,
    StorageError,
    DiskSpaceError,
    PermissionError,
    FileNotFoundError,
    DatabaseError,
    ConnectionPoolError,
    SchemaError,
    LoadError,
    ArcGISError,
    GeoprocessingError,
    LicenseError,
    WorkspaceError,
    PipelineError,
    DependencyError,
    ResourceError,
    CircuitBreakerError,
    is_recoverable_error,
    get_retry_delay,
    format_error_context
)


class TestETLError:
    """Test base ETLError functionality."""
    
    @pytest.mark.unit
    def test_basic_etl_error(self):
        error = ETLError("Test error message")
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.source_name is None
        assert error.context == {}
        assert error.recoverable is True
        assert error.retry_after is None
    
    @pytest.mark.unit
    def test_etl_error_with_context(self):
        error = ETLError(
            "Test error",
            source_name="test_source",
            context={"key": "value"},
            recoverable=False,
            retry_after=60
        )
        assert error.source_name == "test_source"
        assert error.context["key"] == "value"
        assert error.recoverable is False
        assert error.retry_after == 60
    
    @pytest.mark.unit
    def test_etl_error_string_representation(self):
        error = ETLError("Test error", source_name="test_source")
        assert str(error) == "Test error (source: test_source)"


class TestConfigurationErrors:
    """Test configuration-related errors."""
    
    @pytest.mark.unit
    def test_configuration_error(self):
        error = ConfigurationError("Invalid config", config_file="config.yaml")
        assert error.config_file == "config.yaml"
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_validation_error(self):
        error = ValidationError("Invalid field", field_name="test_field")
        assert error.field_name == "test_field"


class TestSourceErrors:
    """Test source-related errors."""
    
    @pytest.mark.unit
    def test_source_unavailable_error(self):
        error = SourceUnavailableError("Service temporarily down")
        assert error.retry_after == 300  # Default 5 minutes
        assert error.recoverable is True
    
    @pytest.mark.unit
    def test_source_not_found_error(self):
        error = SourceNotFoundError("Source does not exist")
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_authentication_error(self):
        error = AuthenticationError("Invalid credentials")
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_rate_limit_error(self):
        error = RateLimitError("Rate limit exceeded", retry_after=120)
        assert error.retry_after == 120
        assert error.recoverable is True
    
    @pytest.mark.unit
    def test_rate_limit_error_default_retry(self):
        error = RateLimitError("Rate limit exceeded")
        assert error.retry_after == 60  # Default


class TestNetworkErrors:
    """Test network-related errors."""
    
    @pytest.mark.unit
    def test_network_error(self):
        error = NetworkError("Network issue")
        assert error.retry_after == 30  # Default
        assert error.recoverable is True
    
    @pytest.mark.unit
    def test_connection_error(self):
        error = ConnectionError("Connection failed")
        assert isinstance(error, NetworkError)
    
    @pytest.mark.unit
    def test_timeout_error(self):
        error = TimeoutError("Request timed out")
        assert isinstance(error, NetworkError)
    
    @pytest.mark.unit
    def test_http_error_recoverable(self):
        error = HTTPError("Server error", status_code=500)
        assert error.status_code == 500
        assert error.recoverable is True  # 5xx errors are recoverable
    
    @pytest.mark.unit
    def test_http_error_non_recoverable(self):
        error = HTTPError("Bad request", status_code=400)
        assert error.status_code == 400
        assert error.recoverable is False  # 4xx errors (except 429) are not recoverable
    
    @pytest.mark.unit
    def test_http_error_rate_limit_recoverable(self):
        error = HTTPError("Too many requests", status_code=429)
        assert error.status_code == 429
        assert error.recoverable is True  # 429 is recoverable


class TestDataErrors:
    """Test data processing errors."""
    
    @pytest.mark.unit
    def test_data_format_error(self):
        error = DataFormatError("Invalid JSON", format_type="json")
        assert error.format_type == "json"
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_data_quality_error(self):
        error = DataQualityError("Invalid geometry", quality_check="geometry_validation")
        assert error.quality_check == "geometry_validation"
    
    @pytest.mark.unit
    def test_transformation_error(self):
        error = TransformationError("Projection failed")
        assert isinstance(error, DataError)
    
    @pytest.mark.unit
    def test_geospatial_error(self):
        error = GeospatialError("Buffer operation failed", operation="buffer")
        assert error.operation == "buffer"


class TestStorageErrors:
    """Test storage-related errors."""
    
    @pytest.mark.unit
    def test_disk_space_error(self):
        error = DiskSpaceError("Insufficient disk space")
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_permission_error(self):
        error = PermissionError("Access denied")
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_file_not_found_error(self):
        error = FileNotFoundError("File missing", file_path="/path/to/file")
        assert error.file_path == "/path/to/file"
        assert error.recoverable is False


class TestDatabaseErrors:
    """Test database-related errors."""
    
    @pytest.mark.unit
    def test_connection_pool_error(self):
        error = ConnectionPoolError("Pool exhausted")
        assert error.retry_after == 60
        assert error.recoverable is True
    
    @pytest.mark.unit
    def test_schema_error(self):
        error = SchemaError("Invalid schema")
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_load_error(self):
        error = LoadError("Load failed")
        assert isinstance(error, DatabaseError)


class TestArcGISErrors:
    """Test ArcGIS-related errors."""
    
    @pytest.mark.unit
    def test_geoprocessing_error(self):
        error = GeoprocessingError("Tool failed", tool_name="Buffer")
        assert error.tool_name == "Buffer"
    
    @pytest.mark.unit
    def test_license_error(self):
        error = LicenseError("License unavailable")
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_workspace_error(self):
        error = WorkspaceError("Cannot access workspace")
        assert isinstance(error, ArcGISError)


class TestPipelineErrors:
    """Test pipeline workflow errors."""
    
    @pytest.mark.unit
    def test_dependency_error(self):
        error = DependencyError("Missing dependency", dependency="arcpy")
        assert error.dependency == "arcpy"
        assert error.recoverable is False
    
    @pytest.mark.unit
    def test_resource_error(self):
        error = ResourceError("Insufficient memory", resource_type="memory")
        assert error.resource_type == "memory"
    
    @pytest.mark.unit
    def test_circuit_breaker_error(self):
        error = CircuitBreakerError("Circuit breaker open", service_name="api_service")
        assert error.service_name == "api_service"
        assert error.retry_after == 300


class TestUtilityFunctions:
    """Test utility functions for error handling."""
    
    @pytest.mark.unit
    def test_is_recoverable_error_with_etl_error(self):
        recoverable_error = SourceUnavailableError("Temporary issue")
        non_recoverable_error = SourceNotFoundError("Not found")
        
        assert is_recoverable_error(recoverable_error) is True
        assert is_recoverable_error(non_recoverable_error) is False
    
    @pytest.mark.unit
    def test_is_recoverable_error_with_standard_exceptions(self):
        assert is_recoverable_error(ConnectionRefusedError()) is True
        assert is_recoverable_error(TimeoutError()) is True
        assert is_recoverable_error(ValueError()) is False
    
    @pytest.mark.unit
    def test_get_retry_delay(self):
        error_with_delay = RateLimitError("Rate limited", retry_after=120)
        error_without_delay = ValueError("Standard error")
        
        assert get_retry_delay(error_with_delay) == 120
        assert get_retry_delay(error_without_delay) is None
    
    @pytest.mark.unit
    def test_format_error_context(self):
        error = ETLError(
            "Test error",
            source_name="test_source",
            context={"url": "https://example.com", "status": 500},
            recoverable=False
        )
        
        formatted = format_error_context(error)
        assert "Test error (source: test_source)" in formatted
        assert "url=https://example.com" in formatted
        assert "status=500" in formatted
        assert "(non-recoverable)" in formatted
    
    @pytest.mark.unit
    def test_format_error_context_with_retry_after(self):
        error = ETLError("Test error", retry_after=60)
        formatted = format_error_context(error)
        assert "(retry after 60s)" in formatted