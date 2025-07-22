"""Enhanced exception hierarchy for ETL pipeline operations.

This module provides a streamlined set of 8 core exceptions with enhanced
error classification, recovery hints, and structured logging support.
"""
from __future__ import annotations

# Import the new core exception system
from .exceptions.core import (
    ETLError,
    NetworkError,
    DataError,
    SystemError,
    ConfigurationError,
    SourceError,
    ProcessingError,
    PipelineError,
    ConcurrentError,
    ErrorContext,
    ErrorSeverity,
    ErrorCategory,
    classify_exception,
    is_recoverable_error,
    get_retry_delay,
    format_error_for_logging
)

# Import backwards compatibility layer
from .exceptions.compat import (
    # Legacy exception names (mapped to new system)
    HTTPError,
    ConnectionError,
    TimeoutError,
    RateLimitError,
    DataFormatError,
    DataQualityError,
    ValidationError,
    GeospatialError,
    TransformationError,
    StorageError,
    ResourceError,
    PermissionError,
    DiskSpaceError,
    FileNotFoundError,
    SourceUnavailableError,
    SourceNotFoundError,
    AuthenticationError,
    GeoprocessingError,
    LoadError,
    DependencyError,
    CircuitBreakerError,
    ArcGISError,
    LicenseError,
    WorkspaceError,
    DatabaseError,
    ConnectionPoolError,
    SchemaError,
    # Legacy utility functions
    format_error_context,
    create_http_error,
    create_rate_limit_error,
    create_data_format_error,
    create_source_unavailable_error,
    create_configuration_error
)


# All exception classes are now imported from the core and compat modules above
# The new system provides 8 core exception types with enhanced functionality:
#
# 1. NetworkError - HTTP, connection, timeout, rate limit issues
# 2. DataError - Data format, quality, validation, geospatial issues
# 3. SystemError - Storage, resources, permissions, disk space
# 4. ConfigurationError - Configuration and setup problems
# 5. SourceError - Source availability, authentication, access
# 6. ProcessingError - Transformation, geoprocessing, loading
# 7. PipelineError - Pipeline dependencies, circuit breakers
# 8. ConcurrentError - Concurrent operation failures
#
# Legacy exception names are preserved for backwards compatibility
# All exceptions now have enhanced error context and structured logging support
