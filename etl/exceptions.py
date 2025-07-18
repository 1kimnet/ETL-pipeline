"""Enhanced exception hierarchy for ETL pipeline operations.

This module provides a streamlined set of 8 core exceptions with enhanced
error classification, recovery hints, and structured logging support.
"""

from __future__ import annotations  # pylint: disable=unused-import

# Import the new core exception system
from .exceptions.core import (
    ConfigurationError,
    DataError,
    NetworkError,
    PipelineError,
    ProcessingError,
    SourceError,
)
from .exceptions.core import SystemError as ETLSystemError
from .exceptions.core import format_error_for_logging

# Define compatibility aliases to avoid conflicts with built-in names
# Legacy exception names mapped to the new core exception system
HTTPError = NetworkError
ETLConnectionError = NetworkError  # Renamed to avoid conflict with built-in
ETLTimeoutError = NetworkError  # Renamed to avoid conflict with built-in
RateLimitError = NetworkError
DataFormatError = DataError
DataQualityError = DataError
ValidationError = DataError
GeospatialError = DataError
TransformationError = ProcessingError
StorageError = ETLSystemError
ResourceError = ETLSystemError
ETLPermissionError = ETLSystemError  # Renamed to avoid conflict with built-in
DiskSpaceError = ETLSystemError
ETLFileNotFoundError = ETLSystemError  # Renamed to avoid conflict with built-in
SourceUnavailableError = SourceError
SourceNotFoundError = SourceError
AuthenticationError = SourceError
GeoprocessingError = ProcessingError
LoadError = ProcessingError
DependencyError = PipelineError
CircuitBreakerError = PipelineError
ArcGISError = ProcessingError
LicenseError = ConfigurationError
WorkspaceError = ETLSystemError
DatabaseError = DataError
ConnectionPoolError = NetworkError
SchemaError = DataError

# Legacy utility functions mapped to new core functionality
format_error_context = format_error_for_logging


def create_http_error(*args, **kwargs):
    """Create HTTP error with legacy interface."""
    return NetworkError(*args, **kwargs)


def create_rate_limit_error(*args, **kwargs):
    """Create rate limit error with legacy interface."""
    return NetworkError(*args, **kwargs)


def create_data_format_error(*args, **kwargs):
    """Create data format error with legacy interface."""
    return DataError(*args, **kwargs)


def create_source_unavailable_error(*args, **kwargs):
    """Create source unavailable error with legacy interface."""
    return SourceError(*args, **kwargs)


def create_configuration_error(*args, **kwargs):
    """Create configuration error with legacy interface."""
    return ConfigurationError(*args, **kwargs)


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
