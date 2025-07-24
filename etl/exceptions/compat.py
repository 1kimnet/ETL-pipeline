"""Backwards compatibility for existing exception usage."""
from __future__ import annotations

from typing import Optional, Dict, Any

from .core import (
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
    ErrorCategory
)

# Legacy exceptions mapped to new core exceptions
# This allows existing code to continue working while using the new system

# Network errors
HTTPError = NetworkError
ConnectionError = NetworkError
TimeoutError = NetworkError
RateLimitError = NetworkError

# Data errors
DataFormatError = DataError
DataQualityError = DataError
ValidationError = DataError
GeospatialError = DataError
TransformationError = DataError

# System errors
StorageError = SystemError
ResourceError = SystemError
PermissionError = SystemError
DiskSpaceError = SystemError
FileNotFoundError = SystemError

# Source errors
SourceUnavailableError = SourceError
SourceNotFoundError = SourceError
AuthenticationError = SourceError

# Processing errors
GeoprocessingError = ProcessingError
LoadError = ProcessingError

# Pipeline errors
DependencyError = PipelineError
CircuitBreakerError = PipelineError

# ArcGIS errors
ArcGISError = ProcessingError
LicenseError = ProcessingError
WorkspaceError = ProcessingError

# Database errors
DatabaseError = SystemError
ConnectionPoolError = SystemError
SchemaError = ConfigurationError


# Legacy utility functions
def format_error_context(error: ETLError) -> str:
    """Legacy function for formatting error context."""
    if isinstance(error, ETLError):
        return str(error)
    return str(error)


# Helper functions for creating legacy-style errors
def create_http_error(
        message: str,
        status_code: Optional[int] = None,
        **kwargs) -> NetworkError:
    """Create HTTP error with legacy interface."""
    return NetworkError(
        message,
        status_code=status_code,
        context=ErrorContext(
            source_name=kwargs.get('source_name'),
            url=kwargs.get('url'),
            operation='http_request'
        )
    )


def create_rate_limit_error(
        message: str,
        retry_after: Optional[int] = None,
        **kwargs) -> NetworkError:
    """Create rate limit error with legacy interface."""
    return NetworkError(
        message,
        status_code=429,
        context=ErrorContext(
            source_name=kwargs.get('source_name'),
            url=kwargs.get('url'),
            operation='http_request'
        )
    )


def create_data_format_error(
        message: str,
        format_type: Optional[str] = None,
        **kwargs) -> DataError:
    """Create data format error with legacy interface."""
    return DataError(
        message,
        data_type=format_type,
        context=ErrorContext(
            source_name=kwargs.get('source_name'),
            file_path=kwargs.get('file_path'),
            operation='data_parsing'
        )
    )


def create_source_unavailable_error(message: str, **kwargs) -> SourceError:
    """Create source unavailable error with legacy interface."""
    return SourceError(
        message,
        available=False,
        context=ErrorContext(
            source_name=kwargs.get('source_name'),
            url=kwargs.get('url'),
            operation='source_access'
        )
    )


def create_configuration_error(
        message: str,
        config_file: Optional[str] = None,
        **kwargs) -> ConfigurationError:
    """Create configuration error with legacy interface."""
    return ConfigurationError(
        message,
        config_file=config_file,
        context=ErrorContext(
            operation='configuration'
        )
    )
