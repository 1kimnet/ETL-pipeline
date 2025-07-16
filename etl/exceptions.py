"""Custom exception hierarchy for ETL pipeline operations.

This module defines a comprehensive set of exceptions for handling various
error conditions in the ETL pipeline, providing better error classification
and handling strategies.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class ETLError(Exception):
    """Base exception for all ETL pipeline errors.
    
    Provides common functionality for error tracking, context, and recovery hints.
    """
    
    def __init__(
        self,
        message: str,
        *,
        source_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        recoverable: bool = True,
        retry_after: Optional[int] = None
    ):
        super().__init__(message)
        self.message = message
        self.source_name = source_name
        self.context = context or {}
        self.recoverable = recoverable
        self.retry_after = retry_after
    
    def __str__(self) -> str:
        parts = [self.message]
        if self.source_name:
            parts.append(f"(source: {self.source_name})")
        return " ".join(parts)


# Configuration and Validation Errors
class ConfigurationError(ETLError):
    """Raised when configuration is invalid or missing."""
    
    def __init__(self, message: str, config_file: Optional[str] = None, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)
        self.config_file = config_file


class ValidationError(ETLError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, field_name: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.field_name = field_name


# Source and Download Errors
class SourceError(ETLError):
    """Base class for source-related errors."""
    pass


class SourceUnavailableError(SourceError):
    """Raised when a data source is temporarily unavailable."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, retry_after=300, **kwargs)  # Suggest 5min retry


class SourceNotFoundError(SourceError):
    """Raised when a data source cannot be found."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)


class AuthenticationError(SourceError):
    """Raised when authentication with a data source fails."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)


class RateLimitError(SourceError):
    """Raised when rate limits are exceeded."""
    
    def __init__(self, message: str, retry_after: Optional[int] = None, **kwargs):
        super().__init__(message, retry_after=retry_after or 60, **kwargs)


# Network and HTTP Errors
class NetworkError(ETLError):
    """Base class for network-related errors."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, retry_after=30, **kwargs)


class ConnectionError(NetworkError):
    """Raised when network connection fails."""
    pass


class TimeoutError(NetworkError):
    """Raised when network operations timeout."""
    pass


class HTTPError(NetworkError):
    """Raised for HTTP-specific errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.status_code = status_code
        
        # Determine if error is recoverable based on status code
        if status_code:
            # 4xx client errors are generally not recoverable except for rate limiting
            if 400 <= status_code < 500 and status_code != 429:
                self.recoverable = False


# Data Processing Errors
class DataError(ETLError):
    """Base class for data processing errors."""
    pass


class DataFormatError(DataError):
    """Raised when data format is invalid or unsupported."""
    
    def __init__(self, message: str, format_type: Optional[str] = None, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)
        self.format_type = format_type


class DataQualityError(DataError):
    """Raised when data quality checks fail."""
    
    def __init__(self, message: str, quality_check: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.quality_check = quality_check


class TransformationError(DataError):
    """Raised when data transformation fails."""
    pass


class GeospatialError(DataError):
    """Raised when geospatial operations fail."""
    
    def __init__(self, message: str, operation: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.operation = operation


# Storage and File System Errors
class StorageError(ETLError):
    """Base class for storage-related errors."""
    pass


class DiskSpaceError(StorageError):
    """Raised when insufficient disk space is available."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)


class PermissionError(StorageError):
    """Raised when file/directory permissions are insufficient."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)


class FileNotFoundError(StorageError):
    """Raised when required files are not found."""
    
    def __init__(self, message: str, file_path: Optional[str] = None, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)
        self.file_path = file_path


# Database and Loading Errors
class DatabaseError(ETLError):
    """Base class for database-related errors."""
    pass


class ConnectionPoolError(DatabaseError):
    """Raised when database connection pool is exhausted."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, retry_after=60, **kwargs)


class SchemaError(DatabaseError):
    """Raised when database schema issues occur."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)


class LoadError(DatabaseError):
    """Raised when data loading operations fail."""
    pass


# ArcGIS and Geoprocessing Errors
class ArcGISError(ETLError):
    """Base class for ArcGIS-related errors."""
    pass


class GeoprocessingError(ArcGISError):
    """Raised when ArcGIS geoprocessing operations fail."""
    
    def __init__(self, message: str, tool_name: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.tool_name = tool_name


class LicenseError(ArcGISError):
    """Raised when ArcGIS licensing issues occur."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)


class WorkspaceError(ArcGISError):
    """Raised when workspace operations fail."""
    pass


# Pipeline and Workflow Errors
class PipelineError(ETLError):
    """Base class for pipeline workflow errors."""
    pass


class DependencyError(PipelineError):
    """Raised when pipeline dependencies are not met."""
    
    def __init__(self, message: str, dependency: Optional[str] = None, **kwargs):
        super().__init__(message, recoverable=False, **kwargs)
        self.dependency = dependency


class ResourceError(PipelineError):
    """Raised when system resources are insufficient."""
    
    def __init__(self, message: str, resource_type: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.resource_type = resource_type


class CircuitBreakerError(PipelineError):
    """Raised when circuit breaker is open."""
    
    def __init__(self, message: str, service_name: Optional[str] = None, **kwargs):
        super().__init__(message, retry_after=300, **kwargs)
        self.service_name = service_name


# Utility functions for error handling
def is_recoverable_error(error: Exception) -> bool:
    """Check if an error is recoverable and should be retried."""
    if isinstance(error, ETLError):
        return error.recoverable
    
    # Handle standard Python exceptions
    if isinstance(error, (ConnectionRefusedError, TimeoutError)):
        return True
    
    return False


def get_retry_delay(error: Exception) -> Optional[int]:
    """Get suggested retry delay for an error."""
    if isinstance(error, ETLError):
        return error.retry_after
    return None


def format_error_context(error: ETLError) -> str:
    """Format error context for logging."""
    parts = [str(error)]
    
    if error.context:
        context_parts = [f"{k}={v}" for k, v in error.context.items()]
        parts.append(f"Context: {', '.join(context_parts)}")
    
    if not error.recoverable:
        parts.append("(non-recoverable)")
    elif error.retry_after:
        parts.append(f"(retry after {error.retry_after}s)")
    
    return " | ".join(parts)