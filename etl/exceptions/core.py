"""Core exception hierarchy for ETL pipeline - consolidated and streamlined."""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum


class ErrorSeverity(Enum):
    """Error severity levels for proper handling."""
    LOW = "low"           # Warnings, can continue
    MEDIUM = "medium"     # Errors, but recoverable
    HIGH = "high"        # Critical errors, stop current operation
    CRITICAL = "critical" # Pipeline-breaking errors


class ErrorCategory(Enum):
    """Error categories for proper classification."""
    NETWORK = "network"
    DATA = "data"
    SYSTEM = "system"
    CONFIGURATION = "configuration"


@dataclass
class ErrorContext:
    """Enhanced error context with structured information."""
    source_name: Optional[str] = None
    operation: Optional[str] = None
    file_path: Optional[str] = None
    url: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "source_name": self.source_name,
            "operation": self.operation,
            "file_path": self.file_path,
            "url": self.url,
            "timestamp": self.timestamp,
            "retry_count": self.retry_count,
            "metadata": self.metadata
        }


class ETLError(Exception):
    """Base exception for all ETL pipeline errors with enhanced functionality."""
    
    def __init__(
        self,
        message: str,
        *,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        context: Optional[ErrorContext] = None,
        recoverable: bool = True,
        retry_after: Optional[float] = None,
        cause: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.severity = severity
        self.category = category
        self.context = context or ErrorContext()
        self.recoverable = recoverable
        self.retry_after = retry_after
        self.cause = cause
        
        # Set the cause for proper exception chaining
        if cause:
            self.__cause__ = cause
    
    def __str__(self) -> str:
        """Enhanced string representation."""
        parts = [self.message]
        
        if self.context.source_name:
            parts.append(f"[source: {self.context.source_name}]")
        
        if self.context.operation:
            parts.append(f"[operation: {self.context.operation}]")
        
        if self.context.retry_count > 0:
            parts.append(f"[retry: {self.context.retry_count}]")
        
        return " ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for structured logging."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "severity": self.severity.value,
            "category": self.category.value,
            "recoverable": self.recoverable,
            "retry_after": self.retry_after,
            "context": self.context.to_dict(),
            "cause": str(self.cause) if self.cause else None
        }


# 1. NETWORK ERRORS (replaces HTTPError, NetworkError, ConnectionError, TimeoutError, RateLimitError)
class NetworkError(ETLError):
    """Network-related errors including HTTP, connection, and timeout issues."""
    
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        url: Optional[str] = None,
        timeout: Optional[float] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        context.url = url
        if timeout:
            context.metadata['timeout'] = timeout
        if status_code:
            context.metadata['status_code'] = status_code
        
        # Determine severity and recoverability based on status code
        severity = ErrorSeverity.MEDIUM
        recoverable = True
        retry_after = None
        
        if status_code:
            if status_code == 429:  # Rate limit
                retry_after = 60.0
                severity = ErrorSeverity.LOW
            elif 500 <= status_code < 600:  # Server errors
                retry_after = 30.0
                severity = ErrorSeverity.MEDIUM
            elif 400 <= status_code < 500:  # Client errors (except 429)
                recoverable = False
                severity = ErrorSeverity.HIGH
        
        super().__init__(
            message,
            severity=severity,
            category=ErrorCategory.NETWORK,
            context=context,
            recoverable=recoverable,
            retry_after=retry_after,
            **kwargs
        )
        
        self.status_code = status_code
        self.url = url
        self.timeout = timeout


# 2. DATA ERRORS (replaces DataError, DataFormatError, DataQualityError, ValidationError, GeospatialError)
class DataError(ETLError):
    """Data-related errors including format, quality, and validation issues."""
    
    def __init__(
        self,
        message: str,
        *,
        data_type: Optional[str] = None,
        file_path: Optional[str] = None,
        field_name: Optional[str] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        context.file_path = file_path
        if data_type:
            context.metadata['data_type'] = data_type
        if field_name:
            context.metadata['field_name'] = field_name
        
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.DATA,
            context=context,
            recoverable=False,  # Data errors usually aren't recoverable
            **kwargs
        )
        
        self.data_type = data_type
        self.file_path = file_path
        self.field_name = field_name


# 3. SYSTEM ERRORS (replaces StorageError, ResourceError, PermissionError, DiskSpaceError)
class SystemError(ETLError):
    """System-related errors including storage, resources, and permissions."""
    
    def __init__(
        self,
        message: str,
        *,
        resource_type: Optional[str] = None,
        available: Optional[Union[int, float]] = None,
        required: Optional[Union[int, float]] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        if resource_type:
            context.metadata['resource_type'] = resource_type
        if available is not None:
            context.metadata['available'] = available
        if required is not None:
            context.metadata['required'] = required
        
        # Determine severity based on resource type
        severity = ErrorSeverity.HIGH
        recoverable = False
        
        if resource_type == "disk_space":
            severity = ErrorSeverity.CRITICAL
        elif resource_type == "memory":
            severity = ErrorSeverity.HIGH
            recoverable = True
            
        super().__init__(
            message,
            severity=severity,
            category=ErrorCategory.SYSTEM,
            context=context,
            recoverable=recoverable,
            **kwargs
        )
        
        self.resource_type = resource_type
        self.available = available
        self.required = required


# 4. CONFIGURATION ERRORS (replaces ConfigurationError)
class ConfigurationError(ETLError):
    """Configuration-related errors."""
    
    def __init__(
        self,
        message: str,
        *,
        config_file: Optional[str] = None,
        config_key: Optional[str] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        context.file_path = config_file
        if config_key:
            context.metadata['config_key'] = config_key
        
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CONFIGURATION,
            context=context,
            recoverable=False,
            **kwargs
        )
        
        self.config_file = config_file
        self.config_key = config_key


# 5. SOURCE ERRORS (replaces SourceError, SourceUnavailableError, SourceNotFoundError, AuthenticationError)
class SourceError(ETLError):
    """Source-related errors including availability, authentication, and access."""
    
    def __init__(
        self,
        message: str,
        *,
        source_type: Optional[str] = None,
        available: bool = True,
        authenticated: bool = True,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        if source_type:
            context.metadata['source_type'] = source_type
        context.metadata['available'] = available
        context.metadata['authenticated'] = authenticated
        
        # Determine severity and recoverability
        severity = ErrorSeverity.MEDIUM
        recoverable = True
        retry_after = None
        
        if not available:
            retry_after = 300.0  # 5 minutes for unavailable sources
        elif not authenticated:
            recoverable = False
            severity = ErrorSeverity.HIGH
            
        super().__init__(
            message,
            severity=severity,
            category=ErrorCategory.NETWORK, # Or a new ErrorCategory.SOURCE
            context=context,
            recoverable=recoverable,
            retry_after=retry_after,
            **kwargs
        )
        
        self.source_type = source_type
        self.available = available
        self.authenticated = authenticated


# 6. PROCESSING ERRORS (replaces TransformationError, GeoprocessingError, LoadError)
class ProcessingError(ETLError):
    """Processing-related errors including transformation, geoprocessing, and loading."""
    
    def __init__(
        self,
        message: str,
        *,
        process_type: Optional[str] = None,
        stage: Optional[str] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        if process_type:
            context.metadata['process_type'] = process_type
        if stage:
            context.metadata['stage'] = stage
        
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.SYSTEM,
            context=context,
            recoverable=True,
            **kwargs
        )
        
        self.process_type = process_type
        self.stage = stage


# 7. PIPELINE ERRORS (replaces PipelineError, DependencyError, CircuitBreakerError)
class PipelineError(ETLError):
    """Pipeline-level errors including dependencies and circuit breakers."""
    
    def __init__(
        self,
        message: str,
        *,
        pipeline_stage: Optional[str] = None,
        dependency: Optional[str] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        if pipeline_stage:
            context.metadata['pipeline_stage'] = pipeline_stage
        if dependency:
            context.metadata['dependency'] = dependency
        
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.SYSTEM,
            context=context,
            recoverable=True,
            retry_after=300.0,  # 5 minutes for pipeline errors
            **kwargs
        )
        
        self.pipeline_stage = pipeline_stage
        self.dependency = dependency


# 8. CONCURRENT ERRORS (new - for concurrent operations)
class ConcurrentError(ETLError):
    """Concurrent operation errors including thread pool and task failures."""
    
    def __init__(
        self,
        message: str,
        *,
        task_name: Optional[str] = None,
        worker_count: Optional[int] = None,
        failed_tasks: Optional[int] = None,
        **kwargs
    ):
        # Set context
        context = kwargs.get('context', ErrorContext())
        if task_name:
            context.metadata['task_name'] = task_name
        if worker_count:
            context.metadata['worker_count'] = worker_count
        if failed_tasks:
            context.metadata['failed_tasks'] = failed_tasks
        
        super().__init__(
            message,
            severity=ErrorSeverity.MEDIUM,
            category=ErrorCategory.SYSTEM,
            context=context,
            recoverable=True,
            **kwargs
        )
        
        self.task_name = task_name
        self.worker_count = worker_count
        self.failed_tasks = failed_tasks


# Utility functions for error handling
def classify_exception(exc: Exception) -> ETLError:
    """Classify a standard exception into our error hierarchy."""
    if isinstance(exc, ETLError):
        return exc
    
    # Network-related exceptions
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return NetworkError(
            f"Network error: {exc}",
            cause=exc,
            context=ErrorContext(operation="network_request")
        )
    
    # File system exceptions
    if isinstance(exc, (FileNotFoundError, PermissionError, OSError)):
        return SystemError(
            f"System error: {exc}",
            resource_type="file_system",
            cause=exc,
            context=ErrorContext(operation="file_system")
        )
    
    # Data format exceptions
    if isinstance(exc, (ValueError, TypeError)) and "json" in str(exc).lower():
        return DataError(
            f"Data format error: {exc}",
            data_type="json",
            cause=exc,
            context=ErrorContext(operation="data_parsing")
        )
    
    # Generic system error
    return SystemError(
        f"Unexpected error: {exc}",
        cause=exc,
        context=ErrorContext(operation="unknown")
    )


def is_recoverable_error(error: Exception) -> bool:
    """Check if an error is recoverable and should be retried."""
    if isinstance(error, ETLError):
        return error.recoverable
    
    # Standard exceptions that are usually recoverable
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    
    return False


def get_retry_delay(error: Exception) -> Optional[float]:
    """Get suggested retry delay for an error."""
    if isinstance(error, ETLError):
        return error.retry_after
    
    # Default delays for standard exceptions
    if isinstance(error, ConnectionError):
        return 30.0
    elif isinstance(error, TimeoutError):
        return 60.0
    
    return None


def format_error_for_logging(error: Exception) -> Dict[str, Any]:
    """Format error for structured logging."""
    if isinstance(error, ETLError):
        return error.to_dict()
    
    # Format standard exceptions
    return {
        "error_type": error.__class__.__name__,
        "message": str(error),
        "severity": "medium",
        "category": "system",
        "recoverable": is_recoverable_error(error),
        "retry_after": get_retry_delay(error),
        "context": {"operation": "unknown"}
    }