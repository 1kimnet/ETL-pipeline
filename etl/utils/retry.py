"""Retry mechanisms with exponential backoff for ETL operations.

This module provides decorators and utilities for implementing robust retry
logic with various backoff strategies, circuit breakers, and error handling.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from typing import Any, Callable, Dict, List, Optional, Type, Union

from ..exceptions import (
    ConcurrentError,
    ErrorContext,
    ETLError,
    NetworkError,
    PipelineError,
    ProcessingError,
    SourceError,
    classify_exception,
    get_retry_delay,
    is_recoverable_error,
)
from ..exceptions import SystemError as ETLSystemError

log = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 300.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        exponential: bool = True,
        recoverable_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.exponential = exponential
        self.recoverable_exceptions = recoverable_exceptions or [
            NetworkError,
            SourceError,
            ETLSystemError,
            ProcessingError,
            ConcurrentError,
            ConnectionError,
            TimeoutError,
        ]

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if an exception should trigger a retry."""
        if attempt >= self.max_attempts:
            return False

        # Check ETL-specific retry logic
        if isinstance(exception, ETLError):
            return is_recoverable_error(exception)

        # Check against configured recoverable exceptions
        return any(isinstance(exception, exc_type)
                   for exc_type in self.recoverable_exceptions)

    def get_delay(
            self,
            attempt: int,
            exception: Optional[Exception] = None) -> float:
        """Calculate delay before next retry attempt."""
        # Check if exception specifies a retry delay
        if exception and isinstance(exception, ETLError):
            suggested_delay = get_retry_delay(exception)
            if suggested_delay:
                return float(suggested_delay)

        if self.exponential:
            delay = self.base_delay * (self.backoff_factor ** (attempt - 1))
        else:
            delay = self.base_delay

        # Apply jitter to avoid thundering herd
        if self.jitter:
            delay *= 0.5 + random.random() * 0.5

        return min(delay, self.max_delay)


class CircuitBreaker:
    """Circuit breaker pattern implementation for external service calls."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def __call__(self, func: Callable) -> Callable:
        """Decorator to apply circuit breaker to a function."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self._call_with_circuit_breaker(func, *args, **kwargs)

        return wrapper

    def _call_with_circuit_breaker(
            self,
            func: Callable,
            *args,
            **kwargs) -> Any:
        """Execute function with circuit breaker logic."""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                log.info("🔄 Circuit breaker half-open, attempting reset")
            else:
                raise PipelineError(
                    f"Circuit breaker is OPEN for {func.__name__}",
                    dependency=func.__name__,
                    context=ErrorContext(operation="circuit_breaker"),
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt circuit breaker reset."""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.recovery_timeout

    def _on_success(self) -> None:
        """Handle successful function execution."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            log.info("✅ Circuit breaker reset to CLOSED")
        self.failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed function execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            log.warning(
                "🔴 Circuit breaker OPEN after %d failures",
                self.failure_count)


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 300.0,
    jitter: bool = True,
) -> Callable:
    """Decorator to add retry logic with exponential backoff to a function.

    Args:
        config: RetryConfig object with retry settings
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay between retries (seconds)
        backoff_factor: Exponential backoff multiplier
        max_delay: Maximum delay between retries (seconds)
        jitter: Add random jitter to delay

    Returns:
        Decorated function with retry logic
    """
    if config is None:
        config = RetryConfig(
            max_attempts=max_attempts,
            base_delay=base_delay,
            backoff_factor=backoff_factor,
            max_delay=max_delay,
            jitter=jitter,
        )

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(1, config.max_attempts + 1):
                try:
                    log.debug(
                        "🔄 Attempting %s (attempt %d/%d)",
                        func.__name__,
                        attempt,
                        config.max_attempts,
                    )
                    result = func(*args, **kwargs)

                    if attempt > 1:
                        log.info(
                            "✅ %s succeeded on attempt %d",
                            func.__name__,
                            attempt)

                    return result

                except Exception as e:
                    last_exception = e

                    if not config.should_retry(e, attempt):
                        log.debug(
                            "❌ %s failed with non-recoverable error: %s",
                            func.__name__,
                            e,
                        )
                        raise

                    if attempt == config.max_attempts:
                        log.error(
                            "❌ %s failed after %d attempts: %s",
                            func.__name__,
                            attempt,
                            e,
                        )
                        raise

                    delay = config.get_delay(attempt, e)
                    log.warning(
                        "⚠️  %s failed (attempt %d/%d): %s. Retrying in %.1fs",
                        func.__name__,
                        attempt,
                        config.max_attempts,
                        e,
                        delay,
                    )

                    time.sleep(delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def retry_on_exceptions(
    exceptions: Union[Type[Exception], List[Type[Exception]]],
    max_attempts: int = 3,
    delay: float = 1.0,
) -> Callable:
    """Simple retry decorator for specific exception types.

    Args:
        exceptions: Exception type(s) to retry on
        max_attempts: Maximum number of attempts
        delay: Fixed delay between retries

    Returns:
        Decorated function with retry logic
    """
    if not isinstance(exceptions, (list, tuple)):
        exceptions = [exceptions]

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not any(isinstance(e, exc_type)
                               for exc_type in exceptions):
                        raise

                    if attempt == max_attempts - 1:
                        raise

                    log.warning(
                        "Retrying %s due to %s",
                        func.__name__,
                        type(e).__name__)
                    time.sleep(delay)

        return wrapper

    return decorator


class RetryableOperation:
    """Context manager for retryable operations with detailed logging."""

    def __init__(
        self,
        operation_name: str,
        config: Optional[RetryConfig] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.operation_name = operation_name
        self.config = config or RetryConfig()
        self.context = context or {}
        self.attempt = 0
        self.start_time = 0.0

    def __enter__(self):
        self.attempt += 1
        self.start_time = time.time()
        log.debug(
            "🚀 Starting %s (attempt %d)",
            self.operation_name,
            self.attempt)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time

        if exc_type is None:
            log.debug("✅ %s completed in %.2fs", self.operation_name, duration)
            return True

        log.error(
            "❌ %s failed after %.2fs: %s",
            self.operation_name,
            duration,
            exc_val)

        return False  # Don't suppress exceptions

    def should_retry(self, exception: Exception) -> bool:
        """Check if operation should be retried."""
        return self.config.should_retry(exception, self.attempt)

    def get_retry_delay(self, exception: Exception) -> float:
        """Get delay before next retry."""
        return self.config.get_delay(self.attempt, exception)


class RetryStatistics:
    """Global retry statistics tracking."""

    def __init__(self):
        self.operation_stats: Dict[str, Dict[str, int]] = {}
        self.total_retries = 0
        self.total_successes = 0
        self.total_failures = 0

    def record_attempt(
            self,
            operation: str,
            success: bool,
            attempt_number: int):
        """Record a retry attempt."""
        if operation not in self.operation_stats:
            self.operation_stats[operation] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "total_retry_attempts": 0,
            }

        stats = self.operation_stats[operation]
        stats["attempts"] += 1

        if success:
            stats["successes"] += 1
            self.total_successes += 1
        else:
            stats["failures"] += 1
            self.total_failures += 1

        if attempt_number > 1:
            stats["total_retry_attempts"] += attempt_number - 1
            self.total_retries += attempt_number - 1

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of retry statistics."""
        return {
            "total_operations": sum(
                stats["attempts"] for stats in self.operation_stats.values()
            ),
            "total_retries": self.total_retries,
            "total_successes": self.total_successes,
            "total_failures": self.total_failures,
            "success_rate": (
                self.total_successes / (self.total_successes + self.total_failures)
            )
            * 100
            if (self.total_successes + self.total_failures) > 0
            else 0,
            "operations": self.operation_stats,
        }


# Global retry statistics instance
_retry_stats = RetryStatistics()


def get_retry_statistics() -> RetryStatistics:
    """Get global retry statistics."""
    return _retry_stats


# Enhanced retry patterns for specific operations
NETWORK_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=2.0,
    max_delay=120.0,
    backoff_factor=2.0,
    recoverable_exceptions=[
        NetworkError,
        SourceError,
        ConnectionError,
        TimeoutError],
)

DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=5.0,
    max_delay=300.0,
    backoff_factor=2.5,
    recoverable_exceptions=[ETLSystemError, ProcessingError],
)

FILE_OPERATION_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=60.0,
    backoff_factor=1.5,
    recoverable_exceptions=[ETLSystemError, ProcessingError, OSError, IOError],
)

CONCURRENT_RETRY_CONFIG = RetryConfig(
    max_attempts=2,
    base_delay=1.0,
    max_delay=30.0,
    backoff_factor=2.0,
    recoverable_exceptions=[ConcurrentError, ETLSystemError],
)


def enhanced_retry_with_stats(
    operation_name: str, config: Optional[RetryConfig] = None
) -> Callable:
    """Enhanced retry decorator with statistics tracking."""
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(1, config.max_attempts + 1):
                try:
                    log.debug(
                        "🔄 %s attempt %d/%d",
                        operation_name,
                        attempt,
                        config.max_attempts,
                    )
                    result = func(*args, **kwargs)

                    # Record successful attempt
                    _retry_stats.record_attempt(operation_name, True, attempt)

                    if attempt > 1:
                        log.info(
                            "✅ %s succeeded on attempt %d",
                            operation_name,
                            attempt)

                    return result

                except Exception as e:
                    last_exception = e

                    # Classify unknown exceptions
                    if not isinstance(e, ETLError):
                        classified_error = classify_exception(e)
                        log.debug(
                            "🔍 Classified %s as %s",
                            type(e).__name__,
                            type(classified_error).__name__,
                        )
                        last_exception = classified_error

                    if not config.should_retry(last_exception, attempt):
                        log.debug(
                            "❌ %s failed with non-recoverable error: %s",
                            operation_name,
                            last_exception,
                        )
                        _retry_stats.record_attempt(
                            operation_name, False, attempt)
                        raise last_exception

                    if attempt == config.max_attempts:
                        log.error(
                            "❌ %s failed after %d attempts: %s",
                            operation_name,
                            attempt,
                            last_exception,
                        )
                        _retry_stats.record_attempt(
                            operation_name, False, attempt)
                        raise last_exception

                    delay = config.get_delay(attempt, last_exception)
                    log.warning(
                        "⚠️  %s failed (attempt %d/%d): %s. Retrying in %.1fs",
                        operation_name,
                        attempt,
                        config.max_attempts,
                        last_exception,
                        delay,
                    )

                    time.sleep(delay)

            # This should never be reached, but just in case
            if last_exception:
                _retry_stats.record_attempt(
                    operation_name, False, config.max_attempts)
                raise last_exception

        return wrapper

    return decorator


def smart_retry(operation_name: Optional[str] = None):
    """Smart retry decorator that automatically selects appropriate config based on operation name."""

    def decorator(func: Callable) -> Callable:
        nonlocal operation_name
        if operation_name is None:  # type: ignore [unreachable]
            operation_name = func.__name__

        # Select appropriate config based on operation name patterns
        if any(
            pattern in operation_name.lower()
            for pattern in ["http", "request", "download", "fetch"]
        ):
            config = NETWORK_RETRY_CONFIG
        elif any(
            pattern in operation_name.lower()
            for pattern in ["database", "sql", "sde", "arcpy"]
        ):
            config = DATABASE_RETRY_CONFIG
        elif any(
            pattern in operation_name.lower()
            for pattern in ["file", "read", "write", "copy"]
        ):
            config = FILE_OPERATION_RETRY_CONFIG
        elif any(
            pattern in operation_name.lower()
            for pattern in ["concurrent", "parallel", "thread"]
        ):
            config = CONCURRENT_RETRY_CONFIG
        else:
            config = RetryConfig()  # Default config

        return enhanced_retry_with_stats(operation_name, config)(func)

    return decorator
