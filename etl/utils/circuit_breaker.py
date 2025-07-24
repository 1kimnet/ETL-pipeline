"""Circuit breaker implementation for external service calls."""
from __future__ import annotations

import functools
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar
from dataclasses import dataclass, field
from enum import Enum

from ..exceptions import (
    PipelineError,
    NetworkError,
    SourceError,
    ErrorContext,
    ErrorSeverity
)

log = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit breaker is open, blocking calls
    HALF_OPEN = "half_open"  # Testing if service is back online


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker operations."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    consecutive_failures: int = 0
    last_failure_time: Optional[float] = None
    state_changes: int = 0
    last_state_change_time: Optional[float] = None

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as percentage."""
        if self.total_calls == 0:
            return 0.0
        return (self.failed_calls / self.total_calls) * 100.0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_calls == 0:
            return 0.0
        return (self.successful_calls / self.total_calls) * 100.0


class CircuitBreaker:
    """Circuit breaker pattern implementation with ETL-specific enhancements."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exceptions: Optional[List[Type[Exception]]] = None,
        half_open_max_calls: int = 3,
        name: Optional[str] = None
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.name = name or "unnamed_circuit_breaker"

        # Default exceptions that trigger circuit breaker
        self.expected_exceptions = expected_exceptions or [
            NetworkError,
            SourceError,
            ConnectionError,
            TimeoutError
        ]

        # State management
        self.state = CircuitBreakerState.CLOSED
        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0

        # Thread safety
        self._lock = threading.Lock()

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to apply circuit breaker to a function."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return self._call_with_circuit_breaker(func, *args, **kwargs)
        return wrapper

    def _call_with_circuit_breaker(
            self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker logic."""
        with self._lock:
            # Check if circuit breaker should block the call
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
                else:
                    raise PipelineError(
                        f"Circuit breaker '{self.name}' is OPEN - calls blocked",
                        dependency=self.name,
                        context=ErrorContext(
                            operation="circuit_breaker_check",
                            metadata={
                                "state": self.state.value,
                                "consecutive_failures": self.stats.consecutive_failures,
                                "last_failure_time": self.stats.last_failure_time}))

            # Track call in half-open state
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_calls += 1

        # Execute the function
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            # Check if this exception should trigger circuit breaker
            if self._should_handle_exception(e):
                self._on_failure(e)
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt circuit breaker reset."""
        if self.stats.last_failure_time is None:
            return True
        return time.time() - self.stats.last_failure_time >= self.recovery_timeout

    def _should_handle_exception(self, exception: Exception) -> bool:
        """Check if the exception should trigger circuit breaker logic."""
        return any(isinstance(exception, exc_type)
                   for exc_type in self.expected_exceptions)

    def _on_success(self) -> None:
        """Handle successful function execution."""
        with self._lock:
            self.stats.total_calls += 1
            self.stats.successful_calls += 1
            self.stats.consecutive_failures = 0

            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    self._transition_to_closed()
                    log.info(
                        "✅ Circuit breaker '%s' reset to CLOSED after successful test",
                        self.name)

            log.debug("✅ Circuit breaker '%s' recorded success", self.name)

    def _on_failure(self, exception: Exception) -> None:
        """Handle failed function execution."""
        with self._lock:
            self.stats.total_calls += 1
            self.stats.failed_calls += 1
            self.stats.consecutive_failures += 1
            self.stats.last_failure_time = time.time()

            if self.state == CircuitBreakerState.HALF_OPEN:
                self._transition_to_open()
                log.warning(
                    "🔴 Circuit breaker '%s' reopened after failure during test",
                    self.name)
            elif self.state == CircuitBreakerState.CLOSED:
                if self.stats.consecutive_failures >= self.failure_threshold:
                    self._transition_to_open()
                    log.warning(
                        "🔴 Circuit breaker '%s' OPENED after %d consecutive failures",
                        self.name,
                        self.stats.consecutive_failures)

            log.debug(
                "❌ Circuit breaker '%s' recorded failure: %s",
                self.name,
                exception)

    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state."""
        self.state = CircuitBreakerState.CLOSED
        self.half_open_calls = 0
        self.stats.state_changes += 1
        self.stats.last_state_change_time = time.time()

    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        self.state = CircuitBreakerState.OPEN
        self.half_open_calls = 0
        self.stats.state_changes += 1
        self.stats.last_state_change_time = time.time()

    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        self.state = CircuitBreakerState.HALF_OPEN
        self.half_open_calls = 0
        self.stats.state_changes += 1
        self.stats.last_state_change_time = time.time()
        log.info(
            "🔄 Circuit breaker '%s' transitioned to HALF_OPEN for testing",
            self.name)

    def reset(self) -> None:
        """Manually reset the circuit breaker to CLOSED state."""
        with self._lock:
            self._transition_to_closed()
            self.stats.consecutive_failures = 0
            log.info("🔄 Circuit breaker '%s' manually reset", self.name)

    def get_stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics."""
        with self._lock:
            return CircuitBreakerStats(
                total_calls=self.stats.total_calls,
                successful_calls=self.stats.successful_calls,
                failed_calls=self.stats.failed_calls,
                consecutive_failures=self.stats.consecutive_failures,
                last_failure_time=self.stats.last_failure_time,
                state_changes=self.stats.state_changes,
                last_state_change_time=self.stats.last_state_change_time
            )

    def get_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        return self.state

    def is_call_permitted(self) -> bool:
        """Check if a call would be permitted without executing it."""
        with self._lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            elif self.state == CircuitBreakerState.OPEN:
                return self._should_attempt_reset()
            else:  # HALF_OPEN
                return self.half_open_calls < self.half_open_max_calls


class CircuitBreakerManager:
    """Manages multiple circuit breakers for different services."""

    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get_circuit_breaker(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exceptions: Optional[List[Type[Exception]]] = None,
        half_open_max_calls: int = 3
    ) -> CircuitBreaker:
        """Get or create a circuit breaker for a service."""
        with self._lock:
            if name not in self.circuit_breakers:
                self.circuit_breakers[name] = CircuitBreaker(
                    failure_threshold=failure_threshold,
                    recovery_timeout=recovery_timeout,
                    expected_exceptions=expected_exceptions,
                    half_open_max_calls=half_open_max_calls,
                    name=name
                )
            return self.circuit_breakers[name]

    def get_all_stats(self) -> Dict[str, CircuitBreakerStats]:
        """Get statistics for all circuit breakers."""
        with self._lock:
            return {name: cb.get_stats()
                    for name, cb in self.circuit_breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            for cb in self.circuit_breakers.values():
                cb.reset()

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all circuit breakers."""
        with self._lock:
            return {
                "total_breakers": len(self.circuit_breakers),
                "states": {
                    name: cb.get_state().value
                    for name, cb in self.circuit_breakers.items()
                },
                "stats": self.get_all_stats()
            }


# Global circuit breaker manager
_circuit_breaker_manager = CircuitBreakerManager()


def get_circuit_breaker_manager() -> CircuitBreakerManager:
    """Get the global circuit breaker manager."""
    return _circuit_breaker_manager


def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exceptions: Optional[List[Type[Exception]]] = None,
    half_open_max_calls: int = 3
):
    """Decorator to add circuit breaker protection to a function."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cb = _circuit_breaker_manager.get_circuit_breaker(
            name=name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exceptions=expected_exceptions,
            half_open_max_calls=half_open_max_calls
        )
        return cb(func)
    return decorator


# Predefined circuit breakers for common services
def http_circuit_breaker(name: str, failure_threshold: int = 5):
    """Circuit breaker specifically for HTTP services."""
    return circuit_breaker(
        name=name,
        failure_threshold=failure_threshold,
        recovery_timeout=30.0,
        expected_exceptions=[
            NetworkError,
            SourceError,
            ConnectionError,
            TimeoutError])


def database_circuit_breaker(name: str, failure_threshold: int = 3):
    """Circuit breaker specifically for database operations."""
    return circuit_breaker(
        name=name,
        failure_threshold=failure_threshold,
        recovery_timeout=60.0,
        expected_exceptions=[SystemError, ConnectionError]
    )


def file_circuit_breaker(name: str, failure_threshold: int = 3):
    """Circuit breaker specifically for file operations."""
    return circuit_breaker(
        name=name,
        failure_threshold=failure_threshold,
        recovery_timeout=15.0,
        expected_exceptions=[SystemError, IOError, OSError]
    )
