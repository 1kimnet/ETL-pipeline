"""Unit tests for etl.utils.retry module."""
import pytest
import time
from unittest.mock import Mock, patch

from etl.utils.retry import (
    RetryConfig,
    CircuitBreaker,
    retry_with_backoff,
    retry_on_exceptions,
    RetryableOperation
)
from etl.exceptions import (
    NetworkError,
    SourceUnavailableError,
    RateLimitError,
    CircuitBreakerError
)


class TestRetryConfig:
    """Test RetryConfig functionality."""

    @pytest.mark.unit
    def test_default_retry_config(self):
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.backoff_factor == 2.0
        assert config.max_delay == 300.0
        assert config.jitter is True
        assert config.exponential is True

    @pytest.mark.unit
    def test_custom_retry_config(self):
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            backoff_factor=3.0,
            max_delay=600.0,
            jitter=False,
            exponential=False
        )
        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.backoff_factor == 3.0
        assert config.max_delay == 600.0
        assert config.jitter is False
        assert config.exponential is False

    @pytest.mark.unit
    def test_should_retry_max_attempts(self):
        config = RetryConfig(max_attempts=3)
        assert config.should_retry(NetworkError("test"), 1) is True
        assert config.should_retry(NetworkError("test"), 3) is False
        assert config.should_retry(NetworkError("test"), 4) is False

    @pytest.mark.unit
    def test_should_retry_recoverable_exceptions(self):
        config = RetryConfig()
        assert config.should_retry(NetworkError("test"), 1) is True
        assert config.should_retry(SourceUnavailableError("test"), 1) is True
        assert config.should_retry(ValueError("test"), 1) is False

    @pytest.mark.unit
    def test_get_delay_exponential(self):
        config = RetryConfig(
            base_delay=1.0,
            backoff_factor=2.0,
            exponential=True,
            jitter=False)
        assert config.get_delay(1) == 1.0
        assert config.get_delay(2) == 2.0
        assert config.get_delay(3) == 4.0

    @pytest.mark.unit
    def test_get_delay_linear(self):
        config = RetryConfig(base_delay=2.0, exponential=False, jitter=False)
        assert config.get_delay(1) == 2.0
        assert config.get_delay(2) == 2.0
        assert config.get_delay(3) == 2.0

    @pytest.mark.unit
    def test_get_delay_max_delay(self):
        config = RetryConfig(
            base_delay=100.0,
            backoff_factor=10.0,
            max_delay=200.0,
            jitter=False)
        # Would be 1000.0, but capped at max_delay
        assert config.get_delay(3) == 200.0

    @pytest.mark.unit
    def test_get_delay_with_exception_retry_after(self):
        config = RetryConfig()
        error = RateLimitError("Rate limited", retry_after=120)
        assert config.get_delay(1, error) == 120.0

    @pytest.mark.unit
    def test_get_delay_with_jitter(self):
        config = RetryConfig(base_delay=10.0, jitter=True, exponential=False)
        delay = config.get_delay(1)
        # With jitter, delay should be between 5.0 and 10.0
        assert 5.0 <= delay <= 10.0


class TestCircuitBreaker:
    """Test CircuitBreaker functionality."""

    @pytest.mark.unit
    def test_circuit_breaker_closed_state(self):
        cb = CircuitBreaker(failure_threshold=3)
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0

    @pytest.mark.unit
    def test_circuit_breaker_success(self):
        cb = CircuitBreaker(failure_threshold=3)

        @cb
        def test_function():
            return "success"

        result = test_function()
        assert result == "success"
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0

    @pytest.mark.unit
    def test_circuit_breaker_single_failure(self):
        cb = CircuitBreaker(failure_threshold=3)

        @cb
        def test_function():
            raise Exception("test error")

        with pytest.raises(Exception, match="test error"):
            test_function()

        assert cb.state == "CLOSED"  # Still closed, below threshold
        assert cb.failure_count == 1

    @pytest.mark.unit
    def test_circuit_breaker_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=2)

        @cb
        def test_function():
            raise Exception("test error")

        # First failure
        with pytest.raises(Exception):
            test_function()
        assert cb.state == "CLOSED"

        # Second failure - should open circuit
        with pytest.raises(Exception):
            test_function()
        assert cb.state == "OPEN"

    @pytest.mark.unit
    def test_circuit_breaker_open_state_blocks_calls(self):
        cb = CircuitBreaker(failure_threshold=1)

        @cb
        def test_function():
            raise Exception("test error")

        # Trigger circuit breaker
        with pytest.raises(Exception):
            test_function()
        assert cb.state == "OPEN"

        # Next call should raise CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            test_function()

    @pytest.mark.unit
    def test_circuit_breaker_half_open_recovery(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        @cb
        def test_function(should_fail=True):
            if should_fail:
                raise Exception("test error")
            return "success"

        # Trigger circuit breaker
        with pytest.raises(Exception):
            test_function()
        assert cb.state == "OPEN"

        # Wait for recovery timeout
        time.sleep(0.15)

        # Next call should attempt half-open
        result = test_function(should_fail=False)
        assert result == "success"
        assert cb.state == "CLOSED"


class TestRetryWithBackoff:
    """Test retry_with_backoff decorator."""

    @pytest.mark.unit
    def test_successful_function_no_retry(self):
        call_count = 0

        @retry_with_backoff(max_attempts=3, base_delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.unit
    def test_function_succeeds_after_retries(self):
        call_count = 0

        @retry_with_backoff(max_attempts=3, base_delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise NetworkError("temporary failure")
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.unit
    def test_function_fails_after_max_attempts(self):
        call_count = 0

        @retry_with_backoff(max_attempts=2, base_delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise NetworkError("persistent failure")

        with pytest.raises(NetworkError, match="persistent failure"):
            test_function()
        assert call_count == 2

    @pytest.mark.unit
    def test_non_recoverable_error_no_retry(self):
        call_count = 0

        @retry_with_backoff(max_attempts=3, base_delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise ValueError("non-recoverable error")

        with pytest.raises(ValueError, match="non-recoverable error"):
            test_function()
        assert call_count == 1

    @pytest.mark.unit
    def test_retry_with_custom_config(self):
        config = RetryConfig(max_attempts=5, base_delay=0.01)
        call_count = 0

        @retry_with_backoff(config=config)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise NetworkError("temporary failure")
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 4


class TestRetryOnExceptions:
    """Test retry_on_exceptions decorator."""

    @pytest.mark.unit
    def test_retry_on_specific_exception(self):
        call_count = 0

        @retry_on_exceptions(ValueError, max_attempts=3, delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("retryable error")
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.unit
    def test_no_retry_on_different_exception(self):
        call_count = 0

        @retry_on_exceptions(ValueError, max_attempts=3, delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            raise TypeError("different error")

        with pytest.raises(TypeError, match="different error"):
            test_function()
        assert call_count == 1

    @pytest.mark.unit
    def test_retry_on_multiple_exceptions(self):
        call_count = 0

        @retry_on_exceptions([ValueError, TypeError],
                             max_attempts=3, delay=0.01)
        def test_function():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("first error")
            elif call_count == 2:
                raise TypeError("second error")
            return "success"

        result = test_function()
        assert result == "success"
        assert call_count == 3


class TestRetryableOperation:
    """Test RetryableOperation context manager."""

    @pytest.mark.unit
    def test_successful_operation(self):
        operation = RetryableOperation("test_operation")

        with operation:
            pass  # Successful operation

        assert operation.attempt == 1

    @pytest.mark.unit
    def test_failed_operation(self):
        operation = RetryableOperation("test_operation")

        with pytest.raises(ValueError):
            with operation:
                raise ValueError("operation failed")

        assert operation.attempt == 1

    @pytest.mark.unit
    def test_should_retry_decision(self):
        config = RetryConfig(max_attempts=3)
        operation = RetryableOperation("test_operation", config=config)

        # Simulate first attempt
        operation.attempt = 1
        assert operation.should_retry(NetworkError("recoverable")) is True
        assert operation.should_retry(ValueError("non-recoverable")) is False

        # Simulate max attempts reached
        operation.attempt = 3
        assert operation.should_retry(NetworkError("recoverable")) is False

    @pytest.mark.unit
    def test_get_retry_delay(self):
        config = RetryConfig(base_delay=1.0, jitter=False)
        operation = RetryableOperation("test_operation", config=config)
        operation.attempt = 1

        delay = operation.get_retry_delay(NetworkError("test"))
        assert delay == 1.0
