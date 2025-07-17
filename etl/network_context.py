"""Network context for managing dynamic network configuration.

This module provides a NetworkContext class that encapsulates mutable network
settings, allowing for graceful degradation without modifying the global
configuration object directly. This makes the flow of dynamic settings clearer
and more maintainable.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

log = logging.getLogger(__name__)


@dataclass
class NetworkContext:
    """Mutable context for network configuration and degradation.
    
    This class provides a way to manage dynamic network settings without
    modifying the global configuration directly. It allows for graceful
    degradation of network parameters in response to failures while
    maintaining clear separation between static configuration and runtime
    adjustments.
    
    Attributes:
        timeout: Current timeout value in seconds
        max_retries: Current maximum retry attempts
        backoff_factor: Current exponential backoff factor
        max_delay: Current maximum delay between retries
        circuit_breaker_threshold: Current circuit breaker failure threshold
        rate_limit_delay: Additional delay for rate limiting
        degraded: Whether the context is in degraded mode
        degradation_history: History of degradation events
    """
    
    # Network timing settings
    timeout: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 2.0
    max_delay: float = 300.0
    
    # Circuit breaker settings
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    
    # Rate limiting
    rate_limit_delay: float = 0.0
    
    # Degradation tracking
    degraded: bool = False
    degradation_level: int = 0
    degradation_history: list[Dict[str, Any]] = field(default_factory=list)
    
    # Context metadata
    source_name: Optional[str] = None
    handler_type: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    
    @classmethod
    def from_global_config(
        cls,
        global_config: Dict[str, Any],
        source_name: Optional[str] = None,
        handler_type: Optional[str] = None
    ) -> NetworkContext:
        """Create a NetworkContext from global configuration.
        
        Args:
            global_config: Global configuration dictionary
            source_name: Name of the source using this context
            handler_type: Type of handler using this context
            
        Returns:
            NetworkContext initialized with global config values
        """
        retry_config = global_config.get("retry", {})
        circuit_config = global_config.get("circuit_breaker", {})
        
        return cls(
            timeout=global_config.get("timeout", 30.0),
            max_retries=retry_config.get("max_attempts", 3),
            backoff_factor=retry_config.get("backoff_factor", 2.0),
            max_delay=retry_config.get("max_delay", 300.0),
            circuit_breaker_threshold=circuit_config.get("threshold", 5),
            circuit_breaker_timeout=circuit_config.get("timeout", 60.0),
            source_name=source_name,
            handler_type=handler_type
        )
    
    def degrade_network_config(
        self,
        reason: str,
        error: Optional[Exception] = None,
        severity: str = "moderate"
    ) -> None:
        """Apply network degradation to improve resilience.
        
        This method implements graceful degradation by adjusting network
        parameters to be more conservative when failures occur. Unlike
        modifying global_cfg directly, this approach keeps the degradation
        isolated to this context instance.
        
        Args:
            reason: Human-readable reason for degradation
            error: Optional exception that triggered degradation
            severity: Degradation severity ("mild", "moderate", "severe")
        """
        self.degradation_level += 1
        self.degraded = True
        
        # Record degradation event
        degradation_event = {
            "timestamp": time.time(),
            "reason": reason,
            "severity": severity,
            "level": self.degradation_level,
            "error_type": type(error).__name__ if error else None,
            "error_message": str(error) if error else None
        }
        self.degradation_history.append(degradation_event)
        
        # Apply degradation based on severity
        if severity == "mild":
            self._apply_mild_degradation()
        elif severity == "moderate":
            self._apply_moderate_degradation()
        elif severity == "severe":
            self._apply_severe_degradation()
        else:
            log.warning("Unknown degradation severity: %s, applying moderate", severity)
            self._apply_moderate_degradation()
        
        log.warning(
            "🔻 Network degradation applied - Source: %s, Reason: %s, Level: %d",
            self.source_name or "unknown",
            reason,
            self.degradation_level
        )
    
    def _apply_mild_degradation(self) -> None:
        """Apply mild network degradation."""
        self.timeout = min(self.timeout * 1.2, 60.0)
        self.max_retries = min(self.max_retries + 1, 5)
        self.rate_limit_delay = max(self.rate_limit_delay, 0.5)
    
    def _apply_moderate_degradation(self) -> None:
        """Apply moderate network degradation."""
        self.timeout = min(self.timeout * 1.5, 90.0)
        self.max_retries = min(self.max_retries + 2, 7)
        self.backoff_factor = min(self.backoff_factor * 1.5, 4.0)
        self.rate_limit_delay = max(self.rate_limit_delay, 1.0)
        self.circuit_breaker_threshold = max(self.circuit_breaker_threshold - 1, 2)
    
    def _apply_severe_degradation(self) -> None:
        """Apply severe network degradation."""
        self.timeout = min(self.timeout * 2.0, 120.0)
        self.max_retries = min(self.max_retries + 3, 10)
        self.backoff_factor = min(self.backoff_factor * 2.0, 5.0)
        self.max_delay = min(self.max_delay * 1.5, 600.0)
        self.rate_limit_delay = max(self.rate_limit_delay, 2.0)
        self.circuit_breaker_threshold = max(self.circuit_breaker_threshold - 2, 1)
    
    def reset_degradation(self, global_config: Dict[str, Any]) -> None:
        """Reset the context to original configuration values.
        
        Args:
            global_config: Global configuration to reset to
        """
        original_context = self.from_global_config(
            global_config,
            self.source_name,
            self.handler_type
        )
        
        # Reset network settings to original values
        self.timeout = original_context.timeout
        self.max_retries = original_context.max_retries
        self.backoff_factor = original_context.backoff_factor
        self.max_delay = original_context.max_delay
        self.circuit_breaker_threshold = original_context.circuit_breaker_threshold
        self.circuit_breaker_timeout = original_context.circuit_breaker_timeout
        self.rate_limit_delay = 0.0
        
        # Reset degradation state
        self.degraded = False
        self.degradation_level = 0
        
        log.info(
            "🔄 Network context reset - Source: %s",
            self.source_name or "unknown"
        )
    
    def should_apply_rate_limit(self) -> bool:
        """Check if rate limiting should be applied."""
        return self.rate_limit_delay > 0
    
    def apply_rate_limit(self) -> None:
        """Apply rate limiting delay if configured."""
        if self.should_apply_rate_limit():
            log.debug(
                "⏳ Applying rate limit delay: %.2fs - Source: %s",
                self.rate_limit_delay,
                self.source_name or "unknown"
            )
            time.sleep(self.rate_limit_delay)
    
    def get_retry_config_dict(self) -> Dict[str, Any]:
        """Get current configuration as a dictionary for retry mechanisms.
        
        Returns:
            Dictionary compatible with RetryConfig
        """
        return {
            "max_attempts": self.max_retries,
            "base_delay": 1.0,  # Keep base delay constant
            "backoff_factor": self.backoff_factor,
            "max_delay": self.max_delay
        }
    
    def get_circuit_breaker_config_dict(self) -> Dict[str, Any]:
        """Get current circuit breaker configuration as a dictionary.
        
        Returns:
            Dictionary with circuit breaker settings
        """
        return {
            "failure_threshold": self.circuit_breaker_threshold,
            "recovery_timeout": self.circuit_breaker_timeout
        }
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get a summary of the current context status.
        
        Returns:
            Dictionary with context status information
        """
        return {
            "source_name": self.source_name,
            "handler_type": self.handler_type,
            "degraded": self.degraded,
            "degradation_level": self.degradation_level,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "backoff_factor": self.backoff_factor,
            "rate_limit_delay": self.rate_limit_delay,
            "degradation_events": len(self.degradation_history),
            "created_at": self.created_at
        }
    
    def __str__(self) -> str:
        """String representation of the context."""
        status = "degraded" if self.degraded else "normal"
        return (
            f"NetworkContext(source={self.source_name}, "
            f"status={status}, level={self.degradation_level}, "
            f"timeout={self.timeout}s, retries={self.max_retries})"
        )