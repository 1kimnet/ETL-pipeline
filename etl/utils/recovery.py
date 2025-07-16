"""Error recovery and graceful degradation mechanisms for ETL pipeline."""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar
from dataclasses import dataclass, field
from enum import Enum

from ..exceptions import (
    ETLError,
    NetworkError,
    SourceError,
    DataError,
    SystemError,
    ProcessingError,
    PipelineError,
    ConcurrentError,
    ErrorContext,
    ErrorSeverity
)

log = logging.getLogger(__name__)

T = TypeVar('T')


class RecoveryStrategy(Enum):
    """Available recovery strategies."""
    SKIP = "skip"                    # Skip the failed operation
    RETRY = "retry"                  # Retry with backoff
    FALLBACK = "fallback"           # Use fallback data/method
    PARTIAL = "partial"             # Continue with partial results
    DEGRADE = "degrade"             # Reduce quality/functionality
    MANUAL = "manual"               # Require manual intervention
    ABORT = "abort"                 # Stop the pipeline


@dataclass
class RecoveryAction:
    """Represents a recovery action for a specific error."""
    strategy: RecoveryStrategy
    action_func: Optional[Callable[[], Any]] = None
    fallback_data: Optional[Any] = None
    description: str = ""
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def execute(self) -> Any:
        """Execute the recovery action."""
        if self.action_func:
            return self.action_func()
        return self.fallback_data


@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    success: bool
    strategy_used: RecoveryStrategy
    recovered_data: Any = None
    error: Optional[Exception] = None
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class RecoveryManager:
    """Manages error recovery strategies and graceful degradation."""
    
    def __init__(self):
        self.recovery_strategies: Dict[str, List[RecoveryAction]] = {}
        self.global_strategies: List[RecoveryAction] = []
        self.recovery_stats: Dict[str, Dict[str, int]] = {}
        self.degradation_level = 0
        self.max_degradation_level = 3
    
    def register_recovery_strategy(
        self,
        error_type: Union[str, Type[Exception]],
        strategy: RecoveryStrategy,
        action_func: Optional[Callable[[], Any]] = None,
        fallback_data: Optional[Any] = None,
        description: str = "",
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register a recovery strategy for a specific error type."""
        error_key = error_type.__name__ if isinstance(error_type, type) else str(error_type)
        
        if error_key not in self.recovery_strategies:
            self.recovery_strategies[error_key] = []
        
        recovery_action = RecoveryAction(
            strategy=strategy,
            action_func=action_func,
            fallback_data=fallback_data,
            description=description,
            priority=priority,
            metadata=metadata or {}
        )
        
        self.recovery_strategies[error_key].append(recovery_action)
        
        # Sort by priority (higher first)
        self.recovery_strategies[error_key].sort(key=lambda x: x.priority, reverse=True)
        
        log.debug("📝 Registered recovery strategy for %s: %s", error_key, strategy.value)
    
    def register_global_strategy(
        self,
        strategy: RecoveryStrategy,
        action_func: Optional[Callable[[], Any]] = None,
        fallback_data: Optional[Any] = None,
        description: str = "",
        priority: int = 0
    ) -> None:
        """Register a global recovery strategy that applies to all errors."""
        recovery_action = RecoveryAction(
            strategy=strategy,
            action_func=action_func,
            fallback_data=fallback_data,
            description=description,
            priority=priority
        )
        
        self.global_strategies.append(recovery_action)
        self.global_strategies.sort(key=lambda x: x.priority, reverse=True)
        
        log.debug("📝 Registered global recovery strategy: %s", strategy.value)
    
    def recover_from_error(
        self,
        error: Exception,
        operation_context: str,
        fallback_data: Optional[Any] = None
    ) -> RecoveryResult:
        """Attempt to recover from an error using registered strategies."""
        error_type = type(error).__name__
        
        # Update statistics
        if operation_context not in self.recovery_stats:
            self.recovery_stats[operation_context] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0
            }
        
        self.recovery_stats[operation_context]["attempts"] += 1
        
        log.info(
            "🔄 Attempting recovery from %s in %s: %s",
            error_type,
            operation_context,
            str(error)
        )
        
        # Try specific strategies first
        strategies_to_try = []
        
        # Add error-specific strategies
        if error_type in self.recovery_strategies:
            strategies_to_try.extend(self.recovery_strategies[error_type])
        
        # Add global strategies
        strategies_to_try.extend(self.global_strategies)
        
        # Sort by priority
        strategies_to_try.sort(key=lambda x: x.priority, reverse=True)
        
        # Try each strategy
        for strategy in strategies_to_try:
            try:
                log.debug("🔄 Trying recovery strategy: %s", strategy.strategy.value)
                
                if strategy.strategy == RecoveryStrategy.SKIP:
                    result = self._handle_skip_strategy(error, operation_context, strategy)
                elif strategy.strategy == RecoveryStrategy.FALLBACK:
                    result = self._handle_fallback_strategy(error, operation_context, strategy, fallback_data)
                elif strategy.strategy == RecoveryStrategy.PARTIAL:
                    result = self._handle_partial_strategy(error, operation_context, strategy)
                elif strategy.strategy == RecoveryStrategy.DEGRADE:
                    result = self._handle_degrade_strategy(error, operation_context, strategy)
                elif strategy.strategy == RecoveryStrategy.MANUAL:
                    result = self._handle_manual_strategy(error, operation_context, strategy)
                elif strategy.strategy == RecoveryStrategy.ABORT:
                    result = self._handle_abort_strategy(error, operation_context, strategy)
                else:
                    # Custom strategy with action function
                    recovered_data = strategy.execute()
                    result = RecoveryResult(
                        success=True,
                        strategy_used=strategy.strategy,
                        recovered_data=recovered_data,
                        message=f"Custom recovery strategy succeeded: {strategy.description}"
                    )
                
                if result.success:
                    self.recovery_stats[operation_context]["successes"] += 1
                    log.info("✅ Recovery successful using %s strategy", strategy.strategy.value)
                    return result
                    
            except Exception as recovery_error:
                log.warning(
                    "⚠️ Recovery strategy %s failed: %s",
                    strategy.strategy.value,
                    recovery_error
                )
                continue
        
        # All strategies failed
        self.recovery_stats[operation_context]["failures"] += 1
        log.error("❌ All recovery strategies failed for %s", operation_context)
        
        return RecoveryResult(
            success=False,
            strategy_used=RecoveryStrategy.ABORT,
            error=error,
            message=f"No recovery strategy succeeded for {error_type}"
        )
    
    def _handle_skip_strategy(
        self,
        error: Exception,
        operation_context: str,
        strategy: RecoveryAction
    ) -> RecoveryResult:
        """Handle skip recovery strategy."""
        log.warning("⏭️ Skipping failed operation: %s", operation_context)
        
        return RecoveryResult(
            success=True,
            strategy_used=RecoveryStrategy.SKIP,
            recovered_data=None,
            message=f"Skipped operation due to error: {error}"
        )
    
    def _handle_fallback_strategy(
        self,
        error: Exception,
        operation_context: str,
        strategy: RecoveryAction,
        fallback_data: Optional[Any] = None
    ) -> RecoveryResult:
        """Handle fallback recovery strategy."""
        data_to_use = fallback_data or strategy.fallback_data
        
        if data_to_use is None and strategy.action_func:
            data_to_use = strategy.action_func()
        
        if data_to_use is not None:
            log.info("🔄 Using fallback data for: %s", operation_context)
            return RecoveryResult(
                success=True,
                strategy_used=RecoveryStrategy.FALLBACK,
                recovered_data=data_to_use,
                message=f"Used fallback data: {strategy.description}"
            )
        
        return RecoveryResult(
            success=False,
            strategy_used=RecoveryStrategy.FALLBACK,
            error=error,
            message="No fallback data available"
        )
    
    def _handle_partial_strategy(
        self,
        error: Exception,
        operation_context: str,
        strategy: RecoveryAction
    ) -> RecoveryResult:
        """Handle partial recovery strategy."""
        log.info("🔄 Continuing with partial results for: %s", operation_context)
        
        partial_data = strategy.execute() if strategy.action_func else []
        
        return RecoveryResult(
            success=True,
            strategy_used=RecoveryStrategy.PARTIAL,
            recovered_data=partial_data,
            message=f"Continuing with partial results: {strategy.description}"
        )
    
    def _handle_degrade_strategy(
        self,
        error: Exception,
        operation_context: str,
        strategy: RecoveryAction
    ) -> RecoveryResult:
        """Handle degradation recovery strategy."""
        if self.degradation_level >= self.max_degradation_level:
            log.error("❌ Maximum degradation level reached, cannot degrade further")
            return RecoveryResult(
                success=False,
                strategy_used=RecoveryStrategy.DEGRADE,
                error=error,
                message="Maximum degradation level reached"
            )
        
        self.degradation_level += 1
        log.warning("⬇️ Degrading service level to %d for: %s", self.degradation_level, operation_context)
        
        degraded_data = strategy.execute() if strategy.action_func else None
        
        return RecoveryResult(
            success=True,
            strategy_used=RecoveryStrategy.DEGRADE,
            recovered_data=degraded_data,
            message=f"Service degraded to level {self.degradation_level}: {strategy.description}",
            metadata={"degradation_level": self.degradation_level}
        )
    
    def _handle_manual_strategy(
        self,
        error: Exception,
        operation_context: str,
        strategy: RecoveryAction
    ) -> RecoveryResult:
        """Handle manual intervention recovery strategy."""
        log.error("🔧 Manual intervention required for: %s", operation_context)
        log.error("   Error: %s", str(error))
        log.error("   Instructions: %s", strategy.description or "No specific instructions provided")
        
        return RecoveryResult(
            success=False,
            strategy_used=RecoveryStrategy.MANUAL,
            error=error,
            message=f"Manual intervention required: {strategy.description}"
        )
    
    def _handle_abort_strategy(
        self,
        error: Exception,
        operation_context: str,
        strategy: RecoveryAction
    ) -> RecoveryResult:
        """Handle abort recovery strategy."""
        log.error("🛑 Aborting operation: %s", operation_context)
        
        return RecoveryResult(
            success=False,
            strategy_used=RecoveryStrategy.ABORT,
            error=error,
            message=f"Operation aborted: {strategy.description}"
        )
    
    def get_recovery_stats(self) -> Dict[str, Dict[str, Union[int, float]]]:
        """Get recovery statistics."""
        stats = {}
        for context, data in self.recovery_stats.items():
            total = data["attempts"]
            success_rate = (data["successes"] / total * 100) if total > 0 else 0
            stats[context] = {
                **data,
                "success_rate": success_rate
            }
        return stats
    
    def reset_degradation_level(self) -> None:
        """Reset degradation level to normal."""
        old_level = self.degradation_level
        self.degradation_level = 0
        if old_level > 0:
            log.info("⬆️ Service degradation level reset from %d to 0", old_level)
    
    def get_degradation_level(self) -> int:
        """Get current degradation level."""
        return self.degradation_level


@contextmanager
def graceful_degradation(
    operation_name: str,
    recovery_manager: Optional[RecoveryManager] = None,
    fallback_data: Optional[Any] = None
):
    """Context manager for graceful degradation of operations."""
    mgr = recovery_manager or get_global_recovery_manager()
    
    try:
        yield mgr
    except Exception as e:
        log.warning("⚠️ Operation '%s' failed, attempting recovery", operation_name)
        
        recovery_result = mgr.recover_from_error(
            error=e,
            operation_context=operation_name,
            fallback_data=fallback_data
        )
        
        if recovery_result.success:
            log.info("✅ Recovery successful for '%s'", operation_name)
            # Return recovered data somehow - this is a limitation of context managers
            # In practice, you'd handle this at the application level
        else:
            log.error("❌ Recovery failed for '%s', re-raising exception", operation_name)
            raise


def recoverable_operation(
    operation_name: str,
    recovery_manager: Optional[RecoveryManager] = None,
    fallback_data: Optional[Any] = None
):
    """Decorator to make operations recoverable."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs) -> T:
            mgr = recovery_manager or get_global_recovery_manager()
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.warning("⚠️ Function '%s' failed, attempting recovery", operation_name)
                
                recovery_result = mgr.recover_from_error(
                    error=e,
                    operation_context=operation_name,
                    fallback_data=fallback_data
                )
                
                if recovery_result.success:
                    log.info("✅ Recovery successful for '%s'", operation_name)
                    return recovery_result.recovered_data
                else:
                    log.error("❌ Recovery failed for '%s', re-raising exception", operation_name)
                    raise
        
        return wrapper
    return decorator


# Global recovery manager
_global_recovery_manager = RecoveryManager()


def get_global_recovery_manager() -> RecoveryManager:
    """Get the global recovery manager."""
    return _global_recovery_manager


def setup_default_recovery_strategies():
    """Setup default recovery strategies for common error types."""
    mgr = _global_recovery_manager
    
    # Network errors - retry with degradation
    mgr.register_recovery_strategy(
        NetworkError,
        RecoveryStrategy.DEGRADE,
        description="Reduce concurrent connections and retry",
        priority=3
    )
    
    mgr.register_recovery_strategy(
        NetworkError,
        RecoveryStrategy.SKIP,
        description="Skip failed network operation",
        priority=1
    )
    
    # Source errors - fallback to cached data
    mgr.register_recovery_strategy(
        SourceError,
        RecoveryStrategy.FALLBACK,
        description="Use cached data if available",
        priority=2
    )
    
    mgr.register_recovery_strategy(
        SourceError,
        RecoveryStrategy.SKIP,
        description="Skip unavailable source",
        priority=1
    )
    
    # Data errors - use partial data
    mgr.register_recovery_strategy(
        DataError,
        RecoveryStrategy.PARTIAL,
        description="Continue with valid data only",
        priority=2
    )
    
    mgr.register_recovery_strategy(
        DataError,
        RecoveryStrategy.SKIP,
        description="Skip invalid data",
        priority=1
    )
    
    # System errors - manual intervention
    mgr.register_recovery_strategy(
        SystemError,
        RecoveryStrategy.MANUAL,
        description="Check system resources and permissions",
        priority=3
    )
    
    mgr.register_recovery_strategy(
        SystemError,
        RecoveryStrategy.SKIP,
        description="Skip system-dependent operation",
        priority=1
    )
    
    # Processing errors - degrade quality
    mgr.register_recovery_strategy(
        ProcessingError,
        RecoveryStrategy.DEGRADE,
        description="Reduce processing quality/complexity",
        priority=2
    )
    
    mgr.register_recovery_strategy(
        ProcessingError,
        RecoveryStrategy.SKIP,
        description="Skip complex processing",
        priority=1
    )
    
    # Pipeline errors - abort
    mgr.register_recovery_strategy(
        PipelineError,
        RecoveryStrategy.ABORT,
        description="Critical pipeline failure",
        priority=1
    )
    
    # Concurrent errors - retry with reduced concurrency
    mgr.register_recovery_strategy(
        ConcurrentError,
        RecoveryStrategy.DEGRADE,
        description="Reduce concurrent workers",
        priority=2
    )
    
    mgr.register_recovery_strategy(
        ConcurrentError,
        RecoveryStrategy.SKIP,
        description="Skip concurrent operation",
        priority=1
    )
    
    # Global fallback - skip operation
    mgr.register_global_strategy(
        RecoveryStrategy.SKIP,
        description="Skip failed operation as last resort",
        priority=0
    )


class GracefulDegradationConfig:
    """Configuration for graceful degradation behavior."""
    
    def __init__(self):
        self.max_concurrent_downloads = 5
        self.max_retry_attempts = 3
        self.timeout_seconds = 30
        self.max_file_size_mb = 100
        self.enable_fallback_data = True
        self.degradation_thresholds = {
            1: {"concurrent_downloads": 3, "timeout": 60, "max_file_size": 50},
            2: {"concurrent_downloads": 1, "timeout": 120, "max_file_size": 25},
            3: {"concurrent_downloads": 1, "timeout": 300, "max_file_size": 10}
        }
    
    def get_degraded_config(self, level: int) -> Dict[str, Any]:
        """Get configuration for a specific degradation level."""
        if level == 0:
            return {
                "concurrent_downloads": self.max_concurrent_downloads,
                "timeout": self.timeout_seconds,
                "max_file_size": self.max_file_size_mb
            }
        
        return self.degradation_thresholds.get(level, self.degradation_thresholds[3])


# Initialize default strategies
setup_default_recovery_strategies()