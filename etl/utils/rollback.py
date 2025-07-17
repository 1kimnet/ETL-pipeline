"""Rollback mechanisms for ETL pipeline operations."""
from __future__ import annotations

import logging
import threading
import shutil
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, Callable
from dataclasses import dataclass, field
from enum import Enum

# Fixed: Import threading at the top of the file to avoid NameError
# when threading.RLock() is called in RollbackManager

log = logging.getLogger(__name__)


class RollbackType(Enum):
    """Types of rollback operations."""
    FILE_DELETION = "file_deletion"
    DIRECTORY_CLEANUP = "directory_cleanup"
    FEATURE_CLASS_DELETION = "feature_class_deletion"
    DATABASE_TRANSACTION = "database_transaction"
    HTTP_SESSION_CLEANUP = "http_session_cleanup"
    TEMP_WORKSPACE_CLEANUP = "temp_workspace_cleanup"
    CUSTOM = "custom"


@dataclass
class RollbackAction:
    """Represents a single rollback action."""
    action_type: RollbackType
    action_func: Callable[[], None]
    description: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # Higher priority executed first
    created_at: float = field(default_factory=time.time)
    
    def execute(self) -> bool:
        """Execute the rollback action."""
        try:
            log.debug("🔄 Executing rollback: %s", self.description)
            self.action_func()
            log.debug("✅ Rollback completed: %s", self.description)
            return True
        except Exception as e:
            log.error("❌ Rollback failed: %s - %s", self.description, e)
            return False


class RollbackManager:
    """Manages rollback actions for ETL operations."""
    
    def __init__(self, operation_name: str = "unknown"):
        self.operation_name = operation_name
        self.actions: List[RollbackAction] = []
        self.executed_actions: List[RollbackAction] = []
        self.rollback_enabled = True
        self._lock = threading.RLock()  # Now threading is imported at the top
    
    def add_action(
        self,
        action_type: RollbackType,
        action_func: Callable[[], None],
        description: str,
        metadata: Optional[Dict[str, Any]] = None,
        priority: int = 0
    ) -> None:
        """Add a rollback action to the stack."""
        with self._lock:
            action = RollbackAction(
                action_type=action_type,
                action_func=action_func,
                description=description,
                metadata=metadata or {},
                priority=priority
            )
            self.actions.append(action)
            log.debug("📝 Added rollback action: %s", description)
    
    def clear_actions(self) -> None:
        """Clear all pending rollback actions without executing them."""
        with self._lock:
            log.debug("🗑️ Cleared %d rollback actions for: %s", len(self.actions), self.operation_name)
            self.actions.clear()
    
    def execute_rollback(self, reason: str = "Operation failed") -> bool:
        """Execute all rollback actions in reverse order (LIFO)."""
        if not self.rollback_enabled:
            log.info("🚫 Rollback disabled for operation: %s", self.operation_name)
            return True
        
        if not self.actions:
            log.debug("ℹ️ No rollback actions to execute for: %s", self.operation_name)
            return True
        
        log.info("🔄 Starting rollback for '%s': %s", self.operation_name, reason)
        
        # Sort by priority (higher first), then reverse chronological order
        sorted_actions = sorted(
            self.actions,
            key=lambda a: (a.priority, -a.created_at),
            reverse=True
        )
        
        success_count = 0
        total_actions = len(sorted_actions)
        
        for action in sorted_actions:
            if action.execute():
                success_count += 1
                self.executed_actions.append(action)
            # Continue even if individual actions fail
        
        log.info(
            "🏁 Rollback completed for '%s': %d/%d actions successful",
            self.operation_name,
            success_count,
            total_actions
        )
        
        # Clear actions after rollback
        self.actions.clear()
        
        return success_count == total_actions


@contextmanager
def rollback_on_failure(operation_name: str, auto_rollback: bool = True):
    """Context manager that provides automatic rollback on operation failure."""
    rollback_manager = RollbackManager(operation_name)
    try:
        yield rollback_manager
        # Success - clear rollback actions
        if auto_rollback:
            rollback_manager.clear_actions()
    except Exception as e:
        # Failure - execute rollback
        if auto_rollback:
            rollback_manager.execute_rollback(f"Exception occurred: {e}")
        raise


# Global rollback manager for tracking pipeline-level rollbacks
_global_rollback_manager = RollbackManager("global_pipeline")


def get_global_rollback_manager() -> RollbackManager:
    """Get the global rollback manager for pipeline-level operations."""
    return _global_rollback_manager


def execute_pipeline_rollback(reason: str = "Pipeline failed") -> bool:
    """Execute global pipeline rollback."""
    return _global_rollback_manager.execute_rollback(reason)