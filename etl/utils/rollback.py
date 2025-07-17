"""Rollback mechanisms for ETL pipeline operations."""
from __future__ import annotations

import logging
import shutil
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, Callable
from dataclasses import dataclass, field
from enum import Enum

try:
    import arcpy
except ImportError:
    # Mock arcpy for testing environments
    class MockArcPy:
        @staticmethod
        def Exists(path):
            return False
        
        class management:
            @staticmethod
            def Delete(path):
                pass
            
            @staticmethod
            def CreateFeatureclass(*args, **kwargs):
                pass
                
            @staticmethod
            def CreateFileGDB(*args, **kwargs):
                pass
        
        @staticmethod
        def ListFeatureClasses():
            return []
            
        @staticmethod
        def ListTables():
            return []
            
        class env:
            workspace = None
    
    arcpy = MockArcPy()

from ..exceptions import (
    ETLError,
    SystemError,
    ProcessingError,
    PipelineError,
    ErrorContext,
    ErrorSeverity
)

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
        self._lock = threading.Lock()
    
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
    
    def add_file_deletion(self, file_path: Path, description: Optional[str] = None) -> None:
        """Add file deletion rollback action."""
        desc = description or f"Delete file: {file_path}"
        self.add_action(
            RollbackType.FILE_DELETION,
            lambda: self._safe_file_delete(file_path),
            desc,
            {"file_path": str(file_path)},
            priority=1
        )
    
    def add_directory_cleanup(self, dir_path: Path, description: Optional[str] = None) -> None:
        """Add directory cleanup rollback action."""
        desc = description or f"Clean directory: {dir_path}"
        self.add_action(
            RollbackType.DIRECTORY_CLEANUP,
            lambda: self._safe_directory_cleanup(dir_path),
            desc,
            {"dir_path": str(dir_path)},
            priority=2
        )
    
    def add_feature_class_deletion(self, fc_path: str, description: Optional[str] = None) -> None:
        """Add feature class deletion rollback action."""
        desc = description or f"Delete feature class: {fc_path}"
        self.add_action(
            RollbackType.FEATURE_CLASS_DELETION,
            lambda: self._safe_fc_delete(fc_path),
            desc,
            {"fc_path": fc_path},
            priority=3
        )
    
    def add_temp_workspace_cleanup(self, workspace_path: str, description: Optional[str] = None) -> None:
        """Add temporary workspace cleanup rollback action."""
        desc = description or f"Clean temp workspace: {workspace_path}"
        self.add_action(
            RollbackType.TEMP_WORKSPACE_CLEANUP,
            lambda: self._safe_workspace_cleanup(workspace_path),
            desc,
            {"workspace_path": workspace_path},
            priority=4
        )
    
    def add_custom_action(
        self,
        action_func: Callable[[], None],
        description: str,
        metadata: Optional[Dict[str, Any]] = None,
        priority: int = 0
    ) -> None:
        """Add custom rollback action."""
        self.add_action(
            RollbackType.CUSTOM,
            action_func,
            description,
            metadata,
            priority
        )
    
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
    
    def clear_actions(self) -> None:
        """Clear all pending rollback actions without executing them."""
        with self._lock:
            log.debug("🗑️ Cleared %d rollback actions for: %s", len(self.actions), self.operation_name)
            self.actions.clear()
    
    def disable_rollback(self) -> None:
        """Disable rollback for this manager."""
        self.rollback_enabled = False
        log.debug("🚫 Rollback disabled for: %s", self.operation_name)
    
    def enable_rollback(self) -> None:
        """Enable rollback for this manager."""
        self.rollback_enabled = True
        log.debug("✅ Rollback enabled for: %s", self.operation_name)
    
    def get_pending_actions(self) -> List[RollbackAction]:
        """Get list of pending rollback actions."""
        return self.actions.copy()
    
    def get_executed_actions(self) -> List[RollbackAction]:
        """Get list of executed rollback actions."""
        return self.executed_actions.copy()
    
    @staticmethod
    def _safe_file_delete(file_path: Path) -> None:
        """Safely delete a file."""
        if file_path.exists() and file_path.is_file():
            file_path.unlink()
            log.debug("🗑️ Deleted file: %s", file_path)
    
    @staticmethod
    def _safe_directory_cleanup(dir_path: Path) -> None:
        """Safely clean a directory."""
        if dir_path.exists() and dir_path.is_dir():
            shutil.rmtree(dir_path)
            log.debug("🗑️ Cleaned directory: %s", dir_path)
    
    @staticmethod
    def _safe_fc_delete(fc_path: str) -> None:
        """Safely delete a feature class."""
        if arcpy.Exists(fc_path):
            arcpy.management.Delete(fc_path)
            log.debug("🗑️ Deleted feature class: %s", fc_path)
    
    @staticmethod
    def _safe_workspace_cleanup(workspace_path: str) -> None:
        """Safely cleanup temporary workspace."""
        if arcpy.Exists(workspace_path):
            try:
                # Clear workspace
                arcpy.env.workspace = workspace_path
                for fc in arcpy.ListFeatureClasses():
                    arcpy.management.Delete(fc)
                for table in arcpy.ListTables():
                    arcpy.management.Delete(table)
                log.debug("🗑️ Cleaned workspace: %s", workspace_path)
            except Exception as e:
                log.warning("⚠️ Failed to clean workspace %s: %s", workspace_path, e)


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


class TransactionalOperation:
    """Base class for transactional operations with rollback support."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.rollback_manager = RollbackManager(operation_name)
        self.started = False
        self.completed = False
    
    def start(self) -> None:
        """Start the transactional operation."""
        if self.started:
            raise PipelineError(
                f"Operation '{self.operation_name}' already started",
                context=ErrorContext(operation="transactional_operation")
            )
        
        self.started = True
        log.debug("🚀 Started transactional operation: %s", self.operation_name)
    
    def commit(self) -> None:
        """Commit the operation (clear rollback actions)."""
        if not self.started:
            raise PipelineError(
                f"Operation '{self.operation_name}' not started",
                context=ErrorContext(operation="transactional_operation")
            )
        
        self.rollback_manager.clear_actions()
        self.completed = True
        log.debug("✅ Committed transactional operation: %s", self.operation_name)
    
    def rollback(self, reason: str = "Manual rollback") -> None:
        """Rollback the operation."""
        if not self.started:
            log.warning("⚠️ Attempting to rollback operation that was not started: %s", self.operation_name)
            return
        
        self.rollback_manager.execute_rollback(reason)
        self.completed = True
        log.debug("🔄 Rolled back transactional operation: %s", self.operation_name)
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not self.completed:
            # Exception occurred, rollback
            self.rollback(f"Exception: {exc_val}")
        elif not self.completed:
            # Normal completion, commit
            self.commit()


class FileOperationTransaction(TransactionalOperation):
    """Transactional file operations with automatic rollback."""
    
    def __init__(self, operation_name: str = "file_operation"):
        super().__init__(operation_name)
        self.created_files: List[Path] = []
        self.created_directories: List[Path] = []
    
    def create_file(self, file_path: Path, content: str = "") -> Path:
        """Create a file with rollback support."""
        if file_path.exists():
            log.warning("⚠️ File already exists: %s", file_path)
            return file_path
        
        # Create parent directories if needed
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create file
        file_path.write_text(content)
        self.created_files.append(file_path)
        
        # Add rollback action
        self.rollback_manager.add_file_deletion(file_path)
        
        log.debug("📄 Created file: %s", file_path)
        return file_path
    
    def create_directory(self, dir_path: Path) -> Path:
        """Create a directory with rollback support."""
        if dir_path.exists():
            log.warning("⚠️ Directory already exists: %s", dir_path)
            return dir_path
        
        dir_path.mkdir(parents=True, exist_ok=True)
        self.created_directories.append(dir_path)
        
        # Add rollback action
        self.rollback_manager.add_directory_cleanup(dir_path)
        
        log.debug("📁 Created directory: %s", dir_path)
        return dir_path
    
    def copy_file(self, src: Path, dst: Path) -> Path:
        """Copy file with rollback support."""
        if dst.exists():
            log.warning("⚠️ Destination file already exists: %s", dst)
            return dst
        
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        self.created_files.append(dst)
        
        # Add rollback action
        self.rollback_manager.add_file_deletion(dst)
        
        log.debug("📋 Copied file: %s -> %s", src, dst)
        return dst


class ArcPyTransaction(TransactionalOperation):
    """Transactional ArcPy operations with automatic rollback."""
    
    def __init__(self, operation_name: str = "arcpy_operation"):
        super().__init__(operation_name)
        self.created_feature_classes: List[str] = []
        self.temp_workspaces: List[str] = []
    
    def create_feature_class(
        self,
        out_path: str,
        out_name: str,
        geometry_type: str,
        spatial_reference: Optional[Any] = None
    ) -> str:
        """Create feature class with rollback support."""
        fc_path = f"{out_path}\\{out_name}"
        
        if arcpy.Exists(fc_path):
            log.warning("⚠️ Feature class already exists: %s", fc_path)
            return fc_path
        
        arcpy.management.CreateFeatureclass(
            out_path=out_path,
            out_name=out_name,
            geometry_type=geometry_type,
            spatial_reference=spatial_reference
        )
        
        self.created_feature_classes.append(fc_path)
        
        # Add rollback action
        self.rollback_manager.add_feature_class_deletion(fc_path)
        
        log.debug("🗺️ Created feature class: %s", fc_path)
        return fc_path
    
    def create_temp_workspace(self, workspace_type: str = "FILEGDB") -> str:
        """Create temporary workspace with rollback support."""
        import tempfile
        
        if workspace_type.upper() == "FILEGDB":
            temp_dir = tempfile.mkdtemp(prefix="etl_temp_")
            workspace_path = str(Path(temp_dir) / "temp.gdb")
            arcpy.management.CreateFileGDB(temp_dir, "temp.gdb")
        else:
            workspace_path = tempfile.mkdtemp(prefix="etl_workspace_")
        
        self.temp_workspaces.append(workspace_path)
        
        # Add rollback action
        self.rollback_manager.add_temp_workspace_cleanup(workspace_path)
        
        log.debug("🏗️ Created temp workspace: %s", workspace_path)
        return workspace_path


# Global rollback manager for tracking pipeline-level rollbacks
_global_rollback_manager = RollbackManager("global_pipeline")


def get_global_rollback_manager() -> RollbackManager:
    """Get the global rollback manager for pipeline-level operations."""
    return _global_rollback_manager


def add_pipeline_rollback_action(
    action_type: RollbackType,
    action_func: Callable[[], None],
    description: str,
    metadata: Optional[Dict[str, Any]] = None,
    priority: int = 0
) -> None:
    """Add a rollback action to the global pipeline rollback manager."""
    _global_rollback_manager.add_action(
        action_type=action_type,
        action_func=action_func,
        description=description,
        metadata=metadata,
        priority=priority
    )


def execute_pipeline_rollback(reason: str = "Pipeline failed") -> bool:
    """Execute global pipeline rollback."""
    return _global_rollback_manager.execute_rollback(reason)