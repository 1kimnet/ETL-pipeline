"""ArcPy context managers for safe workspace and environment management."""
from __future__ import annotations

import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Union

import arcpy

log = logging.getLogger(__name__)


class ArcPyWorkspaceManager:
    """Context manager for safe ArcPy workspace management."""
    
    def __init__(self, workspace: Union[str, Path], overwrite_output: bool = True):
        self.workspace = str(workspace)
        self.overwrite_output = overwrite_output
        self.original_workspace: Optional[str] = None
        self.original_overwrite: Optional[bool] = None
        
    def __enter__(self) -> str:
        """Enter the context and set workspace."""
        # Store original settings
        self.original_workspace = arcpy.env.workspace
        self.original_overwrite = arcpy.env.overwriteOutput
        
        # Set new workspace
        arcpy.env.workspace = self.workspace
        arcpy.env.overwriteOutput = self.overwrite_output
        
        log.debug("Set ArcPy workspace to: %s", self.workspace)
        return self.workspace
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context and restore original settings."""
        try:
            # Restore original settings
            arcpy.env.workspace = self.original_workspace
            arcpy.env.overwriteOutput = self.original_overwrite
            log.debug("Restored ArcPy workspace to: %s", self.original_workspace)
        except Exception as e:
            log.error("Failed to restore ArcPy workspace: %s", e)
        
        # Clear any ArcPy geoprocessing results
        try:
            arcpy.ClearWorkspaceCache_management()
        except Exception as e:
            log.debug("Failed to clear workspace cache: %s", e)


class ArcPyEnvironmentManager:
    """Context manager for comprehensive ArcPy environment management."""
    
    def __init__(self, **env_settings):
        self.env_settings = env_settings
        self.original_settings: Dict[str, Any] = {}
        
    def __enter__(self) -> Dict[str, Any]:
        """Enter the context and set environment variables."""
        # Store original settings
        for key, value in self.env_settings.items():
            if hasattr(arcpy.env, key):
                self.original_settings[key] = getattr(arcpy.env, key)
                setattr(arcpy.env, key, value)
                log.debug("Set arcpy.env.%s = %s", key, value)
        
        return self.env_settings
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context and restore original settings."""
        try:
            # Restore original settings
            for key, original_value in self.original_settings.items():
                if hasattr(arcpy.env, key):
                    setattr(arcpy.env, key, original_value)
                    log.debug("Restored arcpy.env.%s = %s", key, original_value)
        except Exception as e:
            log.error("Failed to restore ArcPy environment: %s", e)
        
        # Clear workspace cache
        try:
            arcpy.ClearWorkspaceCache_management()
        except Exception as e:
            log.debug("Failed to clear workspace cache: %s", e)


class ArcPyTempWorkspace:
    """Context manager for temporary workspace operations."""
    
    def __init__(self, prefix: str = "etl_temp_", cleanup: bool = True):
        self.prefix = prefix
        self.cleanup = cleanup
        self.temp_dir: Optional[Path] = None
        self.original_workspace: Optional[str] = None
        
    def __enter__(self) -> Path:
        """Create temporary workspace and set as current."""
        # Create temporary directory
        self.temp_dir = Path(tempfile.mkdtemp(prefix=self.prefix))
        
        # Store original workspace
        self.original_workspace = arcpy.env.workspace
        
        # Set temporary workspace
        arcpy.env.workspace = str(self.temp_dir)
        
        log.debug("Created temporary workspace: %s", self.temp_dir)
        return self.temp_dir
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original workspace and cleanup if requested."""
        try:
            # Restore original workspace
            arcpy.env.workspace = self.original_workspace
            log.debug("Restored workspace from temporary: %s", self.original_workspace)
            
            # Clear workspace cache
            arcpy.ClearWorkspaceCache_management()
            
            # Cleanup temporary directory if requested
            if self.cleanup and self.temp_dir and self.temp_dir.exists():
                import shutil
                shutil.rmtree(self.temp_dir, ignore_errors=True)
                log.debug("Cleaned up temporary workspace: %s", self.temp_dir)
                
        except Exception as e:
            log.error("Failed to cleanup temporary workspace: %s", e)


@contextmanager
def arcpy_workspace(workspace: Union[str, Path], 
                   overwrite_output: bool = True) -> Generator[str, None, None]:
    """Context manager for safe ArcPy workspace operations."""
    with ArcPyWorkspaceManager(workspace, overwrite_output) as ws:
        yield ws


@contextmanager
def arcpy_environment(**env_settings) -> Generator[Dict[str, Any], None, None]:
    """Context manager for ArcPy environment settings."""
    with ArcPyEnvironmentManager(**env_settings) as env:
        yield env


@contextmanager
def arcpy_temp_workspace(prefix: str = "etl_temp_", 
                        cleanup: bool = True) -> Generator[Path, None, None]:
    """Context manager for temporary workspace operations."""
    with ArcPyTempWorkspace(prefix, cleanup) as temp_ws:
        yield temp_ws


def safe_arcpy_operation(func):
    """Decorator for safe ArcPy operations with automatic cleanup."""
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            try:
                arcpy.ClearWorkspaceCache_management()
            except Exception as e:
                log.debug("Failed to clear workspace cache: %s", e)
    return wrapper