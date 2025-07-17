"""Thread-safe concurrent processing utilities for ETL pipeline operations."""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class ConcurrentResult:
    """Result of a concurrent operation."""
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConcurrentDownloadManager:
    """Thread-safe concurrent download manager."""
    
    def __init__(self, max_workers: Optional[int] = None, timeout: Optional[float] = None):
        self.max_workers = max_workers or 5
        self.timeout = timeout
        self._lock = threading.RLock()
        # Thread-safe: Each instance gets its own configuration
        # avoiding the singleton pattern mentioned in the PR comments
    
    def execute_concurrent(
        self, 
        tasks: List[Tuple[Callable[..., T], Tuple, Dict[str, Any]]], 
        task_names: Optional[List[str]] = None,
        fail_fast: bool = False
    ) -> List[ConcurrentResult]:
        """Execute multiple tasks concurrently in a thread-safe manner."""
        if not tasks:
            return []
        
        task_names = task_names or [f"task_{i}" for i in range(len(tasks))]
        results = []
        
        # Thread-safe: Use local configuration instead of modifying global state
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._execute_task, func, args, kwargs, task_names[i]): i
                for i, (func, args, kwargs) in enumerate(tasks)
            }
            
            # Collect results
            for future in as_completed(future_to_task):
                try:
                    result = future.result(timeout=self.timeout)
                    results.append(result)
                    
                    if fail_fast and not result.success:
                        # Cancel remaining tasks
                        for remaining_future in future_to_task:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
                        
                except Exception as e:
                    task_index = future_to_task[future]
                    task_name = task_names[task_index]
                    error_result = ConcurrentResult(
                        success=False,
                        error=e,
                        metadata={"task_name": task_name}
                    )
                    results.append(error_result)
        
        return results
    
    def _execute_task(self, func: Callable, args: Tuple, kwargs: Dict, task_name: str) -> ConcurrentResult:
        """Execute a single task with error handling and timing."""
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            return ConcurrentResult(
                success=True,
                result=result,
                duration=duration,
                metadata={"task_name": task_name}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            log.debug("Task '%s' failed after %.2fs: %s", task_name, duration, e)
            
            return ConcurrentResult(
                success=False,
                error=e,
                duration=duration,
                metadata={"task_name": task_name}
            )


class ConcurrentLayerDownloader:
    """Thread-safe downloader for REST API layers."""
    
    def __init__(self, max_workers: int = 5, timeout: float = 300.0):
        # Thread-safe: Each instance has its own manager instead of shared singleton
        self.manager = ConcurrentDownloadManager(max_workers, timeout)
    
    def download_layers_concurrent(
        self, 
        handler,  # RestApiDownloadHandler instance
        layers_info: List[Dict[str, Any]],
        fail_fast: bool = False
    ) -> List[ConcurrentResult]:
        """Download multiple layers concurrently in a thread-safe manner."""
        if not layers_info:
            return []
        
        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []
        
        for layer_info in layers_info:
            layer_name = layer_info.get("name", f"layer_{layer_info.get('id', 'unknown')}")
            task_names.append(f"layer_{layer_name}")
            
            # Create task tuple: (function, args, kwargs)
            # Thread-safe: Each task gets a copy of layer_info
            task = (
                handler._fetch_layer_data,
                (layer_info.copy(),),  # Use copy to avoid shared state
                {"layer_metadata_from_service": layer_info.get("metadata")}
            )
            tasks.append(task)
        
        log.info("Starting concurrent download of %d layers", len(layers_info))
        return self.manager.execute_concurrent(tasks, task_names, fail_fast)


# Thread-safe factory functions instead of global singletons
def create_layer_downloader(max_workers: int = 5, timeout: float = 300.0) -> ConcurrentLayerDownloader:
    """Create a new thread-safe layer downloader instance."""
    return ConcurrentLayerDownloader(max_workers, timeout)


def create_download_manager(max_workers: Optional[int] = None, timeout: Optional[float] = None) -> ConcurrentDownloadManager:
    """Create a new thread-safe download manager instance."""
    return ConcurrentDownloadManager(max_workers, timeout)


# For backward compatibility, provide factory functions with sensible defaults
def get_layer_downloader() -> ConcurrentLayerDownloader:
    """Get a new layer downloader instance (thread-safe)."""
    return create_layer_downloader()


def get_collection_downloader() -> ConcurrentLayerDownloader:
    """Get a new collection downloader instance (thread-safe)."""
    return create_layer_downloader(max_workers=3, timeout=600.0)


def get_file_downloader() -> ConcurrentLayerDownloader:
    """Get a new file downloader instance (thread-safe)."""
    return create_layer_downloader(max_workers=4, timeout=1800.0)


@contextmanager
def concurrent_download_manager(max_workers: Optional[int] = None, 
                              timeout: Optional[float] = None):
    """Context manager for thread-safe concurrent download operations."""
    manager = ConcurrentDownloadManager(max_workers, timeout)
    try:
        yield manager
    finally:
        # Cleanup if needed
        pass