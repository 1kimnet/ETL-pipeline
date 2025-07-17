"""Thread-safe concurrent processing utilities for ETL pipeline operations."""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar, Union
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
    """Thread-safe manager for concurrent download operations with configurable parameters."""
    
    def __init__(self, default_max_workers: Optional[int] = None):
        """Initialize with optional default worker count."""
        self.default_max_workers = default_max_workers or 5
        self._lock = threading.RLock()
        log.info("Initialized ConcurrentDownloadManager with default max_workers=%d", self.default_max_workers)
    
    def execute_concurrent(
        self, 
        tasks: List[Tuple[Callable[..., T], Tuple, Dict[str, Any]]], 
        task_names: Optional[List[str]] = None,
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Execute multiple tasks concurrently with configurable worker count.
        
        Args:
            tasks: List of (function, args, kwargs) tuples to execute
            task_names: Optional names for tasks (for logging)
            fail_fast: If True, stop execution on first failure
            max_workers: Override for maximum number of workers (thread-safe)
            
        Returns:
            List of ConcurrentResult objects
        """
        if not tasks:
            return []
        
        # Use provided max_workers or default, ensuring thread safety
        workers = max_workers if max_workers is not None else self.default_max_workers
        task_names = task_names or [f"task_{i}" for i in range(len(tasks))]
        
        log.info("Starting concurrent execution of %d tasks with %d workers", len(tasks), workers)
        
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_task = {}
            for i, (func, args, kwargs) in enumerate(tasks):
                future = executor.submit(self._execute_task, func, args, kwargs, task_names[i])
                future_to_task[future] = (i, task_names[i])
            
            # Collect results as they complete
            for future in as_completed(future_to_task):
                task_index, task_name = future_to_task[future]
                try:
                    result = future.result()
                    results.append((task_index, result))
                    
                    if not result.success and fail_fast:
                        log.warning("Task '%s' failed, stopping execution (fail_fast=True)", task_name)
                        # Cancel remaining futures
                        for remaining_future in future_to_task:
                            if remaining_future != future:
                                remaining_future.cancel()
                        break
                        
                except Exception as e:
                    # This shouldn't happen since _execute_task handles exceptions
                    error_result = ConcurrentResult(
                        success=False,
                        error=e,
                        metadata={"task_name": task_name}
                    )
                    results.append((task_index, error_result))
                    log.error("Unexpected error in task '%s': %s", task_name, e)
        
        # Sort results by original task order
        results.sort(key=lambda x: x[0])
        final_results = [result for _, result in results]
        
        # Log completion statistics
        successful = sum(1 for r in final_results if r.success)
        log.info("Concurrent execution completed: %d/%d successful", successful, len(final_results))
        
        return final_results
    
    def _execute_task(self, func: Callable, args: Tuple, kwargs: Dict, task_name: str) -> ConcurrentResult:
        """Execute a single task with error handling and timing."""
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            log.debug("Task '%s' completed successfully in %.2fs", task_name, duration)
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
    """Specialized downloader for REST API layers with configurable concurrency."""
    
    def __init__(self, default_max_workers: int = 5):
        self.manager = ConcurrentDownloadManager(default_max_workers)
    
    def download_layers_concurrent(
        self, 
        handler,  # RestApiDownloadHandler instance
        layers_info: List[Dict[str, Any]],
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Download multiple layers concurrently with configurable worker count.
        
        Args:
            handler: RestApiDownloadHandler instance
            layers_info: List of layer information dictionaries
            fail_fast: If True, stop on first failure
            max_workers: Override for maximum number of workers
            
        Returns:
            List of ConcurrentResult objects
        """
        if not layers_info:
            return []
        
        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []
        
        for layer_info in layers_info:
            layer_name = layer_info.get("name", f"layer_{layer_info.get('id', 'unknown')}")
            task_names.append(f"layer_{layer_name}")
            
            # Create task tuple: (function, args, kwargs)
            task = (
                handler._fetch_layer_data,
                (layer_info,),
                {"layer_metadata_from_service": layer_info.get("metadata")}
            )
            tasks.append(task)
        
        log.info("Starting concurrent download of %d layers with max_workers=%s", 
                len(layers_info), max_workers or self.manager.default_max_workers)
        
        return self.manager.execute_concurrent(tasks, task_names, fail_fast, max_workers)


class ConcurrentCollectionDownloader:
    """Specialized downloader for OGC API collections with configurable concurrency."""
    
    def __init__(self, default_max_workers: int = 3):
        self.manager = ConcurrentDownloadManager(default_max_workers)
    
    def download_collections_concurrent(
        self,
        handler,  # OgcApiDownloadHandler instance
        collections: List[Dict[str, Any]],
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Download multiple collections concurrently with configurable worker count.
        
        Args:
            handler: OgcApiDownloadHandler instance
            collections: List of collection information dictionaries
            fail_fast: If True, stop on first failure
            max_workers: Override for maximum number of workers
            
        Returns:
            List of ConcurrentResult objects
        """
        if not collections:
            return []
        
        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []
        
        for collection in collections:
            collection_id = collection.get("id", "unknown")
            task_names.append(f"collection_{collection_id}")
            
            # Create task tuple: (function, args, kwargs)
            task = (
                handler._fetch_collection,
                (collection,),
                {}
            )
            tasks.append(task)
        
        log.info("Starting concurrent download of %d collections with max_workers=%s", 
                len(collections), max_workers or self.manager.default_max_workers)
        
        return self.manager.execute_concurrent(tasks, task_names, fail_fast, max_workers)


class ConcurrentFileDownloader:
    """Specialized downloader for file downloads with configurable concurrency."""
    
    def __init__(self, default_max_workers: int = 4):
        self.manager = ConcurrentDownloadManager(default_max_workers)
    
    def download_files_concurrent(
        self,
        handler,  # FileDownloadHandler instance
        file_stems: List[str],
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Download multiple files concurrently with configurable worker count.
        
        Args:
            handler: FileDownloadHandler instance
            file_stems: List of file stem identifiers
            fail_fast: If True, stop on first failure
            max_workers: Override for maximum number of workers
            
        Returns:
            List of ConcurrentResult objects
        """
        if not file_stems:
            return []
        
        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []
        
        for file_stem in file_stems:
            task_names.append(f"file_{file_stem}")
            
            # Create task tuple: (function, args, kwargs)
            task = (
                handler._download_single_file_stem,
                (file_stem,),
                {}
            )
            tasks.append(task)
        
        log.info("Starting concurrent download of %d files with max_workers=%s", 
                len(file_stems), max_workers or self.manager.default_max_workers)
        
        return self.manager.execute_concurrent(tasks, task_names, fail_fast, max_workers)


@contextmanager
def concurrent_download_manager(max_workers: Optional[int] = None) -> Generator[ConcurrentDownloadManager, None, None]:
    """Context manager for concurrent download operations with configurable worker count."""
    manager = ConcurrentDownloadManager(max_workers)
    try:
        yield manager
    finally:
        # Cleanup if needed
        pass


# Global instances for easy access (thread-safe)
_layer_downloader = None
_collection_downloader = None  
_file_downloader = None
_global_lock = threading.RLock()


def get_layer_downloader() -> ConcurrentLayerDownloader:
    """Get global layer downloader instance (thread-safe)."""
    global _layer_downloader
    with _global_lock:
        if _layer_downloader is None:
            _layer_downloader = ConcurrentLayerDownloader()
        return _layer_downloader


def get_collection_downloader() -> ConcurrentCollectionDownloader:
    """Get global collection downloader instance (thread-safe)."""
    global _collection_downloader
    with _global_lock:
        if _collection_downloader is None:
            _collection_downloader = ConcurrentCollectionDownloader()
        return _collection_downloader


def get_file_downloader() -> ConcurrentFileDownloader:
    """Get global file downloader instance (thread-safe)."""
    global _file_downloader
    with _global_lock:
        if _file_downloader is None:
            _file_downloader = ConcurrentFileDownloader()
        return _file_downloader


# Configuration utility functions
def configure_concurrent_downloads(
    layer_workers: Optional[int] = None,
    collection_workers: Optional[int] = None,
    file_workers: Optional[int] = None
) -> None:
    """Configure global concurrent downloader settings (thread-safe)."""
    global _layer_downloader, _collection_downloader, _file_downloader
    
    with _global_lock:
        if layer_workers is not None:
            _layer_downloader = ConcurrentLayerDownloader(layer_workers)
            log.info("Configured layer downloader with %d workers", layer_workers)
            
        if collection_workers is not None:
            _collection_downloader = ConcurrentCollectionDownloader(collection_workers)
            log.info("Configured collection downloader with %d workers", collection_workers)
            
        if file_workers is not None:
            _file_downloader = ConcurrentFileDownloader(file_workers)
            log.info("Configured file downloader with %d workers", file_workers)