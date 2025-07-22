"""Thread-safe concurrent processing utilities for ETL pipeline operations."""
from __future__ import annotations

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar
from dataclasses import dataclass, field
from pathlib import Path

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


@dataclass
class ConcurrentConfig:
    """Thread-safe configuration for concurrent operations."""
    max_workers: int = 5
    timeout: Optional[float] = None
    fail_fast: bool = False
    
    def copy(self) -> 'ConcurrentConfig':
        """Create a copy of the configuration."""
        return ConcurrentConfig(
            max_workers=self.max_workers,
            timeout=self.timeout,
            fail_fast=self.fail_fast
        )


class ThreadSafeConcurrentDownloader:
    """Thread-safe concurrent downloader that accepts configuration parameters."""
    
    def __init__(self, default_config: Optional[ConcurrentConfig] = None):
        self.default_config = default_config or ConcurrentConfig()
        self._lock = threading.RLock()
    
    def download_layers_concurrent(
        self, 
        handler,  # RestApiDownloadHandler instance
        layers_info: List[Dict[str, Any]],
        config: Optional[ConcurrentConfig] = None
    ) -> List[ConcurrentResult]:
        """Download multiple layers concurrently with thread-safe configuration."""
        if not layers_info:
            return []
        
        # Use provided config or default
        effective_config = config or self.default_config.copy()
        
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
        
        log.info("Starting concurrent download of %d layers with %d workers", 
                len(layers_info), effective_config.max_workers)
        
        return self._execute_concurrent_tasks(tasks, task_names, effective_config)
    
    def download_collections_concurrent(
        self,
        handler,  # OgcApiDownloadHandler instance
        collections: List[Dict[str, Any]],
        config: Optional[ConcurrentConfig] = None
    ) -> List[ConcurrentResult]:
        """Download multiple collections concurrently with thread-safe configuration."""
        if not collections:
            return []
        
        # Use provided config or default
        effective_config = config or self.default_config.copy()
        
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
        
        log.info("Starting concurrent download of %d collections with %d workers", 
                len(collections), effective_config.max_workers)
        
        return self._execute_concurrent_tasks(tasks, task_names, effective_config)
    
    def download_files_concurrent(
        self,
        handler,  # FileDownloadHandler instance
        file_stems: List[str],
        config: Optional[ConcurrentConfig] = None
    ) -> List[ConcurrentResult]:
        """Download multiple files concurrently with thread-safe configuration."""
        if not file_stems:
            return []
        
        # Use provided config or default
        effective_config = config or self.default_config.copy()
        
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
        
        log.info("Starting concurrent download of %d files with %d workers", 
                len(file_stems), effective_config.max_workers)
        
        return self._execute_concurrent_tasks(tasks, task_names, effective_config)
    
    def _execute_concurrent_tasks(
        self,
        tasks: List[Tuple[Callable, Tuple, Dict]],
        task_names: List[str],
        config: ConcurrentConfig
    ) -> List[ConcurrentResult]:
        """Execute tasks concurrently with proper error handling."""
        results = []
        
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            # Submit all tasks
            future_to_task = {}
            for i, (task_func, args, kwargs) in enumerate(tasks):
                future = executor.submit(self._execute_single_task, task_func, args, kwargs, task_names[i])
                future_to_task[future] = (i, task_names[i])
            
            # Collect results
            for future in as_completed(future_to_task, timeout=config.timeout):
                try:
                    result = future.result()
                    results.append(result)
                    
                    if config.fail_fast and not result.success:
                        log.warning("Fail-fast enabled, stopping on first failure")
                        # Cancel remaining futures
                        for remaining_future in future_to_task:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
                        
                except Exception as e:
                    task_index, task_name = future_to_task[future]
                    error_result = ConcurrentResult(
                        success=False,
                        error=e,
                        metadata={"task_name": task_name}
                    )
                    results.append(error_result)
                    log.error("Task %s failed with exception: %s", task_name, e)
        
        # Sort results by original task order
        results.sort(key=lambda r: task_names.index(r.metadata.get("task_name", "")))
        
        successful_count = sum(1 for r in results if r.success)
        log.info("Concurrent execution completed: %d/%d successful", 
                successful_count, len(tasks))
        
        return results
    
    def _execute_single_task(
        self, 
        task_func: Callable, 
        args: Tuple, 
        kwargs: Dict, 
        task_name: str
    ) -> ConcurrentResult:
        """Execute a single task with error handling and timing."""
        start_time = time.time()
        
        try:
            result = task_func(*args, **kwargs)
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


@contextmanager
def concurrent_download_manager(
    max_workers: int = 5,
    timeout: Optional[float] = None,
    fail_fast: bool = False
) -> Generator[ThreadSafeConcurrentDownloader, None, None]:
    """Context manager for thread-safe concurrent download operations."""
    config = ConcurrentConfig(
        max_workers=max_workers,
        timeout=timeout,
        fail_fast=fail_fast
    )
    downloader = ThreadSafeConcurrentDownloader(config)
    try:
        yield downloader
    finally:
        # Cleanup if needed
        pass


# Factory functions for different downloader types
def create_layer_downloader(max_workers: int = 5) -> ThreadSafeConcurrentDownloader:
    """Create a downloader optimized for REST API layers."""
    config = ConcurrentConfig(max_workers=max_workers, timeout=300.0)
    return ThreadSafeConcurrentDownloader(config)


def create_collection_downloader(max_workers: int = 3) -> ThreadSafeConcurrentDownloader:
    """Create a downloader optimized for OGC API collections."""
    config = ConcurrentConfig(max_workers=max_workers, timeout=600.0)
    return ThreadSafeConcurrentDownloader(config)


def create_file_downloader(max_workers: int = 4) -> ThreadSafeConcurrentDownloader:
    """Create a downloader optimized for file downloads."""
    config = ConcurrentConfig(max_workers=max_workers, timeout=1800.0)
    return ThreadSafeConcurrentDownloader(config)