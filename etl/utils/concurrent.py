"""Concurrent processing utilities for ETL pipeline operations.

Uses ONLY standard library and ArcGIS Pro 3.3 bundled libraries.
No external dependencies required.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class ConcurrentResult:
    """Result of a concurrent operation."""

    success: bool
    result: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConcurrentStats:
    """Statistics for concurrent operations."""

    total_tasks: int = 0
    completed_tasks: int = 0
    successful_tasks: int = 0
    failed_tasks: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    max_duration: float = 0.0
    min_duration: float = float("inf")

    def update(self, result: ConcurrentResult):
        """Update statistics with a new result."""
        self.completed_tasks += 1
        self.total_duration += result.duration

        if result.success:
            self.successful_tasks += 1
        else:
            self.failed_tasks += 1

        self.max_duration = max(self.max_duration, result.duration)
        self.min_duration = min(self.min_duration, result.duration)

        if self.completed_tasks > 0:
            self.avg_duration = self.total_duration / self.completed_tasks

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.completed_tasks == 0:
            return 0.0
        return (self.successful_tasks / self.completed_tasks) * 100

    @property
    def is_complete(self) -> bool:
        """Check if all tasks are completed."""
        return self.completed_tasks == self.total_tasks


class ConcurrentDownloadManager:
    """Manages concurrent download operations using only standard library."""

    def __init__(
            self,
            max_workers: Optional[int] = None,
            timeout: Optional[float] = None):
        self.max_workers = max_workers or self._get_optimal_worker_count()
        self.timeout = timeout or 300.0
        self.stats = ConcurrentStats()
        self.lock = threading.RLock()

        log.info(
            "Initialized ConcurrentDownloadManager with %d workers",
            self.max_workers)

    def _get_optimal_worker_count(self) -> int:
        """Determine optimal number of workers based on CPU count."""
        cpu_count = os.cpu_count() or 4
        # Conservative approach: don't exceed 2x CPU count for I/O bound tasks
        return min(max(2, cpu_count), 8)

    def execute_concurrent(
        self,
        tasks: List[Tuple[Callable[..., T], Tuple, Dict[str, Any]]],
        task_names: Optional[List[str]] = None,
        fail_fast: bool = False,
    ) -> List[ConcurrentResult]:
        """
        Execute multiple tasks concurrently using ThreadPoolExecutor.

        Args:
            tasks: List of (function, args, kwargs) tuples
            task_names: Optional names for tasks (for logging)
            fail_fast: If True, stop on first failure

        Returns:
            List of ConcurrentResult objects
        """
        if not tasks:
            return []

        self.stats = ConcurrentStats()
        self.stats.total_tasks = len(tasks)

        task_names = task_names or [f"task_{i}" for i in range(len(tasks))]

        log.info(
            "Starting concurrent execution of %d tasks with %d workers",
            len(tasks),
            self.max_workers,
        )

        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {}
            for i, (func, args, kwargs) in enumerate(tasks):
                future = executor.submit(
                    self._execute_task, func, args, kwargs, task_names[i]
                )
                future_to_task[future] = i

            # Collect results as they complete
            for future in as_completed(future_to_task, timeout=self.timeout):
                task_index = future_to_task[future]

                try:
                    result = future.result()
                    results.append((task_index, result))

                    with self.lock:
                        self.stats.update(result)

                    if fail_fast and not result.success:
                        log.warning(
                            "Fail-fast enabled, cancelling remaining tasks")
                        # Cancel remaining futures
                        for remaining_future in future_to_task:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break

                except Exception as e:
                    # Handle timeout or other executor errors
                    error_result = ConcurrentResult(
                        success=False,
                        error=e,
                        metadata={"task_name": task_names[task_index]},
                    )
                    results.append((task_index, error_result))

                    with self.lock:
                        self.stats.update(error_result)

        # Sort results by original task order
        results.sort(key=lambda x: x[0])
        final_results = [result for _, result in results]

        # Pad with error results for any missing tasks (cancelled, etc.)
        while len(final_results) < len(tasks):
            missing_result = ConcurrentResult(
                success=False,
                error=Exception("Task was cancelled or timed out"),
                metadata={"task_name": f"cancelled_task_{len(final_results)}"},
            )
            final_results.append(missing_result)

        self._log_completion_stats()
        return final_results

    def _execute_task(
        self, func: Callable, args: Tuple, kwargs: Dict, task_name: str
    ) -> ConcurrentResult:
        """Execute a single task with error handling and timing."""
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time

            return ConcurrentResult(
                success=True,
                result=result,
                duration=duration,
                metadata={"task_name": task_name},
            )

        except Exception as e:
            duration = time.time() - start_time
            log.debug(
                "Task '%s' failed after %.2fs: %s",
                task_name,
                duration,
                e)

            return ConcurrentResult(
                success=False,
                error=e,
                duration=duration,
                metadata={"task_name": task_name},
            )

    def _log_completion_stats(self):
        """Log completion statistics."""
        log.info(
            "Concurrent execution completed: %d/%d successful (%.1f%%), "
            "avg=%.2fs, max=%.2fs, total=%.2fs",
            self.stats.successful_tasks,
            self.stats.total_tasks,
            self.stats.success_rate,
            self.stats.avg_duration,
            self.stats.max_duration,
            self.stats.total_duration,
        )


class ConcurrentLayerDownloader:
    """Specialized downloader for REST API layers with concurrent processing."""

    def __init__(self, max_workers: int = 5, timeout: float = 300.0):
        self.manager = ConcurrentDownloadManager(max_workers, timeout)

    def download_layers_concurrent(
        self,
        handler,  # RestApiDownloadHandler instance
        layers_info: List[Dict[str, Any]],
        fail_fast: bool = False,
    ) -> List[ConcurrentResult]:
        """Download multiple layers concurrently."""
        if not layers_info:
            return []

        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []

        for layer_info in layers_info:
            layer_name = layer_info.get(
                "name", f"layer_{layer_info.get('id', 'unknown')}"
            )
            task_names.append(f"layer_{layer_name}")

            # Create task tuple: (function, args, kwargs)
            task = (
                handler._fetch_layer_data,
                (layer_info,),
                {"layer_metadata_from_service": layer_info.get("metadata")},
            )
            tasks.append(task)

        log.info("Starting concurrent download of %d layers", len(layers_info))
        return self.manager.execute_concurrent(tasks, task_names, fail_fast)


class ConcurrentCollectionDownloader:
    """Specialized downloader for OGC API collections with concurrent processing."""

    def __init__(self, max_workers: int = 3, timeout: float = 600.0):
        self.manager = ConcurrentDownloadManager(max_workers, timeout)

    def download_collections_concurrent(
        self,
        handler,  # OgcApiDownloadHandler instance
        collections: List[Dict[str, Any]],
        fail_fast: bool = False,
    ) -> List[ConcurrentResult]:
        """Download multiple collections concurrently."""
        if not collections:
            return []

        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []

        for collection in collections:
            collection_id = collection.get("id", "unknown")
            task_names.append(f"collection_{collection_id}")

            # Create task tuple: (function, args, kwargs)
            task = (handler._fetch_collection, (collection,), {})
            tasks.append(task)

        log.info(
            "Starting concurrent download of %d collections",
            len(collections))
        return self.manager.execute_concurrent(tasks, task_names, fail_fast)


class ConcurrentFileDownloader:
    """Specialized downloader for file downloads with concurrent processing."""

    def __init__(self, max_workers: int = 4, timeout: float = 1800.0):
        self.manager = ConcurrentDownloadManager(max_workers, timeout)

    def download_files_concurrent(
        self,
        handler,  # FileDownloadHandler instance
        file_stems: List[str],
        fail_fast: bool = False,
    ) -> List[ConcurrentResult]:
        """Download multiple files concurrently."""
        if not file_stems:
            return []

        # Prepare tasks for concurrent execution
        tasks = []
        task_names = []

        for file_stem in file_stems:
            task_names.append(f"file_{file_stem}")

            # Create task tuple: (function, args, kwargs)
            task = (handler._download_single_file_stem, (file_stem,), {})
            tasks.append(task)

        log.info("Starting concurrent download of %d files", len(file_stems))
        return self.manager.execute_concurrent(tasks, task_names, fail_fast)


@contextmanager
def concurrent_download_manager(
    max_workers: Optional[int] = None, timeout: Optional[float] = None
) -> Generator[ConcurrentDownloadManager, None, None]:
    """Context manager for concurrent download operations."""
    manager = ConcurrentDownloadManager(max_workers, timeout)
    try:
        yield manager
    finally:
        # Cleanup if needed
        pass


# Global instances for easy access
_layer_downloader = None
_collection_downloader = None
_file_downloader = None


def get_layer_downloader() -> ConcurrentLayerDownloader:
    """Get global layer downloader instance."""
    global _layer_downloader
    if _layer_downloader is None:
        _layer_downloader = ConcurrentLayerDownloader()
    return _layer_downloader


def get_collection_downloader() -> ConcurrentCollectionDownloader:
    """Get global collection downloader instance."""
    global _collection_downloader
    if _collection_downloader is None:
        _collection_downloader = ConcurrentCollectionDownloader()
    return _collection_downloader


def get_file_downloader() -> ConcurrentFileDownloader:
    """Get global file downloader instance."""
    global _file_downloader
    if _file_downloader is None:
        _file_downloader = ConcurrentFileDownloader()
    return _file_downloader
