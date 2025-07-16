"""Advanced performance optimization for ETL pipeline operations."""
from __future__ import annotations

import gc
import logging
import psutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Tuple, Union
from queue import Queue, Empty
import resource
import sys

log = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations."""
    operation_name: str
    start_time: float
    end_time: float
    duration: float
    memory_before: float
    memory_after: float
    memory_peak: float
    cpu_percent: float
    worker_count: int
    items_processed: int = 0
    bytes_processed: int = 0
    
    @property
    def throughput_items_per_sec(self) -> float:
        """Calculate items processed per second."""
        return self.items_processed / self.duration if self.duration > 0 else 0
    
    @property
    def throughput_mb_per_sec(self) -> float:
        """Calculate MB processed per second."""
        mb_processed = self.bytes_processed / (1024 * 1024)
        return mb_processed / self.duration if self.duration > 0 else 0
    
    @property
    def memory_efficiency(self) -> float:
        """Calculate memory efficiency (items per MB of memory used)."""
        memory_used = self.memory_peak - self.memory_before
        return self.items_processed / memory_used if memory_used > 0 else 0


@dataclass
class SystemResources:
    """Current system resource usage."""
    cpu_percent: float
    memory_percent: float
    memory_available_gb: float
    disk_free_gb: float
    network_connections: int
    
    @property
    def is_under_pressure(self) -> bool:
        """Check if system is under resource pressure."""
        return (
            self.cpu_percent > 80 or
            self.memory_percent > 85 or
            self.memory_available_gb < 0.5
        )
    
    @property
    def pressure_level(self) -> str:
        """Get system pressure level."""
        if self.cpu_percent > 90 or self.memory_percent > 95:
            return "critical"
        elif self.cpu_percent > 80 or self.memory_percent > 85:
            return "high"
        elif self.cpu_percent > 60 or self.memory_percent > 70:
            return "moderate"
        return "low"


class MemoryOptimizer:
    """Memory optimization utilities."""
    
    def __init__(self):
        self.memory_threshold = 0.85  # 85% memory usage threshold
        self.gc_threshold = 0.90      # 90% memory usage triggers aggressive GC
        
    def get_memory_usage(self) -> float:
        """Get current memory usage as percentage."""
        return psutil.virtual_memory().percent / 100.0
    
    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        return psutil.Process().memory_info().rss / (1024 * 1024)
    
    @contextmanager
    def memory_monitoring(self, operation_name: str):
        """Context manager for monitoring memory usage."""
        initial_memory = self.get_memory_usage_mb()
        peak_memory = initial_memory
        
        def monitor_memory():
            nonlocal peak_memory
            current = self.get_memory_usage_mb()
            if current > peak_memory:
                peak_memory = current
        
        # Start memory monitoring
        monitor_thread = threading.Thread(target=lambda: monitor_memory(), daemon=True)
        monitor_thread.start()
        
        try:
            yield lambda: peak_memory
        finally:
            final_memory = self.get_memory_usage_mb()
            memory_delta = final_memory - initial_memory
            
            log.debug(
                "Memory usage for %s: initial=%.1fMB, peak=%.1fMB, final=%.1fMB, delta=%.1fMB",
                operation_name, initial_memory, peak_memory, final_memory, memory_delta
            )
            
            # Trigger garbage collection if memory usage is high
            if self.get_memory_usage() > self.gc_threshold:
                log.info("🧹 High memory usage detected, triggering garbage collection")
                collected = gc.collect()
                log.debug("Garbage collection freed %d objects", collected)
    
    def optimize_memory_usage(self) -> None:
        """Optimize current memory usage."""
        current_usage = self.get_memory_usage()
        
        if current_usage > self.memory_threshold:
            log.info("🧹 Memory usage at %.1f%%, optimizing...", current_usage * 100)
            
            # Force garbage collection
            collected = gc.collect()
            
            # Clear caches if available
            self._clear_internal_caches()
            
            new_usage = self.get_memory_usage()
            reduction = (current_usage - new_usage) * 100
            
            log.info(
                "✅ Memory optimization complete: %.1f%% → %.1f%% (reduced by %.1f%%)",
                current_usage * 100, new_usage * 100, reduction
            )
    
    def _clear_internal_caches(self) -> None:
        """Clear internal caches to free memory."""
        try:
            # Clear HTTP connection pools
            import requests
            requests.Session().close()
        except ImportError:
            pass
        
        # Clear any other internal caches
        if hasattr(sys, '_clear_type_cache'):
            sys._clear_type_cache()


class ConcurrencyOptimizer:
    """Optimizes concurrency based on system resources and workload characteristics."""
    
    def __init__(self):
        self.cpu_count = psutil.cpu_count()
        self.memory_gb = psutil.virtual_memory().total / (1024**3)
        self.optimal_workers_cache: Dict[str, int] = {}
        
    def calculate_optimal_workers(
        self,
        operation_type: str,
        workload_size: int,
        item_complexity: str = "medium",
        memory_per_item_mb: float = 10.0
    ) -> int:
        """Calculate optimal number of workers for a given operation."""
        
        # Check cache first
        cache_key = f"{operation_type}_{workload_size}_{item_complexity}_{memory_per_item_mb}"
        if cache_key in self.optimal_workers_cache:
            return self.optimal_workers_cache[cache_key]
        
        # Get current system resources
        resources = self._get_current_resources()
        
        # Base calculations
        if operation_type == "network_io":
            # Network I/O can be highly concurrent
            base_workers = min(workload_size, self.cpu_count * 4)
        elif operation_type == "cpu_intensive":
            # CPU-intensive tasks should match CPU cores
            base_workers = min(workload_size, self.cpu_count)
        elif operation_type == "mixed":
            # Mixed workload - balance between I/O and CPU
            base_workers = min(workload_size, self.cpu_count * 2)
        else:
            # Default conservative approach
            base_workers = min(workload_size, self.cpu_count)
        
        # Adjust for item complexity
        complexity_multiplier = {
            "low": 1.5,
            "medium": 1.0,
            "high": 0.7,
            "very_high": 0.5
        }.get(item_complexity, 1.0)
        
        base_workers = int(base_workers * complexity_multiplier)
        
        # Adjust for memory constraints
        total_memory_needed = workload_size * memory_per_item_mb / 1024  # GB
        if total_memory_needed > resources.memory_available_gb * 0.8:
            # Memory-constrained, reduce workers
            memory_limited_workers = int(resources.memory_available_gb * 0.8 * 1024 / memory_per_item_mb)
            base_workers = min(base_workers, memory_limited_workers)
        
        # Adjust for current system pressure
        if resources.pressure_level == "critical":
            base_workers = max(1, base_workers // 4)
        elif resources.pressure_level == "high":
            base_workers = max(1, base_workers // 2)
        elif resources.pressure_level == "moderate":
            base_workers = max(1, int(base_workers * 0.75))
        
        # Ensure minimum and maximum bounds
        optimal_workers = max(1, min(base_workers, 20))  # Max 20 workers
        
        # Cache the result
        self.optimal_workers_cache[cache_key] = optimal_workers
        
        log.debug(
            "Calculated optimal workers for %s: %d (workload=%d, complexity=%s, memory=%.1fMB/item, pressure=%s)",
            operation_type, optimal_workers, workload_size, item_complexity, memory_per_item_mb, resources.pressure_level
        )
        
        return optimal_workers
    
    def _get_current_resources(self) -> SystemResources:
        """Get current system resource usage."""
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return SystemResources(
            cpu_percent=psutil.cpu_percent(interval=0.1),
            memory_percent=memory.percent,
            memory_available_gb=memory.available / (1024**3),
            disk_free_gb=disk.free / (1024**3),
            network_connections=len(psutil.net_connections())
        )
    
    def adaptive_worker_adjustment(
        self,
        current_workers: int,
        performance_metrics: List[PerformanceMetrics],
        target_cpu_usage: float = 0.75
    ) -> int:
        """Adaptively adjust worker count based on performance metrics."""
        
        if not performance_metrics:
            return current_workers
        
        # Analyze recent performance
        recent_metrics = performance_metrics[-5:]  # Last 5 operations
        avg_cpu = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics)
        avg_throughput = sum(m.throughput_items_per_sec for m in recent_metrics) / len(recent_metrics)
        
        # Get current system state
        resources = self._get_current_resources()
        
        # Adjustment logic
        if resources.is_under_pressure:
            # System under pressure, reduce workers
            new_workers = max(1, int(current_workers * 0.8))
            log.info("🔻 System under pressure, reducing workers: %d → %d", current_workers, new_workers)
        elif avg_cpu < target_cpu_usage * 0.7 and avg_throughput > 0:
            # CPU underutilized, can increase workers
            new_workers = min(current_workers + 1, 20)
            log.info("🔺 CPU underutilized, increasing workers: %d → %d", current_workers, new_workers)
        elif avg_cpu > target_cpu_usage * 1.2:
            # CPU overutilized, reduce workers
            new_workers = max(1, current_workers - 1)
            log.info("🔻 CPU overutilized, reducing workers: %d → %d", current_workers, new_workers)
        else:
            # Performance is stable, no change needed
            new_workers = current_workers
        
        return new_workers


class AdaptiveExecutor:
    """Adaptive thread/process pool executor that optimizes performance dynamically."""
    
    def __init__(self, operation_type: str = "mixed"):
        self.operation_type = operation_type
        self.concurrency_optimizer = ConcurrencyOptimizer()
        self.memory_optimizer = MemoryOptimizer()
        self.performance_history: List[PerformanceMetrics] = []
        self.current_executor: Optional[Union[ThreadPoolExecutor, ProcessPoolExecutor]] = None
        self.current_workers = 1
        
    def execute_workload(
        self,
        tasks: List[Callable],
        task_args: List[Tuple] = None,
        workload_name: str = "unknown",
        memory_per_item_mb: float = 10.0,
        use_processes: bool = False
    ) -> List[Any]:
        """Execute a workload with adaptive optimization."""
        
        if not tasks:
            return []
        
        task_args = task_args or [() for _ in tasks]
        workload_size = len(tasks)
        
        log.info("🚀 Starting adaptive workload execution: %s (%d tasks)", workload_name, workload_size)
        
        # Calculate optimal worker count
        optimal_workers = self.concurrency_optimizer.calculate_optimal_workers(
            operation_type=self.operation_type,
            workload_size=workload_size,
            item_complexity="medium",
            memory_per_item_mb=memory_per_item_mb
        )
        
        # Adjust based on performance history
        if self.performance_history:
            optimal_workers = self.concurrency_optimizer.adaptive_worker_adjustment(
                current_workers=self.current_workers,
                performance_metrics=self.performance_history
            )
        
        self.current_workers = optimal_workers
        
        # Start performance monitoring
        start_time = time.time()
        start_memory = self.memory_optimizer.get_memory_usage_mb()
        start_cpu = psutil.cpu_percent()
        
        results = []
        
        with self.memory_optimizer.memory_monitoring(workload_name) as get_peak_memory:
            try:
                # Choose executor type
                executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
                
                with executor_class(max_workers=optimal_workers) as executor:
                    self.current_executor = executor
                    
                    # Submit all tasks
                    future_to_task = {
                        executor.submit(task, *args): (i, task)
                        for i, (task, args) in enumerate(zip(tasks, task_args))
                    }
                    
                    # Collect results with progress monitoring
                    for i, future in enumerate(as_completed(future_to_task)):
                        try:
                            result = future.result()
                            results.append(result)
                            
                            # Log progress every 10% or every 10 items
                            if (i + 1) % max(1, workload_size // 10) == 0 or (i + 1) % 10 == 0:
                                progress = (i + 1) / workload_size * 100
                                log.debug("Progress: %.1f%% (%d/%d tasks completed)", progress, i + 1, workload_size)
                                
                                # Check for memory pressure and optimize if needed
                                if self.memory_optimizer.get_memory_usage() > 0.85:
                                    self.memory_optimizer.optimize_memory_usage()
                        
                        except Exception as e:
                            log.error("Task failed: %s", e)
                            results.append(None)  # Placeholder for failed task
            
            finally:
                self.current_executor = None
        
        # Calculate performance metrics
        end_time = time.time()
        duration = end_time - start_time
        end_memory = self.memory_optimizer.get_memory_usage_mb()
        peak_memory = get_peak_memory()
        end_cpu = psutil.cpu_percent()
        
        metrics = PerformanceMetrics(
            operation_name=workload_name,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            memory_before=start_memory,
            memory_after=end_memory,
            memory_peak=peak_memory,
            cpu_percent=(start_cpu + end_cpu) / 2,
            worker_count=optimal_workers,
            items_processed=len([r for r in results if r is not None])
        )
        
        # Store performance history (keep last 20 entries)
        self.performance_history.append(metrics)
        if len(self.performance_history) > 20:
            self.performance_history.pop(0)
        
        # Log performance summary
        log.info(
            "✅ Workload completed: %s - %d/%d tasks succeeded in %.2fs (%.1f items/sec) using %d workers",
            workload_name,
            metrics.items_processed,
            workload_size,
            duration,
            metrics.throughput_items_per_sec,
            optimal_workers
        )
        
        return results
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary statistics."""
        if not self.performance_history:
            return {"message": "No performance data available"}
        
        recent_metrics = self.performance_history[-10:]  # Last 10 operations
        
        return {
            "total_operations": len(self.performance_history),
            "recent_operations": len(recent_metrics),
            "average_duration": sum(m.duration for m in recent_metrics) / len(recent_metrics),
            "average_throughput": sum(m.throughput_items_per_sec for m in recent_metrics) / len(recent_metrics),
            "average_workers": sum(m.worker_count for m in recent_metrics) / len(recent_metrics),
            "memory_efficiency": sum(m.memory_efficiency for m in recent_metrics) / len(recent_metrics),
            "current_workers": self.current_workers
        }


class BatchProcessor:
    """Intelligent batch processing for large datasets."""
    
    def __init__(self, max_memory_mb: float = 512):
        self.max_memory_mb = max_memory_mb
        self.memory_optimizer = MemoryOptimizer()
        
    def calculate_optimal_batch_size(
        self,
        total_items: int,
        item_size_mb: float,
        processing_overhead: float = 1.5
    ) -> int:
        """Calculate optimal batch size based on memory constraints."""
        
        # Calculate how many items fit in memory
        available_memory = self.max_memory_mb / processing_overhead
        items_per_batch = int(available_memory / item_size_mb)
        
        # Ensure batch size is reasonable
        min_batch = 1
        max_batch = min(1000, total_items)  # Never more than 1000 items per batch
        
        optimal_batch = max(min_batch, min(items_per_batch, max_batch))
        
        log.debug(
            "Calculated optimal batch size: %d items (%.1fMB per item, %.1fMB available)",
            optimal_batch, item_size_mb, available_memory
        )
        
        return optimal_batch
    
    def process_in_batches(
        self,
        items: List[Any],
        processor_func: Callable[[List[Any]], List[Any]],
        item_size_mb: float = 1.0,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> List[Any]:
        """Process items in optimal batches to manage memory usage."""
        
        if not items:
            return []
        
        total_items = len(items)
        batch_size = self.calculate_optimal_batch_size(total_items, item_size_mb)
        
        log.info("🔄 Processing %d items in batches of %d", total_items, batch_size)
        
        results = []
        processed_count = 0
        
        for i in range(0, total_items, batch_size):
            batch = items[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_items + batch_size - 1) // batch_size
            
            log.debug("Processing batch %d/%d (%d items)", batch_num, total_batches, len(batch))
            
            # Process batch with memory monitoring
            with self.memory_optimizer.memory_monitoring(f"batch_{batch_num}"):
                try:
                    batch_results = processor_func(batch)
                    results.extend(batch_results)
                    processed_count += len(batch)
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(processed_count, total_items)
                    
                    # Optimize memory after each batch
                    if self.memory_optimizer.get_memory_usage() > 0.75:
                        self.memory_optimizer.optimize_memory_usage()
                
                except Exception as e:
                    log.error("Batch %d/%d failed: %s", batch_num, total_batches, e)
                    # Continue with next batch rather than failing entirely
                    continue
        
        log.info("✅ Batch processing complete: %d items processed", len(results))
        return results


# Global instances for easy access
_memory_optimizer = MemoryOptimizer()
_concurrency_optimizer = ConcurrencyOptimizer()


def get_memory_optimizer() -> MemoryOptimizer:
    """Get global memory optimizer instance."""
    return _memory_optimizer


def get_concurrency_optimizer() -> ConcurrencyOptimizer:
    """Get global concurrency optimizer instance."""
    return _concurrency_optimizer


@contextmanager
def performance_optimization():
    """Context manager for automatic performance optimization."""
    memory_opt = get_memory_optimizer()
    
    # Initial optimization
    memory_opt.optimize_memory_usage()
    
    try:
        yield
    finally:
        # Final optimization
        memory_opt.optimize_memory_usage()


def optimize_for_production():
    """Apply production-ready performance optimizations."""
    
    log.info("🚀 Applying production performance optimizations...")
    
    # Garbage collection tuning
    import gc
    gc.set_threshold(700, 10, 10)  # More aggressive garbage collection
    
    # Memory optimization
    memory_opt = get_memory_optimizer()
    memory_opt.optimize_memory_usage()
    
    # Log system resources
    resources = ConcurrencyOptimizer()._get_current_resources()
    log.info(
        "📊 System resources: CPU=%.1f%%, Memory=%.1f%% (%.1fGB available), Disk=%.1fGB free",
        resources.cpu_percent,
        resources.memory_percent,
        resources.memory_available_gb,
        resources.disk_free_gb
    )
    
    log.info("✅ Production optimizations applied")