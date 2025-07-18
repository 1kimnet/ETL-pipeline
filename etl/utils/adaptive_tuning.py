"""Adaptive performance tuning system for ETL pipeline."""
from __future__ import annotations

import logging
import time
import threading
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import statistics
import psutil

from .performance_optimizer import PerformanceMetrics, SystemResources
from .intelligent_cache import get_global_cache
from ..exceptions import SystemError, ProcessingError

log = logging.getLogger(__name__)


class TuningStrategy(Enum):
    """Available tuning strategies."""
    CONSERVATIVE = "conservative"    # Small, safe adjustments
    AGGRESSIVE = "aggressive"       # Larger adjustments for faster optimization
    BALANCED = "balanced"          # Balance between speed and safety
    EXPERIMENTAL = "experimental"  # Test new optimization strategies


@dataclass
class PerformanceBaseline:
    """Baseline performance metrics for comparison."""
    operation_name: str
    avg_duration: float
    avg_throughput: float
    avg_memory_usage: float
    avg_cpu_usage: float
    success_rate: float
    sample_count: int
    established_at: float
    
    def is_degraded(self, current_metrics: PerformanceMetrics, threshold: float = 0.2) -> bool:
        """Check if current performance is degraded compared to baseline."""
        duration_increase = (current_metrics.duration - self.avg_duration) / self.avg_duration
        throughput_decrease = (self.avg_throughput - current_metrics.throughput_items_per_sec) / self.avg_throughput
        
        return duration_increase > threshold or throughput_decrease > threshold


@dataclass
class TuningAction:
    """Represents a tuning action to be applied."""
    parameter: str
    current_value: Any
    new_value: Any
    reason: str
    confidence: float  # 0.0 to 1.0
    expected_impact: str  # "positive", "negative", "neutral"
    priority: int = 0
    
    def apply(self, config: Dict[str, Any]) -> bool:
        """Apply the tuning action to configuration."""
        try:
            old_value = config.get(self.parameter, self.current_value)
            config[self.parameter] = self.new_value
            
            log.info(
                "🔧 Applied tuning: %s = %s → %s (reason: %s, confidence: %.2f)",
                self.parameter, old_value, self.new_value, self.reason, self.confidence
            )
            return True
        except Exception as e:
            log.error("Failed to apply tuning action for %s: %s", self.parameter, e)
            return False


class AdaptivePerformanceTuner:
    """Adaptive performance tuning engine that learns and optimizes."""
    
    def __init__(
        self,
        strategy: TuningStrategy = TuningStrategy.BALANCED,
        learning_window_size: int = 20,
        min_confidence_threshold: float = 0.6
    ):
        self.strategy = strategy
        self.learning_window_size = learning_window_size
        self.min_confidence_threshold = min_confidence_threshold
        
        # Performance tracking
        self.performance_history: Dict[str, deque] = {}
        self.baselines: Dict[str, PerformanceBaseline] = {}
        self.tuning_history: List[Tuple[TuningAction, float]] = []  # (action, timestamp)
        
        # Tuning parameters and their ranges
        self.tunable_parameters = {
            "concurrent_download_workers": {
                "min": 1, "max": 20, "type": "int",
                "impact": "throughput", "sensitivity": "high"
            },
            "concurrent_collection_workers": {
                "min": 1, "max": 10, "type": "int",
                "impact": "throughput", "sensitivity": "high"
            },
            "concurrent_file_workers": {
                "min": 1, "max": 15, "type": "int",
                "impact": "throughput", "sensitivity": "high"
            },
            "timeout": {
                "min": 10, "max": 300, "type": "float",
                "impact": "reliability", "sensitivity": "medium"
            },
            "max_file_size_mb": {
                "min": 10, "max": 500, "type": "float",
                "impact": "memory", "sensitivity": "medium"
            },
            "retry_attempts": {
                "min": 1, "max": 10, "type": "int",
                "impact": "reliability", "sensitivity": "low"
            },
            "cache_memory_mb": {
                "min": 64, "max": 1024, "type": "float",
                "impact": "memory", "sensitivity": "medium"
            },
            "batch_size": {
                "min": 1, "max": 1000, "type": "int",
                "impact": "throughput", "sensitivity": "medium"
            }
        }
        
        # System monitoring
        self.system_monitor = SystemMonitor()
        
        # Thread safety
        self.lock = threading.RLock()
        
        log.info("Initialized AdaptivePerformanceTuner with %s strategy", strategy.value)
    
    def record_performance(self, metrics: PerformanceMetrics) -> None:
        """Record performance metrics for learning."""
        with self.lock:
            operation = metrics.operation_name
            
            # Initialize history if needed
            if operation not in self.performance_history:
                self.performance_history[operation] = deque(maxlen=self.learning_window_size)
            
            # Add to history
            self.performance_history[operation].append(metrics)
            
            # Update or establish baseline
            self._update_baseline(operation)
            
            # Check if tuning is needed
            if self._should_tune(operation):
                tuning_actions = self._generate_tuning_actions(operation)
                if tuning_actions:
                    log.info("🎯 Generated %d tuning actions for %s", len(tuning_actions), operation)
                    return tuning_actions
    
    def tune_configuration(self, config: Dict[str, Any], operation: str) -> List[TuningAction]:
        """Generate tuning actions for configuration optimization."""
        with self.lock:
            if operation not in self.performance_history:
                log.debug("No performance history for %s, skipping tuning", operation)
                return []
            
            actions = self._generate_tuning_actions(operation)
            
            # Filter actions by confidence
            high_confidence_actions = [
                action for action in actions 
                if action.confidence >= self.min_confidence_threshold
            ]
            
            # Sort by priority and confidence
            high_confidence_actions.sort(key=lambda x: (x.priority, x.confidence), reverse=True)
            
            return high_confidence_actions
    
    def apply_tuning_actions(self, actions: List[TuningAction], config: Dict[str, Any]) -> int:
        """Apply tuning actions to configuration."""
        applied_count = 0
        
        for action in actions:
            if action.apply(config):
                self.tuning_history.append((action, time.time()))
                applied_count += 1
        
        if applied_count > 0:
            log.info("✅ Applied %d tuning actions", applied_count)
        
        return applied_count
    
    def _should_tune(self, operation: str) -> bool:
        """Determine if tuning is needed for an operation."""
        if operation not in self.performance_history:
            return False
        
        history = self.performance_history[operation]
        
        # Need minimum samples
        if len(history) < 5:
            return False
        
        # Check for performance degradation
        if operation in self.baselines:
            baseline = self.baselines[operation]
            recent_metrics = list(history)[-3:]  # Last 3 samples
            
            degraded_count = sum(
                1 for metrics in recent_metrics 
                if baseline.is_degraded(metrics)
            )
            
            return degraded_count >= 2  # 2 out of 3 recent samples degraded
        
        # Check for performance variance (instability)
        durations = [m.duration for m in history]
        if len(durations) >= 5:
            cv = statistics.stdev(durations) / statistics.mean(durations)
            return cv > 0.3  # High coefficient of variation
        
        return False
    
    def _generate_tuning_actions(self, operation: str) -> List[TuningAction]:
        """Generate tuning actions based on performance analysis."""
        actions = []
        
        if operation not in self.performance_history:
            return actions
        
        history = list(self.performance_history[operation])
        recent_metrics = history[-3:]  # Last 3 samples
        
        # Get current system resources
        system_resources = self.system_monitor.get_current_resources()
        
        # Analyze performance patterns
        avg_duration = statistics.mean(m.duration for m in recent_metrics)
        avg_throughput = statistics.mean(m.throughput_items_per_sec for m in recent_metrics)
        avg_cpu = statistics.mean(m.cpu_percent for m in recent_metrics)
        avg_memory = statistics.mean(m.memory_peak for m in recent_metrics)
        
        # Generate actions based on analysis
        actions.extend(self._analyze_concurrency_settings(operation, recent_metrics, system_resources))
        actions.extend(self._analyze_timeout_settings(operation, recent_metrics))
        actions.extend(self._analyze_memory_settings(operation, recent_metrics, system_resources))
        actions.extend(self._analyze_caching_settings(operation, recent_metrics))
        
        return actions
    
    def _analyze_concurrency_settings(
        self, 
        operation: str, 
        metrics: List[PerformanceMetrics], 
        system_resources: SystemResources
    ) -> List[TuningAction]:
        """Analyze and tune concurrency settings."""
        actions = []
        
        avg_cpu = statistics.mean(m.cpu_percent for m in metrics)
        avg_throughput = statistics.mean(m.throughput_items_per_sec for m in metrics)
        avg_workers = statistics.mean(m.worker_count for m in metrics)
        
        # Concurrency tuning based on operation type
        if "download" in operation.lower():
            param = "concurrent_download_workers"
        elif "collection" in operation.lower():
            param = "concurrent_collection_workers"
        elif "file" in operation.lower():
            param = "concurrent_file_workers"
        else:
            return actions
        
        current_workers = int(avg_workers)
        param_config = self.tunable_parameters[param]
        
        # CPU underutilization - increase workers
        if avg_cpu < 50 and not system_resources.is_under_pressure:
            new_workers = min(current_workers + 1, param_config["max"])
            if new_workers > current_workers:
                actions.append(TuningAction(
                    parameter=param,
                    current_value=current_workers,
                    new_value=new_workers,
                    reason=f"CPU underutilized ({avg_cpu:.1f}%), increasing concurrency",
                    confidence=0.8,
                    expected_impact="positive",
                    priority=2
                ))
        
        # CPU overutilization or system pressure - decrease workers
        elif avg_cpu > 85 or system_resources.is_under_pressure:
            new_workers = max(current_workers - 1, param_config["min"])
            if new_workers < current_workers:
                actions.append(TuningAction(
                    parameter=param,
                    current_value=current_workers,
                    new_value=new_workers,
                    reason=f"CPU overutilized ({avg_cpu:.1f}%) or system pressure, reducing concurrency",
                    confidence=0.9,
                    expected_impact="positive",
                    priority=3
                ))
        
        # Low throughput with moderate CPU - try increasing workers
        elif avg_throughput < 1.0 and avg_cpu < 70:
            new_workers = min(current_workers + 2, param_config["max"])
            if new_workers > current_workers:
                actions.append(TuningAction(
                    parameter=param,
                    current_value=current_workers,
                    new_value=new_workers,
                    reason=f"Low throughput ({avg_throughput:.2f}/sec), increasing concurrency",
                    confidence=0.7,
                    expected_impact="positive",
                    priority=2
                ))
        
        return actions
    
    def _analyze_timeout_settings(
        self, 
        operation: str, 
        metrics: List[PerformanceMetrics]
    ) -> List[TuningAction]:
        """Analyze and tune timeout settings."""
        actions = []
        
        # Look for timeout-related patterns in duration
        durations = [m.duration for m in metrics]
        avg_duration = statistics.mean(durations)
        max_duration = max(durations)
        
        # If operations are taking a long time, increase timeout
        if max_duration > 60:  # Operations taking more than 1 minute
            current_timeout = 30  # Default assumption
            new_timeout = min(int(max_duration * 1.5), 300)  # 1.5x max duration, cap at 5 minutes
            
            if new_timeout > current_timeout:
                actions.append(TuningAction(
                    parameter="timeout",
                    current_value=current_timeout,
                    new_value=new_timeout,
                    reason=f"Long operation durations (max: {max_duration:.1f}s), increasing timeout",
                    confidence=0.8,
                    expected_impact="positive",
                    priority=1
                ))
        
        # If operations are consistently fast, can reduce timeout
        elif avg_duration < 5 and max_duration < 10:
            current_timeout = 30
            new_timeout = max(int(max_duration * 2), 10)  # 2x max duration, minimum 10s
            
            if new_timeout < current_timeout:
                actions.append(TuningAction(
                    parameter="timeout",
                    current_value=current_timeout,
                    new_value=new_timeout,
                    reason=f"Fast operations (avg: {avg_duration:.1f}s), reducing timeout",
                    confidence=0.6,
                    expected_impact="neutral",
                    priority=0
                ))
        
        return actions
    
    def _analyze_memory_settings(
        self, 
        operation: str, 
        metrics: List[PerformanceMetrics], 
        system_resources: SystemResources
    ) -> List[TuningAction]:
        """Analyze and tune memory-related settings."""
        actions = []
        
        avg_memory = statistics.mean(m.memory_peak for m in metrics)
        max_memory = max(m.memory_peak for m in metrics)
        
        # High memory usage - reduce batch size or file size limits
        if avg_memory > 1024 or system_resources.memory_percent > 85:
            if "batch_size" in operation.lower():
                actions.append(TuningAction(
                    parameter="batch_size",
                    current_value=100,  # Default assumption
                    new_value=50,
                    reason=f"High memory usage ({avg_memory:.1f}MB), reducing batch size",
                    confidence=0.8,
                    expected_impact="positive",
                    priority=2
                ))
            
            actions.append(TuningAction(
                parameter="max_file_size_mb",
                current_value=100,  # Default assumption
                new_value=50,
                reason=f"High memory usage ({avg_memory:.1f}MB), reducing file size limit",
                confidence=0.7,
                expected_impact="positive",
                priority=1
            ))
        
        # Low memory usage - can increase batch size
        elif avg_memory < 256 and system_resources.memory_percent < 50:
            if "batch" in operation.lower():
                actions.append(TuningAction(
                    parameter="batch_size",
                    current_value=50,
                    new_value=100,
                    reason=f"Low memory usage ({avg_memory:.1f}MB), increasing batch size",
                    confidence=0.6,
                    expected_impact="positive",
                    priority=1
                ))
        
        return actions
    
    def _analyze_caching_settings(
        self, 
        operation: str, 
        metrics: List[PerformanceMetrics]
    ) -> List[TuningAction]:
        """Analyze and tune caching settings."""
        actions = []
        
        # Get cache statistics
        cache = get_global_cache()
        cache_stats = cache.get_stats()
        
        hit_rate = cache_stats["performance"]["hit_rate_percent"]
        memory_utilization = cache_stats["memory_cache"]["utilization_percent"]
        
        # Low cache hit rate - increase cache size
        if hit_rate < 50 and memory_utilization > 80:
            current_cache_mb = cache_stats["memory_cache"]["size_mb"]
            new_cache_mb = min(current_cache_mb * 1.5, 1024)
            
            actions.append(TuningAction(
                parameter="cache_memory_mb",
                current_value=current_cache_mb,
                new_value=new_cache_mb,
                reason=f"Low cache hit rate ({hit_rate:.1f}%), increasing cache size",
                confidence=0.7,
                expected_impact="positive",
                priority=1
            ))
        
        # High cache hit rate but low utilization - can reduce cache size
        elif hit_rate > 90 and memory_utilization < 30:
            current_cache_mb = cache_stats["memory_cache"]["size_mb"]
            new_cache_mb = max(current_cache_mb * 0.8, 64)
            
            actions.append(TuningAction(
                parameter="cache_memory_mb",
                current_value=current_cache_mb,
                new_value=new_cache_mb,
                reason=f"High hit rate ({hit_rate:.1f}%) with low utilization, reducing cache size",
                confidence=0.5,
                expected_impact="neutral",
                priority=0
            ))
        
        return actions
    
    def _update_baseline(self, operation: str) -> None:
        """Update performance baseline for an operation."""
        if operation not in self.performance_history:
            return
        
        history = self.performance_history[operation]
        
        # Need minimum samples to establish baseline
        if len(history) < 10:
            return
        
        # Calculate baseline metrics
        durations = [m.duration for m in history]
        throughputs = [m.throughput_items_per_sec for m in history]
        memory_usage = [m.memory_peak for m in history]
        cpu_usage = [m.cpu_percent for m in history]
        
        # Calculate success rate (assuming all recorded metrics are from successful operations)
        success_rate = 1.0  # Could be enhanced with actual success tracking
        
        baseline = PerformanceBaseline(
            operation_name=operation,
            avg_duration=statistics.mean(durations),
            avg_throughput=statistics.mean(throughputs),
            avg_memory_usage=statistics.mean(memory_usage),
            avg_cpu_usage=statistics.mean(cpu_usage),
            success_rate=success_rate,
            sample_count=len(history),
            established_at=time.time()
        )
        
        self.baselines[operation] = baseline
        
        log.debug(
            "Updated baseline for %s: duration=%.2fs, throughput=%.2f/s, memory=%.1fMB",
            operation, baseline.avg_duration, baseline.avg_throughput, baseline.avg_memory_usage
        )
    
    def get_tuning_summary(self) -> Dict[str, Any]:
        """Get summary of tuning activities and current state."""
        with self.lock:
            recent_actions = [
                action for action, timestamp in self.tuning_history
                if time.time() - timestamp < 3600  # Last hour
            ]
            
            return {
                "strategy": self.strategy.value,
                "operations_monitored": len(self.performance_history),
                "baselines_established": len(self.baselines),
                "recent_tuning_actions": len(recent_actions),
                "total_tuning_actions": len(self.tuning_history),
                "confidence_threshold": self.min_confidence_threshold,
                "learning_window_size": self.learning_window_size,
                "recent_actions": [
                    {
                        "parameter": action.parameter,
                        "old_value": action.current_value,
                        "new_value": action.new_value,
                        "reason": action.reason,
                        "confidence": action.confidence
                    }
                    for action in recent_actions[-5:]  # Last 5 actions
                ]
            }


class SystemMonitor:
    """System resource monitoring for performance tuning."""
    
    def __init__(self):
        self.monitoring_interval = 5.0  # seconds
        self.history_size = 100
        self.resource_history: deque = deque(maxlen=self.history_size)
        self.monitoring_thread: Optional[threading.Thread] = None
        self.stop_monitoring = threading.Event()
        
    def start_monitoring(self) -> None:
        """Start continuous system monitoring."""
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            return
        
        self.stop_monitoring.clear()
        self.monitoring_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitoring_thread.start()
        
        log.info("Started system monitoring")
    
    def stop_monitoring_thread(self) -> None:
        """Stop system monitoring."""
        if self.monitoring_thread:
            self.stop_monitoring.set()
            self.monitoring_thread.join(timeout=1.0)
            log.info("Stopped system monitoring")
    
    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while not self.stop_monitoring.wait(self.monitoring_interval):
            try:
                resources = self.get_current_resources()
                self.resource_history.append(resources)
            except Exception as e:
                log.warning("Error in system monitoring: %s", e)
    
    def get_current_resources(self) -> SystemResources:
        """Get current system resource usage."""
        memory = psutil.virtual_memory()
        import os
        root_path = os.path.abspath(os.sep)  # Gets 'C:\' on Windows, '/' on Unix
        disk = psutil.disk_usage(root_path)
        
        return SystemResources(
            cpu_percent=psutil.cpu_percent(interval=0.1),
            memory_percent=memory.percent,
            memory_available_gb=memory.available / (1024**3),
            disk_free_gb=disk.free / (1024**3),
            network_connections=len(psutil.net_connections())
        )
    
    def get_resource_trends(self) -> Dict[str, Any]:
        """Get resource usage trends."""
        if len(self.resource_history) < 2:
            return {}
        
        cpu_values = [r.cpu_percent for r in self.resource_history]
        memory_values = [r.memory_percent for r in self.resource_history]
        
        return {
            "cpu_trend": "increasing" if cpu_values[-1] > cpu_values[-5] else "decreasing",
            "memory_trend": "increasing" if memory_values[-1] > memory_values[-5] else "decreasing",
            "cpu_avg": statistics.mean(cpu_values[-10:]),
            "memory_avg": statistics.mean(memory_values[-10:]),
            "pressure_level": self.resource_history[-1].pressure_level
        }


# Global adaptive tuner instance
_global_tuner = AdaptivePerformanceTuner()


def get_global_tuner() -> AdaptivePerformanceTuner:
    """Get the global adaptive performance tuner."""
    return _global_tuner


def configure_global_tuner(
    strategy: TuningStrategy = TuningStrategy.BALANCED,
    learning_window_size: int = 20,
    min_confidence_threshold: float = 0.6
) -> AdaptivePerformanceTuner:
    """Configure the global adaptive tuner."""
    global _global_tuner
    _global_tuner = AdaptivePerformanceTuner(
        strategy=strategy,
        learning_window_size=learning_window_size,
        min_confidence_threshold=min_confidence_threshold
    )
    return _global_tuner


def auto_tune_decorator(operation_name: str):
    """Decorator to automatically tune function performance."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            tuner = get_global_tuner()
            
            # Record performance
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / (1024 * 1024)
            
            try:
                result = func(*args, **kwargs)
                
                # Create performance metrics
                end_time = time.time()
                end_memory = psutil.Process().memory_info().rss / (1024 * 1024)
                
                metrics = PerformanceMetrics(
                    operation_name=operation_name,
                    start_time=start_time,
                    end_time=end_time,
                    duration=end_time - start_time,
                    memory_before=start_memory,
                    memory_after=end_memory,
                    memory_peak=max(start_memory, end_memory),
                    cpu_percent=psutil.cpu_percent(),
                    worker_count=1,
                    items_processed=1
                )
                
                tuner.record_performance(metrics)
                return result
                
            except Exception as e:
                # Still record performance for failed operations
                end_time = time.time()
                end_memory = psutil.Process().memory_info().rss / (1024 * 1024)
                
                metrics = PerformanceMetrics(
                    operation_name=f"{operation_name}_failed",
                    start_time=start_time,
                    end_time=end_time,
                    duration=end_time - start_time,
                    memory_before=start_memory,
                    memory_after=end_memory,
                    memory_peak=max(start_memory, end_memory),
                    cpu_percent=psutil.cpu_percent(),
                    worker_count=1,
                    items_processed=0
                )
                
                tuner.record_performance(metrics)
                raise
        
        return wrapper
    return decorator