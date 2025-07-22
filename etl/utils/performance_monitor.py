"""Comprehensive performance monitoring system for ETL pipeline."""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Union
import json
import statistics
import psutil
from datetime import datetime, timedelta

from .performance_optimizer import PerformanceMetrics, SystemResources
from .adaptive_tuning import get_global_tuner
from .intelligent_cache import get_global_cache

log = logging.getLogger(__name__)


@dataclass
class AlertRule:
    """Performance alert rule configuration."""
    name: str
    metric: str
    condition: str  # "gt", "lt", "eq", "contains"
    threshold: float
    duration_seconds: int = 60
    severity: str = "warning"  # "info", "warning", "error", "critical"
    enabled: bool = True

    def check(self, value: float, duration: float) -> bool:
        """Check if alert condition is met."""
        if not self.enabled:
            return False

        if duration < self.duration_seconds:
            return False

        if self.condition == "gt":
            return value > self.threshold
        elif self.condition == "lt":
            return value < self.threshold
        elif self.condition == "eq":
            return abs(value - self.threshold) < 0.01

        return False


@dataclass
class PerformanceAlert:
    """Performance alert instance."""
    rule_name: str
    metric: str
    value: float
    threshold: float
    severity: str
    timestamp: float
    message: str
    acknowledged: bool = False
    resolved: bool = False

    def acknowledge(self) -> None:
        """Acknowledge the alert."""
        self.acknowledged = True
        log.info("Alert acknowledged: %s", self.rule_name)

    def resolve(self) -> None:
        """Resolve the alert."""
        self.resolved = True
        log.info("Alert resolved: %s", self.rule_name)


@dataclass
class PerformanceReport:
    """Performance report data."""
    period_start: float
    period_end: float
    operations: Dict[str, Dict[str, Any]]
    system_metrics: Dict[str, Any]
    alerts: List[PerformanceAlert]
    recommendations: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "period_start": self.period_start,
            "period_end": self.period_end,
            "duration_hours": (self.period_end - self.period_start) / 3600,
            "operations": self.operations,
            "system_metrics": self.system_metrics,
            "alerts": [
                {
                    "rule_name": alert.rule_name,
                    "metric": alert.metric,
                    "value": alert.value,
                    "threshold": alert.threshold,
                    "severity": alert.severity,
                    "timestamp": alert.timestamp,
                    "message": alert.message,
                    "acknowledged": alert.acknowledged,
                    "resolved": alert.resolved
                }
                for alert in self.alerts
            ],
            "recommendations": self.recommendations
        }


class PerformanceMonitor:
    """Comprehensive performance monitoring system."""

    def __init__(
        self,
        monitoring_interval: float = 10.0,
        history_retention_hours: int = 24,
        enable_alerts: bool = True,
        report_interval_minutes: int = 60
    ):
        self.monitoring_interval = monitoring_interval
        self.history_retention_hours = history_retention_hours
        self.enable_alerts = enable_alerts
        self.report_interval_minutes = report_interval_minutes

        # Data storage
        self.performance_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=1000))
        self.system_history: deque = deque(maxlen=1000)
        self.alerts: List[PerformanceAlert] = []
        self.alert_rules: List[AlertRule] = []
        self.active_alerts: Dict[str, PerformanceAlert] = {}

        # Monitoring state
        self.monitoring_active = False
        self.monitoring_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        # Statistics
        self.stats = {
            "total_operations": 0,
            "total_alerts": 0,
            "monitoring_start_time": time.time(),
            "last_report_time": time.time()
        }

        # Setup default alert rules
        self._setup_default_alert_rules()

        log.info("Initialized PerformanceMonitor")

    def start_monitoring(self) -> None:
        """Start continuous performance monitoring."""
        if self.monitoring_active:
            log.warning("Performance monitoring already active")
            return

        self.monitoring_active = True
        self.stop_event.clear()

        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()

        log.info(
            "🔍 Started performance monitoring (interval: %.1fs)",
            self.monitoring_interval)

    def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        if not self.monitoring_active:
            return

        self.monitoring_active = False
        self.stop_event.set()

        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5.0)

        log.info("Stopped performance monitoring")

    def record_performance(self, metrics: PerformanceMetrics) -> None:
        """Record performance metrics."""
        operation = metrics.operation_name

        # Store in history
        self.performance_history[operation].append(metrics)
        self.stats["total_operations"] += 1

        # Check alerts
        if self.enable_alerts:
            self._check_performance_alerts(metrics)

        # Auto-tune if enabled
        tuner = get_global_tuner()
        tuner.record_performance(metrics)

        log.debug(
            "Recorded performance: %s (%.2fs, %.2f items/s)",
            operation, metrics.duration, metrics.throughput_items_per_sec
        )

    def add_alert_rule(self, rule: AlertRule) -> None:
        """Add performance alert rule."""
        self.alert_rules.append(rule)
        log.info("Added alert rule: %s", rule.name)

    def remove_alert_rule(self, rule_name: str) -> bool:
        """Remove alert rule by name."""
        original_count = len(self.alert_rules)
        self.alert_rules = [
            rule for rule in self.alert_rules if rule.name != rule_name]

        if len(self.alert_rules) < original_count:
            log.info("Removed alert rule: %s", rule_name)
            return True

        return False

    def get_performance_summary(
            self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get performance summary for operation or all operations."""
        if operation:
            if operation not in self.performance_history:
                return {"error": f"No data for operation: {operation}"}

            history = list(self.performance_history[operation])
            return self._summarize_operation_metrics(operation, history)

        # Summary for all operations
        summary = {}
        for op_name, history in self.performance_history.items():
            summary[op_name] = self._summarize_operation_metrics(
                op_name, list(history))

        return summary

    def get_system_health(self) -> Dict[str, Any]:
        """Get current system health status."""
        if not self.system_history:
            return {"status": "no_data"}

        recent_metrics = list(self.system_history)[-10:]  # Last 10 samples

        avg_cpu = statistics.mean(m.cpu_percent for m in recent_metrics)
        avg_memory = statistics.mean(m.memory_percent for m in recent_metrics)
        min_disk_space = min(m.disk_free_gb for m in recent_metrics)

        # Determine health status
        if avg_cpu > 90 or avg_memory > 95 or min_disk_space < 1:
            status = "critical"
        elif avg_cpu > 80 or avg_memory > 85 or min_disk_space < 5:
            status = "warning"
        elif avg_cpu > 70 or avg_memory > 75:
            status = "degraded"
        else:
            status = "healthy"

        return {
            "status": status,
            "cpu_percent": avg_cpu,
            "memory_percent": avg_memory,
            "disk_free_gb": min_disk_space,
            "active_alerts": len(
                self.active_alerts),
            "monitoring_uptime_hours": (
                time.time() -
                self.stats["monitoring_start_time"]) /
            3600}

    def generate_report(self, hours_back: int = 1) -> PerformanceReport:
        """Generate comprehensive performance report."""
        end_time = time.time()
        start_time = end_time - (hours_back * 3600)

        # Collect operation metrics
        operations = {}
        for op_name, history in self.performance_history.items():
            relevant_metrics = [
                m for m in history
                if start_time <= m.start_time <= end_time
            ]

            if relevant_metrics:
                operations[op_name] = self._summarize_operation_metrics(
                    op_name, relevant_metrics)

        # Collect system metrics
        relevant_system_metrics = [
            m for m in self.system_history
            # Approximate, system metrics don't have timestamps
            if start_time <= time.time() <= end_time
        ]

        system_metrics = {}
        if relevant_system_metrics:
            system_metrics = {
                "avg_cpu_percent": statistics.mean(
                    m.cpu_percent for m in relevant_system_metrics), "avg_memory_percent": statistics.mean(
                    m.memory_percent for m in relevant_system_metrics), "min_disk_free_gb": min(
                    m.disk_free_gb for m in relevant_system_metrics), "pressure_periods": sum(
                    1 for m in relevant_system_metrics if m.is_under_pressure)}

        # Collect alerts
        relevant_alerts = [
            alert for alert in self.alerts
            if start_time <= alert.timestamp <= end_time
        ]

        # Generate recommendations
        recommendations = self._generate_recommendations(
            operations, system_metrics, relevant_alerts)

        report = PerformanceReport(
            period_start=start_time,
            period_end=end_time,
            operations=operations,
            system_metrics=system_metrics,
            alerts=relevant_alerts,
            recommendations=recommendations
        )

        self.stats["last_report_time"] = time.time()

        return report

    def save_report(self, report: PerformanceReport, file_path: Path) -> None:
        """Save performance report to file."""
        try:
            with file_path.open('w') as f:
                json.dump(report.to_dict(), f, indent=2)

            log.info("Performance report saved to %s", file_path)
        except Exception as e:
            log.error("Failed to save performance report: %s", e)

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        next_report_time = time.time() + (self.report_interval_minutes * 60)

        while not self.stop_event.wait(self.monitoring_interval):
            try:
                # Collect system metrics
                system_resources = self._get_system_resources()
                self.system_history.append(system_resources)

                # Check system alerts
                if self.enable_alerts:
                    self._check_system_alerts(system_resources)

                # Generate periodic reports
                if time.time() >= next_report_time:
                    self._generate_periodic_report()
                    next_report_time = time.time() + (self.report_interval_minutes * 60)

                # Cleanup old data
                self._cleanup_old_data()

            except Exception as e:
                log.warning("Error in monitoring loop: %s", e)

    def _get_system_resources(self) -> SystemResources:
        """Get current system resources."""
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage(
            getattr(
                self,
                "_ROOT_PATH",
                Path.cwd().anchor))

        return SystemResources(
            cpu_percent=psutil.cpu_percent(interval=0.1),
            memory_percent=memory.percent,
            memory_available_gb=memory.available / (1024**3),
            disk_free_gb=disk.free / (1024**3),
            network_connections=len(psutil.net_connections())
        )

    def _check_performance_alerts(self, metrics: PerformanceMetrics) -> None:
        """Check performance metrics against alert rules."""
        for rule in self.alert_rules:
            if not rule.enabled:
                continue

            value = self._get_metric_value(metrics, rule.metric)
            if value is None:
                continue

            if rule.check(value, metrics.duration):
                alert_key = f"{rule.name}_{metrics.operation_name}"

                if alert_key not in self.active_alerts:
                    alert = PerformanceAlert(
                        rule_name=rule.name,
                        metric=rule.metric,
                        value=value,
                        threshold=rule.threshold,
                        severity=rule.severity,
                        timestamp=time.time(),
                        message=f"Performance alert: {rule.metric} = {value:.2f} (threshold: {rule.threshold})"
                    )

                    self.active_alerts[alert_key] = alert
                    self.alerts.append(alert)
                    self.stats["total_alerts"] += 1

                    log.warning("🚨 Performance alert: %s", alert.message)

    def _check_system_alerts(self, resources: SystemResources) -> None:
        """Check system resources against alert rules."""
        system_metrics = {
            "cpu_percent": resources.cpu_percent,
            "memory_percent": resources.memory_percent,
            "disk_free_gb": resources.disk_free_gb,
            "memory_available_gb": resources.memory_available_gb
        }

        for metric, value in system_metrics.items():
            for rule in self.alert_rules:
                if rule.metric == metric and rule.enabled:
                    if rule.check(value, self.monitoring_interval):
                        alert_key = f"{rule.name}_system"

                        if alert_key not in self.active_alerts:
                            alert = PerformanceAlert(
                                rule_name=rule.name,
                                metric=metric,
                                value=value,
                                threshold=rule.threshold,
                                severity=rule.severity,
                                timestamp=time.time(),
                                message=f"System alert: {metric} = {value:.2f} (threshold: {rule.threshold})"
                            )

                            self.active_alerts[alert_key] = alert
                            self.alerts.append(alert)
                            self.stats["total_alerts"] += 1

                            log.warning("🚨 System alert: %s", alert.message)

    def _get_metric_value(
            self,
            metrics: PerformanceMetrics,
            metric_name: str) -> Optional[float]:
        """Extract metric value from performance metrics."""
        metric_map = {
            "duration": metrics.duration,
            "throughput": metrics.throughput_items_per_sec,
            "memory_usage": metrics.memory_peak,
            "memory_efficiency": metrics.memory_efficiency,
            "cpu_percent": metrics.cpu_percent,
            "worker_count": metrics.worker_count,
            "items_processed": metrics.items_processed
        }

        return metric_map.get(metric_name)

    def _summarize_operation_metrics(
            self, operation: str, metrics: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Summarize metrics for an operation."""
        if not metrics:
            return {"error": "No metrics available"}

        durations = [m.duration for m in metrics]
        throughputs = [m.throughput_items_per_sec for m in metrics]
        memory_usage = [m.memory_peak for m in metrics]

        return {
            "operation_count": len(metrics),
            "duration": {
                "avg": statistics.mean(durations),
                "min": min(durations),
                "max": max(durations),
                "std": statistics.stdev(durations) if len(durations) > 1 else 0
            },
            "throughput": {
                "avg": statistics.mean(throughputs),
                "min": min(throughputs),
                "max": max(throughputs),
                "std": statistics.stdev(throughputs) if len(throughputs) > 1 else 0
            },
            "memory": {
                "avg_mb": statistics.mean(memory_usage),
                "min_mb": min(memory_usage),
                "max_mb": max(memory_usage),
                "std_mb": statistics.stdev(memory_usage) if len(memory_usage) > 1 else 0
            },
            "efficiency": {
                "avg_items_per_mb": statistics.mean(m.memory_efficiency for m in metrics),
                "total_items_processed": sum(m.items_processed for m in metrics),
                "total_bytes_processed": sum(m.bytes_processed for m in metrics)
            }
        }

    def _generate_recommendations(
        self,
        operations: Dict[str, Dict[str, Any]],
        system_metrics: Dict[str, Any],
        alerts: List[PerformanceAlert]
    ) -> List[str]:
        """Generate performance recommendations."""
        recommendations = []

        # Check for high-variance operations
        for op_name, metrics in operations.items():
            if "duration" in metrics and metrics["duration"]["std"] > metrics["duration"]["avg"] * 0.5:
                recommendations.append(
                    f"Operation '{op_name}' has high duration variance - consider investigating intermittent issues"
                )

        # Check for low throughput operations
        for op_name, metrics in operations.items():
            if "throughput" in metrics and metrics["throughput"]["avg"] < 0.5:
                recommendations.append(
                    f"Operation '{op_name}' has low throughput - consider increasing concurrency or optimizing processing"
                )

        # Check for memory-intensive operations
        for op_name, metrics in operations.items():
            if "memory" in metrics and metrics["memory"]["avg_mb"] > 500:
                recommendations.append(
                    f"Operation '{op_name}' uses high memory - consider batch processing or memory optimization"
                )

        # Check system-level issues
        if system_metrics and system_metrics.get("avg_cpu_percent", 0) > 80:
            recommendations.append(
                "High CPU usage detected - consider reducing concurrency or optimizing operations")

        if system_metrics and system_metrics.get("avg_memory_percent", 0) > 85:
            recommendations.append(
                "High memory usage detected - consider increasing memory or optimizing data structures")

        # Check cache performance
        cache_stats = get_global_cache().get_stats()
        if cache_stats["performance"]["hit_rate_percent"] < 50:
            recommendations.append(
                "Low cache hit rate - consider increasing cache size or adjusting TTL settings")

        return recommendations

    def _generate_periodic_report(self) -> None:
        """Generate and log periodic performance report."""
        try:
            report = self.generate_report(hours_back=1)

            log.info("📊 Performance Report (last hour):")
            log.info(f"   Operations monitored: {len(report.operations)}")
            log.info(
                f"   Active alerts: {len([a for a in report.alerts if not a.resolved])}")
            log.info(f"   Recommendations: {len(report.recommendations)}")

            if report.system_metrics:
                log.info(
                    f"   Avg CPU: {report.system_metrics.get('avg_cpu_percent', 0):.1f}%")
                log.info(
                    f"   Avg Memory: {report.system_metrics.get('avg_memory_percent', 0):.1f}%")

            # Show top 3 recommendations
            for rec in report.recommendations[:3]:
                log.info(f"   💡 {rec}")

        except Exception as e:
            log.error("Failed to generate periodic report: %s", e)

    def _cleanup_old_data(self) -> None:
        """Clean up old performance data."""
        cutoff_time = time.time() - (self.history_retention_hours * 3600)

        # Clean up performance history
        for operation in self.performance_history:
            history = self.performance_history[operation]
            # Keep only recent metrics
            while history and history[0].start_time < cutoff_time:
                history.popleft()

        # Clean up alerts
        self.alerts = [
            alert for alert in self.alerts if alert.timestamp > cutoff_time]

        # Clean up active alerts that are resolved
        resolved_alerts = [
            key for key, alert in self.active_alerts.items()
            if alert.resolved or alert.timestamp < cutoff_time
        ]

        for key in resolved_alerts:
            del self.active_alerts[key]

    def _setup_default_alert_rules(self) -> None:
        """Setup default performance alert rules."""
        default_rules = [
            AlertRule(
                name="high_operation_duration",
                metric="duration",
                condition="gt",
                threshold=300.0,  # 5 minutes
                severity="warning"
            ),
            AlertRule(
                name="low_throughput",
                metric="throughput",
                condition="lt",
                threshold=0.1,
                severity="warning"
            ),
            AlertRule(
                name="high_memory_usage",
                metric="memory_usage",
                condition="gt",
                threshold=1024.0,  # 1GB
                severity="warning"
            ),
            AlertRule(
                name="high_cpu_usage",
                metric="cpu_percent",
                condition="gt",
                threshold=90.0,
                severity="warning"
            ),
            AlertRule(
                name="high_system_memory",
                metric="memory_percent",
                condition="gt",
                threshold=90.0,
                severity="critical"
            ),
            AlertRule(
                name="low_disk_space",
                metric="disk_free_gb",
                condition="lt",
                threshold=5.0,
                severity="critical"
            )
        ]

        self.alert_rules.extend(default_rules)
        log.info("Setup %d default alert rules", len(default_rules))


# Global performance monitor instance
_global_monitor = PerformanceMonitor()


def get_global_monitor() -> PerformanceMonitor:
    """Get the global performance monitor."""
    return _global_monitor


def configure_global_monitor(
    monitoring_interval: float = 10.0,
    history_retention_hours: int = 24,
    enable_alerts: bool = True,
    report_interval_minutes: int = 60
) -> PerformanceMonitor:
    """Configure the global performance monitor."""
    global _global_monitor

    # Stop existing monitor
    _global_monitor.stop_monitoring()

    # Create new monitor
    _global_monitor = PerformanceMonitor(
        monitoring_interval=monitoring_interval,
        history_retention_hours=history_retention_hours,
        enable_alerts=enable_alerts,
        report_interval_minutes=report_interval_minutes
    )

    return _global_monitor


def start_global_monitoring() -> None:
    """Start global performance monitoring."""
    _global_monitor.start_monitoring()


def stop_global_monitoring() -> None:
    """Stop global performance monitoring."""
    _global_monitor.stop_monitoring()


def performance_monitored(operation_name: str):
    """Decorator to automatically monitor function performance."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            monitor = get_global_monitor()

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

                monitor.record_performance(metrics)
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

                monitor.record_performance(metrics)
                raise

        return wrapper
    return decorator
