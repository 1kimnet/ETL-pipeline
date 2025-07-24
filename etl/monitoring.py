"""Monitoring and observability for ETL pipeline.

This module provides structured logging, metrics collection, health checks,
and monitoring capabilities for the ETL pipeline operations.
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import threading

from .exceptions import ETLError, format_error_context

# Structured logging formatter


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # Add exception information if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in (
                'name',
                'msg',
                'args',
                'levelname',
                'levelno',
                'pathname',
                'filename',
                'module',
                'lineno',
                'funcName',
                'created',
                'msecs',
                'relativeCreated',
                'thread',
                'threadName',
                'processName',
                'process',
                'exc_info',
                'exc_text',
                    'stack_info'):
                log_data[key] = value

        return json.dumps(log_data, default=str)


@dataclass
class MetricPoint:
    """Single metric data point."""
    name: str
    value: Union[int, float]
    timestamp: float
    tags: Dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"  # gauge, counter, histogram


@dataclass
class HealthCheck:
    """Health check result."""
    name: str
    status: str  # healthy, unhealthy, warning
    message: str
    timestamp: float
    details: Dict[str, Any] = field(default_factory=dict)
    duration_ms: Optional[float] = None


@dataclass
class PipelineRun:
    """Information about a pipeline run."""
    run_id: str
    start_time: float
    end_time: Optional[float] = None
    status: str = "running"  # running, completed, failed
    sources_processed: int = 0
    sources_successful: int = 0
    sources_failed: int = 0
    total_records: int = 0
    total_bytes: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def duration(self) -> float:
        """Get run duration in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def success_rate(self) -> float:
        """Get success rate as percentage."""
        if self.sources_processed == 0:
            return 0.0
        return (self.sources_successful / self.sources_processed) * 100


class MetricsCollector:
    """Collects and manages performance metrics."""

    def __init__(self, max_points: int = 10000):
        self.max_points = max_points
        self._metrics: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_points))
        self._lock = threading.RLock()

        # Built-in metrics
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)

        logging.getLogger(__name__).info(
            "📊 Metrics collector initialized (max_points=%d)", max_points)

    def record_metric(
        self,
        name: str,
        value: Union[int, float],
        tags: Optional[Dict[str, str]] = None,
        metric_type: str = "gauge"
    ):
        """Record a metric data point."""
        timestamp = time.time()
        point = MetricPoint(
            name=name,
            value=value,
            timestamp=timestamp,
            tags=tags or {},
            metric_type=metric_type
        )

        with self._lock:
            self._metrics[name].append(point)

            # Update internal counters/gauges
            if metric_type == "counter":
                self._counters[name] += value
            elif metric_type == "gauge":
                self._gauges[name] = value

    def increment_counter(self, name: str, value: float = 1.0,
                          tags: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        self.record_metric(name, value, tags, "counter")

    def set_gauge(
            self, name: str, value: Union[int, float], tags: Optional[Dict[str, str]] = None):
        """Set a gauge metric value."""
        self.record_metric(name, value, tags, "gauge")

    def record_timing(self, name: str, duration_ms: float,
                      tags: Optional[Dict[str, str]] = None):
        """Record a timing metric."""
        self.record_metric(name, duration_ms, tags, "histogram")

    def get_metric_history(
            self,
            name: str,
            since: Optional[float] = None) -> List[MetricPoint]:
        """Get metric history for a given metric name."""
        with self._lock:
            points = list(self._metrics.get(name, []))

            if since is not None:
                points = [p for p in points if p.timestamp >= since]

            return points

    def get_current_value(self, name: str) -> Optional[float]:
        """Get current value for a metric."""
        with self._lock:
            if name in self._gauges:
                return self._gauges[name]
            elif name in self._counters:
                return self._counters[name]
            elif name in self._metrics:
                points = self._metrics[name]
                return points[-1].value if points else None
            return None

    def get_metric_summary(
            self, name: str, time_window: Optional[float] = None) -> Dict[str, Any]:
        """Get summary statistics for a metric."""
        since = time.time() - time_window if time_window else None
        points = self.get_metric_history(name, since)

        if not points:
            return {"count": 0}

        values = [p.value for p in points]

        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "current": values[-1] if values else None,
            "first_timestamp": points[0].timestamp,
            "last_timestamp": points[-1].timestamp
        }

    def get_all_metrics_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary for all metrics."""
        with self._lock:
            summaries = {}
            for name in self._metrics:
                summaries[name] = self.get_metric_summary(name)
            return summaries

    def clear_metrics(self, older_than: Optional[float] = None):
        """Clear metrics older than specified timestamp."""
        with self._lock:
            if older_than is None:
                # Clear all metrics
                self._metrics.clear()
                self._counters.clear()
                self._gauges.clear()
            else:
                # Clear old metrics
                for name, points in self._metrics.items():
                    while points and points[0].timestamp < older_than:
                        points.popleft()


class HealthMonitor:
    """Monitors system health and performs health checks."""

    def __init__(self):
        self._health_checks: Dict[str, HealthCheck] = {}
        self._check_functions: Dict[str, callable] = {}
        self._lock = threading.RLock()

        # Register default health checks
        self._register_default_checks()

        logging.getLogger(__name__).info("🩺 Health monitor initialized")

    def _register_default_checks(self):
        """Register default health checks."""
        self.register_check("system_time", self._check_system_time)
        self.register_check("memory_usage", self._check_memory_usage)
        self.register_check("disk_space", self._check_disk_space)

    def register_check(self, name: str, check_function: callable):
        """Register a health check function."""
        with self._lock:
            self._check_functions[name] = check_function
        logging.getLogger(__name__).debug("Registered health check: %s", name)

    def run_check(self, name: str) -> HealthCheck:
        """Run a specific health check."""
        if name not in self._check_functions:
            return HealthCheck(
                name=name,
                status="unhealthy",
                message=f"Unknown health check: {name}",
                timestamp=time.time()
            )

        start_time = time.time()
        try:
            check_function = self._check_functions[name]
            result = check_function()

            if isinstance(result, HealthCheck):
                result.duration_ms = (time.time() - start_time) * 1000
                return result
            else:
                # Legacy support for simple boolean/string returns
                status = "healthy" if result else "unhealthy"
                return HealthCheck(
                    name=name,
                    status=status,
                    message=str(result),
                    timestamp=time.time(),
                    duration_ms=(time.time() - start_time) * 1000
                )
        except Exception as e:
            return HealthCheck(
                name=name,
                status="unhealthy",
                message=f"Health check failed: {e}",
                timestamp=time.time(),
                duration_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    def run_all_checks(self) -> Dict[str, HealthCheck]:
        """Run all registered health checks."""
        results = {}

        for name in self._check_functions:
            results[name] = self.run_check(name)

        with self._lock:
            self._health_checks.update(results)

        return results

    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        checks = self.run_all_checks()

        healthy_count = sum(1 for check in checks.values()
                            if check.status == "healthy")
        warning_count = sum(1 for check in checks.values()
                            if check.status == "warning")
        unhealthy_count = sum(1 for check in checks.values()
                              if check.status == "unhealthy")

        overall_status = "healthy"
        if unhealthy_count > 0:
            overall_status = "unhealthy"
        elif warning_count > 0:
            overall_status = "warning"

        return {
            "status": overall_status,
            "timestamp": time.time(),
            "checks": {name: asdict(check) for name, check in checks.items()},
            "summary": {
                "total": len(checks),
                "healthy": healthy_count,
                "warning": warning_count,
                "unhealthy": unhealthy_count
            }
        }

    def _check_system_time(self) -> HealthCheck:
        """Check system time is reasonable."""
        current_time = time.time()
        year = datetime.fromtimestamp(current_time).year

        if year < 2020 or year > 2030:
            return HealthCheck(
                name="system_time",
                status="unhealthy",
                message=f"System time appears incorrect: {datetime.fromtimestamp(current_time)}",
                timestamp=current_time)

        return HealthCheck(
            name="system_time",
            status="healthy",
            message="System time is reasonable",
            timestamp=current_time
        )

    def _check_memory_usage(self) -> HealthCheck:
        """Check memory usage."""
        import psutil
        memory = psutil.virtual_memory()
        usage_percent = memory.percent

        status = "healthy"
        message = f"Memory usage: {usage_percent:.1f}%"

        if usage_percent > 90:
            status = "unhealthy"
            message = f"High memory usage: {usage_percent:.1f}%"
        elif usage_percent > 75:
            status = "warning"
            message = f"Elevated memory usage: {usage_percent:.1f}%"

        return HealthCheck(
            name="memory_usage",
            status=status,
            message=message,
            timestamp=time.time(),
            details={
                "percent": usage_percent,
                "available_gb": memory.available / (1024**3),
                "total_gb": memory.total / (1024**3)
            }
        )

    def _check_disk_space(self) -> HealthCheck:
        """Check disk space."""
        try:
            import shutil
            import os
            total, used, free = shutil.disk_usage(
                getattr(self, "ROOT_PATH", Path.cwd().anchor))
            free_percent = (free / total) * 100

            status = "healthy"
            message = f"Disk space: {free_percent:.1f}% free"

            if free_percent < 5:
                status = "unhealthy"
                message = f"Low disk space: {free_percent:.1f}% free"
            elif free_percent < 15:
                status = "warning"
                message = f"Limited disk space: {free_percent:.1f}% free"

            return HealthCheck(
                name="disk_space",
                status=status,
                message=message,
                timestamp=time.time(),
                details={
                    "free_percent": free_percent,
                    "free_gb": free / (1024**3),
                    "total_gb": total / (1024**3)
                }
            )
        except Exception as e:
            return HealthCheck(
                name="disk_space",
                status="unhealthy",
                message=f"Failed to check disk space: {e}",
                timestamp=time.time()
            )


class PipelineMonitor:
    """Monitors pipeline execution and tracks runs."""

    def __init__(self):
        self._current_run: Optional[PipelineRun] = None
        self._run_history: List[PipelineRun] = []
        self._max_history = 100
        self._lock = threading.RLock()

        logging.getLogger(__name__).info("🔍 Pipeline monitor initialized")

    def start_run(self, run_id: str) -> PipelineRun:
        """Start monitoring a new pipeline run."""
        with self._lock:
            if self._current_run and self._current_run.status == "running":
                # Mark previous run as failed if still running
                self._current_run.status = "failed"
                self._current_run.end_time = time.time()
                self._current_run.errors.append("Run interrupted by new run")

            self._current_run = PipelineRun(
                run_id=run_id,
                start_time=time.time()
            )

            logging.getLogger(__name__).info(
                "🚀 Started pipeline run: %s", run_id)
            return self._current_run

    def end_run(self, status: str = "completed"):
        """End the current pipeline run."""
        with self._lock:
            if self._current_run:
                self._current_run.end_time = time.time()
                self._current_run.status = status

                # Add to history
                self._run_history.append(self._current_run)

                # Trim history
                if len(self._run_history) > self._max_history:
                    self._run_history = self._run_history[-self._max_history:]

                logging.getLogger(__name__).info(
                    "🏁 Ended pipeline run: %s (status=%s, duration=%.2fs)",
                    self._current_run.run_id,
                    status,
                    self._current_run.duration
                )

                self._current_run = None

    def record_source_processed(
            self,
            success: bool,
            records: int = 0,
            bytes_processed: int = 0,
            error: Optional[str] = None):
        """Record processing of a source."""
        with self._lock:
            if self._current_run:
                self._current_run.sources_processed += 1
                self._current_run.total_records += records
                self._current_run.total_bytes += bytes_processed

                if success:
                    self._current_run.sources_successful += 1
                else:
                    self._current_run.sources_failed += 1
                    if error:
                        self._current_run.errors.append(error)

    def get_current_run(self) -> Optional[PipelineRun]:
        """Get current pipeline run."""
        with self._lock:
            return self._current_run

    def get_run_history(
            self,
            limit: Optional[int] = None) -> List[PipelineRun]:
        """Get pipeline run history."""
        with self._lock:
            history = self._run_history.copy()
            if limit:
                history = history[-limit:]
            return history

    def get_run_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get pipeline run statistics for the last N days."""
        cutoff_time = time.time() - (days * 24 * 3600)

        with self._lock:
            recent_runs = [
                run for run in self._run_history if run.start_time >= cutoff_time]

        if not recent_runs:
            return {"total_runs": 0, "period_days": days}

        completed_runs = [
            run for run in recent_runs if run.status == "completed"]
        failed_runs = [run for run in recent_runs if run.status == "failed"]

        total_records = sum(run.total_records for run in recent_runs)
        total_bytes = sum(run.total_bytes for run in recent_runs)
        total_duration = sum(
            run.duration for run in recent_runs if run.end_time)

        avg_duration = total_duration / len(recent_runs) if recent_runs else 0
        success_rate = (len(completed_runs) / len(recent_runs)
                        ) * 100 if recent_runs else 0

        return {
            "period_days": days,
            "total_runs": len(recent_runs),
            "completed_runs": len(completed_runs),
            "failed_runs": len(failed_runs),
            "success_rate_percent": success_rate,
            "total_records_processed": total_records,
            "total_bytes_processed": total_bytes,
            "average_duration_seconds": avg_duration,
            "records_per_second": total_records /
            total_duration if total_duration > 0 else 0,
            "bytes_per_second": total_bytes /
            total_duration if total_duration > 0 else 0}


class StructuredLogger:
    """Enhanced logger with structured logging and metrics integration."""

    def __init__(
        self,
        name: str,
        metrics_collector: Optional[MetricsCollector] = None,
        pipeline_monitor: Optional[PipelineMonitor] = None
    ):
        self.logger = logging.getLogger(name)
        self.metrics = metrics_collector
        self.monitor = pipeline_monitor

    def info(self, message: str, **kwargs):
        """Log info message with structured data."""
        self._log_with_metrics(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with structured data."""
        self._log_with_metrics(logging.WARNING, message, **kwargs)
        if self.metrics:
            self.metrics.increment_counter("log.warnings")

    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        """Log error message with structured data."""
        if error:
            kwargs["error_type"] = type(error).__name__
            kwargs["error_message"] = str(error)

            if isinstance(error, ETLError):
                kwargs["error_context"] = error.context
                kwargs["error_recoverable"] = error.recoverable
                kwargs["error_source"] = error.source_name

        self._log_with_metrics(
            logging.ERROR,
            message,
            exc_info=error,
            **kwargs)

        if self.metrics:
            self.metrics.increment_counter("log.errors")

        if self.monitor and error:
            self.monitor.record_source_processed(
                success=False, error=str(error))

    def debug(self, message: str, **kwargs):
        """Log debug message with structured data."""
        self._log_with_metrics(logging.DEBUG, message, **kwargs)

    def _log_with_metrics(self, level: int, message: str, **kwargs):
        """Log message and record metrics."""
        # Add structured data as extra fields
        extra = {k: v for k, v in kwargs.items() if not k.startswith('exc_')}

        # Log the message
        self.logger.log(level, message, extra=extra, **
                        {k: v for k, v in kwargs.items() if k.startswith('exc_')})

        # Record metrics
        if self.metrics:
            self.metrics.increment_counter(
                f"log.{logging.getLevelName(level).lower()}")


def setup_structured_logging(
    log_dir: Union[str, Path],
    app_name: str = "etl-pipeline",
    level: str = "INFO",
    max_file_size_mb: int = 100,
    backup_count: int = 5
) -> logging.Logger:
    """Set up structured logging configuration."""
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create formatters
    structured_formatter = StructuredFormatter()
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create handlers
    # File handler for structured JSON logs
    json_log_file = log_dir / f"{app_name}.json"
    json_handler = logging.handlers.RotatingFileHandler(
        json_log_file,
        maxBytes=max_file_size_mb * 1024 * 1024,
        backupCount=backup_count
    )
    json_handler.setFormatter(structured_formatter)

    # File handler for human-readable logs
    text_log_file = log_dir / f"{app_name}.log"
    text_handler = logging.handlers.RotatingFileHandler(
        text_log_file,
        maxBytes=max_file_size_mb * 1024 * 1024,
        backupCount=backup_count
    )
    text_handler.setFormatter(console_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Add handlers
    root_logger.addHandler(json_handler)
    root_logger.addHandler(text_handler)
    root_logger.addHandler(console_handler)

    logging.getLogger(__name__).info("📝 Structured logging configured")
    return root_logger


# Global instances
_metrics_collector = None
_health_monitor = None
_pipeline_monitor = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def get_health_monitor() -> HealthMonitor:
    """Get global health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = HealthMonitor()
    return _health_monitor


def get_pipeline_monitor() -> PipelineMonitor:
    """Get global pipeline monitor instance."""
    global _pipeline_monitor
    if _pipeline_monitor is None:
        _pipeline_monitor = PipelineMonitor()
    return _pipeline_monitor


def get_structured_logger(name: str) -> StructuredLogger:
    """Get structured logger instance."""
    return StructuredLogger(
        name,
        metrics_collector=get_metrics_collector(),
        pipeline_monitor=get_pipeline_monitor()
    )
