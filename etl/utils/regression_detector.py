"""Performance regression detection system for ETL pipeline."""
from __future__ import annotations

import logging
import pickle
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
from scipy import stats
import warnings

from .performance_optimizer import PerformanceMetrics
from .performance_monitor import PerformanceAlert

# Suppress scipy warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)

log = logging.getLogger(__name__)


@dataclass
class RegressionBaseline:
    """Baseline performance metrics for regression detection."""
    operation_name: str
    metrics: Dict[str, float]  # metric_name -> value
    confidence_intervals: Dict[str, Tuple[float, float]]  # metric_name -> (lower, upper)
    sample_count: int
    established_at: float
    last_updated: float
    version: str = "1.0"
    
    def is_regression(self, current_value: float, metric_name: str, sensitivity: float = 0.95) -> bool:
        """Check if current value indicates a regression."""
        if metric_name not in self.confidence_intervals:
            return False
        
        lower, upper = self.confidence_intervals[metric_name]
        
        # For metrics where lower is better (duration, memory_usage)
        if metric_name in ["duration", "memory_usage", "memory_peak"]:
            # Regression if significantly higher than baseline
            return current_value > upper
        
        # For metrics where higher is better (throughput, success_rate)
        elif metric_name in ["throughput", "success_rate", "items_per_second"]:
            # Regression if significantly lower than baseline
            return current_value < lower
        
        return False


@dataclass
class RegressionDetection:
    """Detected performance regression."""
    operation_name: str
    metric_name: str
    baseline_value: float
    current_value: float
    regression_magnitude: float  # How much worse as percentage
    confidence_level: float
    detection_time: float
    severity: str  # "minor", "moderate", "major", "critical"
    
    @property
    def is_significant(self) -> bool:
        """Check if regression is statistically significant."""
        return self.confidence_level > 0.95 and abs(self.regression_magnitude) > 0.1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "operation_name": self.operation_name,
            "metric_name": self.metric_name,
            "baseline_value": self.baseline_value,
            "current_value": self.current_value,
            "regression_magnitude": self.regression_magnitude,
            "confidence_level": self.confidence_level,
            "detection_time": self.detection_time,
            "severity": self.severity,
            "is_significant": self.is_significant
        }


class StatisticalAnalyzer:
    """Statistical analysis for performance regression detection."""
    
    @staticmethod
    def calculate_confidence_interval(
        values: List[float], 
        confidence_level: float = 0.95
    ) -> Tuple[float, float]:
        """Calculate confidence interval for a dataset."""
        if len(values) < 2:
            mean_val = values[0] if values else 0
            return (mean_val, mean_val)
        
        mean_val = statistics.mean(values)
        std_val = statistics.stdev(values)
        n = len(values)
        
        # Use t-distribution for small samples
        if n < 30:
            t_critical = stats.t.ppf((1 + confidence_level) / 2, n - 1)
            margin_error = t_critical * (std_val / np.sqrt(n))
        else:
            # Use normal distribution for large samples
            z_critical = stats.norm.ppf((1 + confidence_level) / 2)
            margin_error = z_critical * (std_val / np.sqrt(n))
        
        return (mean_val - margin_error, mean_val + margin_error)
    
    @staticmethod
    def detect_trend(values: List[float], window_size: int = 10) -> str:
        """Detect trend in performance metrics."""
        if len(values) < window_size:
            return "insufficient_data"
        
        recent_values = values[-window_size:]
        
        # Calculate linear regression slope
        x = np.arange(len(recent_values))
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, recent_values)
        
        # Determine trend significance
        if p_value < 0.05:  # Statistically significant
            if slope > 0:
                return "increasing"
            else:
                return "decreasing"
        else:
            return "stable"
    
    @staticmethod
    def calculate_regression_magnitude(baseline: float, current: float) -> float:
        """Calculate regression magnitude as percentage change."""
        if baseline == 0:
            return 0.0
        
        return ((current - baseline) / baseline) * 100
    
    @staticmethod
    def detect_anomalies(values: List[float], threshold: float = 2.0) -> List[int]:
        """Detect anomalies using z-score method."""
        if len(values) < 3:
            return []
        
        mean_val = statistics.mean(values)
        std_val = statistics.stdev(values)
        
        if std_val == 0:
            return []
        
        anomalies = []
        for i, value in enumerate(values):
            z_score = abs((value - mean_val) / std_val)
            if z_score > threshold:
                anomalies.append(i)
        
        return anomalies
    
    @staticmethod
    def perform_change_point_detection(values: List[float], min_segment_length: int = 5) -> List[int]:
        """Detect change points in performance metrics."""
        if len(values) < min_segment_length * 2:
            return []
        
        change_points = []
        n = len(values)
        
        for i in range(min_segment_length, n - min_segment_length):
            # Split data at point i
            segment1 = values[:i]
            segment2 = values[i:]
            
            # Perform t-test
            if len(segment1) >= 2 and len(segment2) >= 2:
                t_stat, p_value = stats.ttest_ind(segment1, segment2)
                
                if p_value < 0.01:  # Highly significant change
                    change_points.append(i)
        
        return change_points


class PerformanceRegressionDetector:
    """Advanced performance regression detection system."""
    
    def __init__(
        self,
        baseline_window_size: int = 50,
        detection_window_size: int = 10,
        min_samples_for_baseline: int = 20,
        baseline_file: Optional[Path] = None
    ):
        self.baseline_window_size = baseline_window_size
        self.detection_window_size = detection_window_size
        self.min_samples_for_baseline = min_samples_for_baseline
        self.baseline_file = baseline_file or Path("performance_baselines.pkl")
        
        # Data storage
        self.performance_data: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.baseline_window_size * 2)
        )
        self.baselines: Dict[str, RegressionBaseline] = {}
        self.detected_regressions: List[RegressionDetection] = []
        
        # Statistical analyzer
        self.analyzer = StatisticalAnalyzer()
        
        # Load existing baselines
        self._load_baselines()
        
        log.info("Initialized PerformanceRegressionDetector")
    
    def record_performance(self, metrics: PerformanceMetrics) -> Optional[List[RegressionDetection]]:
        """Record performance metrics and check for regressions."""
        operation = metrics.operation_name
        
        # Store metrics
        self.performance_data[operation].append(metrics)
        
        # Update or establish baseline
        if self._should_update_baseline(operation):
            self._update_baseline(operation)
        
        # Check for regressions
        if operation in self.baselines:
            regressions = self._detect_regressions(operation, metrics)
            if regressions:
                self.detected_regressions.extend(regressions)
                return regressions
        
        return None
    
    def establish_baseline(self, operation: str, force_update: bool = False) -> bool:
        """Establish performance baseline for an operation."""
        if operation not in self.performance_data:
            log.warning("No performance data available for %s", operation)
            return False
        
        data = list(self.performance_data[operation])
        
        if len(data) < self.min_samples_for_baseline:
            log.warning("Insufficient data for baseline: %s (%d samples)", operation, len(data))
            return False
        
        if operation in self.baselines and not force_update:
            log.info("Baseline already exists for %s", operation)
            return True
        
        return self._update_baseline(operation)
    
    def get_baseline(self, operation: str) -> Optional[RegressionBaseline]:
        """Get baseline for an operation."""
        return self.baselines.get(operation)
    
    def get_regression_summary(self) -> Dict[str, Any]:
        """Get summary of detected regressions."""
        if not self.detected_regressions:
            return {"total_regressions": 0, "operations_affected": 0}
        
        # Group by operation
        by_operation = defaultdict(list)
        for regression in self.detected_regressions:
            by_operation[regression.operation_name].append(regression)
        
        # Calculate summary statistics
        severities = defaultdict(int)
        for regression in self.detected_regressions:
            severities[regression.severity] += 1
        
        significant_regressions = [r for r in self.detected_regressions if r.is_significant]
        
        return {
            "total_regressions": len(self.detected_regressions),
            "significant_regressions": len(significant_regressions),
            "operations_affected": len(by_operation),
            "severity_breakdown": dict(severities),
            "by_operation": {
                op: len(regressions) for op, regressions in by_operation.items()
            },
            "recent_regressions": [
                r.to_dict() for r in sorted(
                    self.detected_regressions, 
                    key=lambda x: x.detection_time, 
                    reverse=True
                )[:10]
            ]
        }
    
    def analyze_performance_trends(self, operation: str) -> Dict[str, Any]:
        """Analyze performance trends for an operation."""
        if operation not in self.performance_data:
            return {"error": f"No data for operation: {operation}"}
        
        data = list(self.performance_data[operation])
        
        if len(data) < 5:
            return {"error": "Insufficient data for trend analysis"}
        
        # Extract metrics
        durations = [m.duration for m in data]
        throughputs = [m.throughput_items_per_sec for m in data]
        memory_usage = [m.memory_peak for m in data]
        
        # Analyze trends
        trends = {
            "duration": self.analyzer.detect_trend(durations),
            "throughput": self.analyzer.detect_trend(throughputs),
            "memory_usage": self.analyzer.detect_trend(memory_usage)
        }
        
        # Detect anomalies
        anomalies = {
            "duration": self.analyzer.detect_anomalies(durations),
            "throughput": self.analyzer.detect_anomalies(throughputs),
            "memory_usage": self.analyzer.detect_anomalies(memory_usage)
        }
        
        # Detect change points
        change_points = {
            "duration": self.analyzer.perform_change_point_detection(durations),
            "throughput": self.analyzer.perform_change_point_detection(throughputs),
            "memory_usage": self.analyzer.perform_change_point_detection(memory_usage)
        }
        
        return {
            "operation": operation,
            "sample_count": len(data),
            "trends": trends,
            "anomalies": anomalies,
            "change_points": change_points,
            "stability_score": self._calculate_stability_score(data)
        }
    
    def generate_regression_report(self, hours_back: int = 24) -> Dict[str, Any]:
        """Generate comprehensive regression report."""
        cutoff_time = time.time() - (hours_back * 3600)
        
        # Filter recent regressions
        recent_regressions = [
            r for r in self.detected_regressions 
            if r.detection_time > cutoff_time
        ]
        
        # Analyze by operation
        operation_analysis = {}
        for operation in self.performance_data.keys():
            if operation in self.baselines:
                operation_analysis[operation] = self.analyze_performance_trends(operation)
        
        # Generate recommendations
        recommendations = self._generate_regression_recommendations(recent_regressions)
        
        return {
            "report_period_hours": hours_back,
            "recent_regressions": len(recent_regressions),
            "total_operations_monitored": len(self.performance_data),
            "baselines_established": len(self.baselines),
            "regressions_by_severity": {
                severity: len([r for r in recent_regressions if r.severity == severity])
                for severity in ["minor", "moderate", "major", "critical"]
            },
            "operation_analysis": operation_analysis,
            "recommendations": recommendations,
            "detailed_regressions": [r.to_dict() for r in recent_regressions]
        }
    
    def save_baselines(self) -> None:
        """Save baselines to file."""
        try:
            with self.baseline_file.open('wb') as f:
                pickle.dump(self.baselines, f)
            log.info("Saved %d baselines to %s", len(self.baselines), self.baseline_file)
        except Exception as e:
            log.error("Failed to save baselines: %s", e)
    
    def _load_baselines(self) -> None:
        """Load baselines from file."""
        if not self.baseline_file.exists():
            return
        
        try:
            with self.baseline_file.open('rb') as f:
                self.baselines = pickle.load(f)
            log.info("Loaded %d baselines from %s", len(self.baselines), self.baseline_file)
        except Exception as e:
            log.warning("Failed to load baselines: %s", e)
            self.baselines = {}
    
    def _should_update_baseline(self, operation: str) -> bool:
        """Determine if baseline should be updated."""
        if operation not in self.baselines:
            return len(self.performance_data[operation]) >= self.min_samples_for_baseline
        
        baseline = self.baselines[operation]
        
        # Update if baseline is old (> 7 days) and we have enough new data
        age_days = (time.time() - baseline.last_updated) / (24 * 3600)
        if age_days > 7 and len(self.performance_data[operation]) >= self.min_samples_for_baseline:
            return True
        
        return False
    
    def _update_baseline(self, operation: str) -> bool:
        """Update baseline for an operation."""
        data = list(self.performance_data[operation])
        
        if len(data) < self.min_samples_for_baseline:
            return False
        
        # Use recent stable data for baseline
        baseline_data = data[-self.baseline_window_size:]
        
        # Calculate baseline metrics
        durations = [m.duration for m in baseline_data]
        throughputs = [m.throughput_items_per_sec for m in baseline_data]
        memory_usage = [m.memory_peak for m in baseline_data]
        
        # Calculate confidence intervals
        metrics = {
            "duration": statistics.mean(durations),
            "throughput": statistics.mean(throughputs),
            "memory_usage": statistics.mean(memory_usage)
        }
        
        confidence_intervals = {
            "duration": self.analyzer.calculate_confidence_interval(durations),
            "throughput": self.analyzer.calculate_confidence_interval(throughputs),
            "memory_usage": self.analyzer.calculate_confidence_interval(memory_usage)
        }
        
        # Create baseline
        baseline = RegressionBaseline(
            operation_name=operation,
            metrics=metrics,
            confidence_intervals=confidence_intervals,
            sample_count=len(baseline_data),
            established_at=time.time(),
            last_updated=time.time()
        )
        
        self.baselines[operation] = baseline
        
        # Save baselines
        self.save_baselines()
        
        log.info("Updated baseline for %s with %d samples", operation, len(baseline_data))
        return True
    
    def _detect_regressions(self, operation: str, metrics: PerformanceMetrics) -> List[RegressionDetection]:
        """Detect regressions in current metrics."""
        if operation not in self.baselines:
            return []
        
        baseline = self.baselines[operation]
        regressions = []
        
        # Check each metric
        current_metrics = {
            "duration": metrics.duration,
            "throughput": metrics.throughput_items_per_sec,
            "memory_usage": metrics.memory_peak
        }
        
        for metric_name, current_value in current_metrics.items():
            if baseline.is_regression(current_value, metric_name):
                baseline_value = baseline.metrics[metric_name]
                regression_magnitude = self.analyzer.calculate_regression_magnitude(
                    baseline_value, current_value
                )
                
                # Determine severity
                severity = self._determine_severity(abs(regression_magnitude))
                
                regression = RegressionDetection(
                    operation_name=operation,
                    metric_name=metric_name,
                    baseline_value=baseline_value,
                    current_value=current_value,
                    regression_magnitude=regression_magnitude,
                    confidence_level=0.95,  # Based on confidence interval
                    detection_time=time.time(),
                    severity=severity
                )
                
                regressions.append(regression)
                
                log.warning(
                    "🔍 Regression detected: %s.%s = %.2f (baseline: %.2f, change: %.1f%%)",
                    operation, metric_name, current_value, baseline_value, regression_magnitude
                )
        
        return regressions
    
    def _determine_severity(self, magnitude: float) -> str:
        """Determine severity based on regression magnitude."""
        if magnitude > 100:  # More than 100% change
            return "critical"
        elif magnitude > 50:  # More than 50% change
            return "major"
        elif magnitude > 20:  # More than 20% change
            return "moderate"
        else:
            return "minor"
    
    def _calculate_stability_score(self, data: List[PerformanceMetrics]) -> float:
        """Calculate stability score for an operation (0-1, higher is better)."""
        if len(data) < 3:
            return 0.0
        
        # Calculate coefficient of variation for key metrics
        durations = [m.duration for m in data]
        throughputs = [m.throughput_items_per_sec for m in data]
        
        duration_cv = statistics.stdev(durations) / statistics.mean(durations)
        throughput_cv = statistics.stdev(throughputs) / statistics.mean(throughputs) if statistics.mean(throughputs) > 0 else 0
        
        # Lower CV means higher stability
        avg_cv = (duration_cv + throughput_cv) / 2
        stability_score = max(0, 1 - avg_cv)
        
        return stability_score
    
    def _generate_regression_recommendations(self, regressions: List[RegressionDetection]) -> List[str]:
        """Generate recommendations based on detected regressions."""
        recommendations = []
        
        # Group by operation
        by_operation = defaultdict(list)
        for regression in regressions:
            by_operation[regression.operation_name].append(regression)
        
        # Generate operation-specific recommendations
        for operation, op_regressions in by_operation.items():
            metric_types = {r.metric_name for r in op_regressions}
            
            if "duration" in metric_types:
                recommendations.append(
                    f"Operation '{operation}' showing increased duration - check for resource contention or inefficient processing"
                )
            
            if "throughput" in metric_types:
                recommendations.append(
                    f"Operation '{operation}' showing decreased throughput - consider scaling resources or optimizing algorithms"
                )
            
            if "memory_usage" in metric_types:
                recommendations.append(
                    f"Operation '{operation}' showing increased memory usage - check for memory leaks or inefficient data structures"
                )
        
        # Generate general recommendations
        critical_regressions = [r for r in regressions if r.severity == "critical"]
        if critical_regressions:
            recommendations.append(
                f"Critical regressions detected in {len(critical_regressions)} operations - immediate investigation recommended"
            )
        
        return recommendations


# Global regression detector instance
_global_detector = PerformanceRegressionDetector()


def get_global_detector() -> PerformanceRegressionDetector:
    """Get the global regression detector."""
    return _global_detector


def configure_global_detector(
    baseline_window_size: int = 50,
    detection_window_size: int = 10,
    min_samples_for_baseline: int = 20,
    baseline_file: Optional[Path] = None
) -> PerformanceRegressionDetector:
    """Configure the global regression detector."""
    global _global_detector
    _global_detector = PerformanceRegressionDetector(
        baseline_window_size=baseline_window_size,
        detection_window_size=detection_window_size,
        min_samples_for_baseline=min_samples_for_baseline,
        baseline_file=baseline_file
    )
    return _global_detector


def regression_monitored(operation_name: str):
    """Decorator to automatically monitor functions for performance regressions."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            import psutil
            from .performance_optimizer import PerformanceMetrics
            
            detector = get_global_detector()
            
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
                
                # Check for regressions
                regressions = detector.record_performance(metrics)
                if regressions:
                    for regression in regressions:
                        if regression.is_significant:
                            log.warning("🔍 Performance regression detected in %s", operation_name)
                
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
                
                detector.record_performance(metrics)
                raise
        
        return wrapper
    return decorator