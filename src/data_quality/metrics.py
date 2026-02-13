"""
Data Quality Metrics Module
============================
Comprehensive data quality metrics collection and reporting.

Metrics Categories:
- Completeness: Missing values, null rates
- Validity: Data type conformance, range violations
- Consistency: Cross-field validations
- Accuracy: Statistical anomalies
- Timeliness: Data freshness, ingestion lag
"""

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DataQualityMetrics:
    """Container for data quality metrics"""

    # Metadata
    table_name: str
    layer: str  # bronze, silver, gold
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Row counts
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0

    # Completeness metrics
    null_counts: dict[str, int] = field(default_factory=dict)
    null_percentages: dict[str, float] = field(default_factory=dict)
    completeness_score: float = 0.0

    # Validity metrics
    type_violations: dict[str, int] = field(default_factory=dict)
    range_violations: dict[str, int] = field(default_factory=dict)
    validity_score: float = 0.0

    # Consistency metrics
    consistency_checks: dict[str, bool] = field(default_factory=dict)
    consistency_score: float = 0.0

    # Accuracy metrics (anomalies)
    anomalies_detected: int = 0
    anomaly_details: list[dict[str, Any]] = field(default_factory=list)
    accuracy_score: float = 0.0

    # Timeliness metrics
    data_freshness_hours: float | None = None
    ingestion_lag_seconds: float | None = None
    timeliness_score: float = 0.0

    # Overall quality score (0-100)
    overall_quality_score: float = 0.0

    # Quality assessment
    quality_level: str = "UNKNOWN"  # EXCELLENT, GOOD, FAIR, POOR, CRITICAL

    # Additional metadata
    checks_passed: int = 0
    checks_failed: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def calculate_overall_score(self):
        """Calculate overall quality score from component scores"""
        scores = [
            self.completeness_score,
            self.validity_score,
            self.consistency_score,
            self.accuracy_score,
            self.timeliness_score,
        ]

        # Weight the scores (can be customized)
        weights = [0.25, 0.25, 0.20, 0.20, 0.10]

        # Filter out zero scores
        weighted_scores = [s * w for s, w in zip(scores, weights) if s > 0]
        total_weight = sum(w for s, w in zip(scores, weights) if s > 0)

        if total_weight > 0:
            self.overall_quality_score = sum(weighted_scores) / total_weight
        else:
            self.overall_quality_score = 0.0

        # Determine quality level
        if self.overall_quality_score >= 95:
            self.quality_level = "EXCELLENT"
        elif self.overall_quality_score >= 85:
            self.quality_level = "GOOD"
        elif self.overall_quality_score >= 70:
            self.quality_level = "FAIR"
        elif self.overall_quality_score >= 50:
            self.quality_level = "POOR"
        else:
            self.quality_level = "CRITICAL"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        # Convert datetime to ISO string
        data["timestamp"] = self.timestamp.isoformat()
        return data

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)

    def get_summary(self) -> str:
        """Get human-readable summary"""
        return f"""
Data Quality Report: {self.table_name} ({self.layer})
{"=" * 80}
Timestamp: {self.timestamp.isoformat()}

Overall Quality: {self.overall_quality_score:.2f}/100 ({self.quality_level})

Row Statistics:
  Total Rows:   {self.total_rows:,}
  Valid Rows:   {self.valid_rows:,} ({self.valid_rows / self.total_rows * 100:.1f}%)
  Invalid Rows: {self.invalid_rows:,} ({self.invalid_rows / self.total_rows * 100:.1f}%)

Quality Scores:
  Completeness: {self.completeness_score:.2f}/100
  Validity:     {self.validity_score:.2f}/100
  Consistency:  {self.consistency_score:.2f}/100
  Accuracy:     {self.accuracy_score:.2f}/100
  Timeliness:   {self.timeliness_score:.2f}/100

Checks:
  Passed: {self.checks_passed}
  Failed: {self.checks_failed}

Warnings: {len(self.warnings)}
Errors:   {len(self.errors)}

{"=" * 80}
        """.strip()


class DataQualityMetricsCollector:
    """Collects and aggregates data quality metrics"""

    def __init__(self, table_name: str, layer: str):
        """Initialize metrics collector

        Args:
            table_name: Name of table being validated
            layer: Data layer (bronze, silver, gold)
        """
        self.table_name = table_name
        self.layer = layer
        self.metrics = DataQualityMetrics(table_name=table_name, layer=layer)

    def set_row_counts(self, total: int, valid: int, invalid: int):
        """Set row count metrics"""
        self.metrics.total_rows = total
        self.metrics.valid_rows = valid
        self.metrics.invalid_rows = invalid

    def add_null_counts(self, column_name: str, null_count: int, total_count: int):
        """Add null count metrics for a column"""
        self.metrics.null_counts[column_name] = null_count

        if total_count > 0:
            null_pct = (null_count / total_count) * 100
            self.metrics.null_percentages[column_name] = round(null_pct, 2)

    def calculate_completeness_score(self):
        """Calculate completeness score based on null percentages"""
        if not self.metrics.null_percentages:
            self.metrics.completeness_score = 100.0
            return

        # Average null percentage across all columns
        avg_null_pct = sum(self.metrics.null_percentages.values()) / len(self.metrics.null_percentages)

        # Score is inverse of null percentage
        self.metrics.completeness_score = max(0, 100 - avg_null_pct)

    def add_type_violation(self, column_name: str, count: int):
        """Add type violation count for a column"""
        self.metrics.type_violations[column_name] = count

    def add_range_violation(self, column_name: str, count: int):
        """Add range violation count for a column"""
        self.metrics.range_violations[column_name] = count

    def calculate_validity_score(self):
        """Calculate validity score based on violations"""
        if self.metrics.total_rows == 0:
            self.metrics.validity_score = 100.0
            return

        # Count total violations
        total_violations = sum(self.metrics.type_violations.values()) + sum(self.metrics.range_violations.values())

        # Calculate violation rate
        violation_rate = (total_violations / self.metrics.total_rows) * 100

        # Score is inverse of violation rate
        self.metrics.validity_score = max(0, 100 - violation_rate)

    def add_consistency_check(self, check_name: str, passed: bool):
        """Add consistency check result"""
        self.metrics.consistency_checks[check_name] = passed

        if passed:
            self.metrics.checks_passed += 1
        else:
            self.metrics.checks_failed += 1

    def calculate_consistency_score(self):
        """Calculate consistency score based on checks"""
        if not self.metrics.consistency_checks:
            self.metrics.consistency_score = 100.0
            return

        passed = sum(1 for v in self.metrics.consistency_checks.values() if v)
        total = len(self.metrics.consistency_checks)

        self.metrics.consistency_score = (passed / total) * 100

    def add_anomaly(self, column_name: str, anomaly_type: str, details: dict[str, Any]):
        """Add detected anomaly"""
        self.metrics.anomalies_detected += 1
        self.metrics.anomaly_details.append({"column": column_name, "type": anomaly_type, "details": details})

    def calculate_accuracy_score(self):
        """Calculate accuracy score based on anomalies"""
        if self.metrics.total_rows == 0:
            self.metrics.accuracy_score = 100.0
            return

        # Accuracy decreases with anomalies
        # Assume each anomaly affects some rows
        estimated_affected_rows = self.metrics.anomalies_detected * 100  # Rough estimate

        if estimated_affected_rows >= self.metrics.total_rows:
            self.metrics.accuracy_score = 50.0  # Cap at 50 if many anomalies
        else:
            anomaly_rate = (estimated_affected_rows / self.metrics.total_rows) * 100
            self.metrics.accuracy_score = max(50, 100 - anomaly_rate)

    def set_timeliness_metrics(self, freshness_hours: float | None = None, ingestion_lag_seconds: float | None = None):
        """Set timeliness metrics"""
        self.metrics.data_freshness_hours = freshness_hours
        self.metrics.ingestion_lag_seconds = ingestion_lag_seconds

        # Calculate timeliness score
        score = 100.0

        # Deduct points for stale data (>24 hours)
        if freshness_hours is not None and freshness_hours > 24:
            score -= min(50, (freshness_hours - 24) * 2)  # Deduct up to 50 points

        # Deduct points for slow ingestion (>60 seconds)
        if ingestion_lag_seconds is not None and ingestion_lag_seconds > 60:
            score -= min(25, (ingestion_lag_seconds - 60) / 10)  # Deduct up to 25 points

        self.metrics.timeliness_score = max(0, score)

    def add_warning(self, message: str):
        """Add warning message"""
        self.metrics.warnings.append(message)
        logger.warning(message)

    def add_error(self, message: str):
        """Add error message"""
        self.metrics.errors.append(message)
        logger.error(message)

    def finalize(self) -> DataQualityMetrics:
        """Finalize metrics calculation and return"""
        # Calculate all scores
        self.calculate_completeness_score()
        self.calculate_validity_score()
        self.calculate_consistency_score()
        self.calculate_accuracy_score()

        # Calculate overall score
        self.metrics.calculate_overall_score()

        return self.metrics


class DataQualityReporter:
    """Reports and persists data quality metrics"""

    def __init__(self, output_dir: str = "logs/data_quality"):
        """Initialize reporter

        Args:
            output_dir: Directory to write quality reports
        """
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        from pathlib import Path

        Path(output_dir).mkdir(parents=True, exist_ok=True)

    def save_metrics(self, metrics: DataQualityMetrics, format: str = "json"):
        """Save metrics to file

        Args:
            metrics: DataQualityMetrics object
            format: Output format ('json' or 'txt')
        """
        from pathlib import Path

        # Generate filename
        timestamp = metrics.timestamp.strftime("%Y%m%d_%H%M%S")
        base_name = f"dq_{metrics.layer}_{metrics.table_name}_{timestamp}"

        if format == "json":
            file_path = Path(self.output_dir) / f"{base_name}.json"
            with open(file_path, "w") as f:
                f.write(metrics.to_json())
            logger.info(f"Saved quality metrics to: {file_path}")

        elif format == "txt":
            file_path = Path(self.output_dir) / f"{base_name}.txt"
            with open(file_path, "w") as f:
                f.write(metrics.get_summary())
            logger.info(f"Saved quality report to: {file_path}")

        return file_path

    def print_summary(self, metrics: DataQualityMetrics):
        """Print metrics summary to console"""
        print(metrics.get_summary())

    def get_recent_metrics(self, table_name: str, layer: str, limit: int = 10) -> list[DataQualityMetrics]:
        """Get recent metrics for a table

        Args:
            table_name: Table name
            layer: Data layer
            limit: Number of recent metrics to retrieve

        Returns:
            List of DataQualityMetrics objects
        """
        import glob
        from pathlib import Path

        # Find matching files
        pattern = f"dq_{layer}_{table_name}_*.json"
        files = glob.glob(str(Path(self.output_dir) / pattern))

        # Sort by modification time (most recent first)
        files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)

        # Load recent metrics
        metrics_list = []
        for file_path in files[:limit]:
            try:
                with open(file_path) as f:
                    data = json.load(f)
                    # Reconstruct DataQualityMetrics object
                    # Note: This is a simple reconstruction; full implementation would need more care
                    metrics_list.append(data)
            except Exception as e:
                logger.error(f"Failed to load metrics from {file_path}: {e}")

        return metrics_list

    def compare_metrics(self, current: DataQualityMetrics, previous: DataQualityMetrics) -> dict[str, Any]:
        """Compare current metrics with previous run

        Args:
            current: Current metrics
            previous: Previous metrics

        Returns:
            Dictionary with comparison results
        """
        comparison = {
            "overall_score_change": current.overall_quality_score - previous.overall_quality_score,
            "completeness_change": current.completeness_score - previous.completeness_score,
            "validity_change": current.validity_score - previous.validity_score,
            "consistency_change": current.consistency_score - previous.consistency_score,
            "accuracy_change": current.accuracy_score - previous.accuracy_score,
            "timeliness_change": current.timeliness_score - previous.timeliness_score,
            "row_count_change": current.total_rows - previous.total_rows,
            "anomalies_change": current.anomalies_detected - previous.anomalies_detected,
            "quality_level_change": f"{previous.quality_level} → {current.quality_level}",
            "trend": "IMPROVING"
            if current.overall_quality_score > previous.overall_quality_score
            else "DEGRADING"
            if current.overall_quality_score < previous.overall_quality_score
            else "STABLE",
        }

        return comparison
