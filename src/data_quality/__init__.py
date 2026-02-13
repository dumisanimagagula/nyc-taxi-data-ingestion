"""
Data Quality Module
===================
Comprehensive data quality framework for the lakehouse platform.

Components:
- Metrics: Collect and report quality metrics
- Error Tracking: Row-level error tracking and persistence
- Reconciliation: Cross-layer data reconciliation
- Anomaly Detection: Statistical anomaly detection
- Great Expectations: Expectation-based validation

Usage:
    from src.data_quality import DataQualityOrchestrator

    orchestrator = DataQualityOrchestrator(spark, config)
    results = orchestrator.validate_table(df, 'bronze', 'taxi_trips')
"""

__version__ = "1.0.0"

from src.data_quality.anomaly_detection import (
    Anomaly,
    AnomalyDetector,
    TimeSeriesAnomalyDetector,
    run_comprehensive_anomaly_detection,
)
from src.data_quality.error_tracking import ErrorPersistence, ErrorTracker, RowError
from src.data_quality.great_expectations import (
    GE_AVAILABLE,
    ExpectationResult,
    ExpectationSuite,
    ExpectationValidator,
    create_standard_expectations,
)
from src.data_quality.lineage import (
    ColumnLineage,
    LineageEvent,
    LineageEventType,
    LineageTracker,
    extract_column_lineage_from_config,
    query_lineage,
)
from src.data_quality.metrics import DataQualityMetrics, DataQualityMetricsCollector, DataQualityReporter
from src.data_quality.orchestrator import DataQualityOrchestrator
from src.data_quality.reconciliation import (
    ReconciliationChecker,
    ReconciliationResult,
    reconcile_bronze_to_silver,
    reconcile_silver_to_gold,
)

__all__ = [
    # Metrics
    "DataQualityMetrics",
    "DataQualityMetricsCollector",
    "DataQualityReporter",
    # Error Tracking
    "RowError",
    "ErrorTracker",
    "ErrorPersistence",
    # Reconciliation
    "ReconciliationResult",
    "ReconciliationChecker",
    "reconcile_bronze_to_silver",
    "reconcile_silver_to_gold",
    # Anomaly Detection
    "Anomaly",
    "AnomalyDetector",
    "TimeSeriesAnomalyDetector",
    "run_comprehensive_anomaly_detection",
    # Great Expectations
    "ExpectationResult",
    "ExpectationSuite",
    "ExpectationValidator",
    "create_standard_expectations",
    "GE_AVAILABLE",
    # Lineage
    "LineageEventType",
    "ColumnLineage",
    "LineageEvent",
    "LineageTracker",
    "extract_column_lineage_from_config",
    "query_lineage",
    # Orchestrator
    "DataQualityOrchestrator",
]
