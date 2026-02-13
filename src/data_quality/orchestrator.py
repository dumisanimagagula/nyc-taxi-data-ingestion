"""
Data Quality Orchestrator
==========================
Comprehensive data quality orchestrator that integrates all quality modules.

Features:
- Unified interface for all quality checks
- Configurable quality rules
- Comprehensive reporting
- Row-level error tracking
- Anomaly detection
- Expectation validation
"""

import logging
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.data_quality.anomaly_detection import run_comprehensive_anomaly_detection
from src.data_quality.error_tracking import ErrorPersistence, ErrorTracker
from src.data_quality.great_expectations import ExpectationSuite, ExpectationValidator, create_standard_expectations
from src.data_quality.metrics import DataQualityMetricsCollector, DataQualityReporter
from src.data_quality.reconciliation import ReconciliationChecker

logger = logging.getLogger(__name__)


class DataQualityOrchestrator:
    """Orchestrate comprehensive data quality validation"""

    def __init__(
        self,
        spark: SparkSession,
        config: dict[str, Any] | None = None,
        catalog=None,
        pipeline_run_id: str | None = None,
    ):
        """Initialize data quality orchestrator

        Args:
            spark: Spark session
            config: Configuration with quality rules
            catalog: Optional Iceberg catalog for error persistence
            pipeline_run_id: Optional default pipeline run ID for validations
        """
        self.spark = spark
        self.config = config or {}
        self.catalog = catalog
        self.default_pipeline_run_id = pipeline_run_id

        # Accept either full root config {"data_quality": {...}} or subsection {...}
        if "data_quality" in self.config and isinstance(self.config.get("data_quality"), dict):
            self.quality_config = self.config.get("data_quality", {})
        else:
            self.quality_config = self.config

        self.fail_on_error = self.quality_config.get("fail_on_error", False)
        self.enable_error_tracking = self.quality_config.get("enable_error_tracking", True)
        self.enable_anomaly_detection = self.quality_config.get("enable_anomaly_detection", True)
        self.enable_expectations = self.quality_config.get("enable_expectations", True)

        # Initialize reporter
        report_dir = self.quality_config.get("report_directory", "logs/data_quality")
        self.reporter = DataQualityReporter(output_dir=report_dir)

        logger.info("Data Quality Orchestrator initialized")
        logger.info(f"  - Fail on error: {self.fail_on_error}")
        logger.info(f"  - Error tracking: {self.enable_error_tracking}")
        logger.info(f"  - Anomaly detection: {self.enable_anomaly_detection}")
        logger.info(f"  - Expectations: {self.enable_expectations}")

    def validate_table(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        expectations: ExpectationSuite | None = None,
        pipeline_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Comprehensive data quality validation for a table

        Args:
            df: Spark DataFrame to validate
            layer: Data layer (bronze, silver, gold)
            table_name: Table name
            expectations: Optional expectation suite
            pipeline_run_id: Optional pipeline run ID

        Returns:
            Comprehensive validation results

        Raises:
            DataQualityError: If fail_on_error=True and validation fails
        """
        logger.info("=" * 80)
        logger.info(f"DATA QUALITY VALIDATION: {layer}.{table_name}")
        logger.info("=" * 80)

        start_time = datetime.now(timezone.utc)
        pipeline_run_id = pipeline_run_id or self.default_pipeline_run_id

        # Initialize metrics collector
        metrics_collector = DataQualityMetricsCollector(table_name, layer)

        # Initialize error tracker
        error_tracker = None
        if self.enable_error_tracking:
            error_tracker = ErrorTracker(
                table_name=table_name,
                layer=layer,
                pipeline_run_id=pipeline_run_id,
                max_errors=self.quality_config.get("max_errors_to_track", 1000),
            )

        # Get row count
        total_rows = df.count()
        metrics_collector.set_row_counts(total=total_rows, valid=0, invalid=0)

        logger.info(f"Total rows: {total_rows:,}")

        # 1. Run basic quality checks
        logger.info("\n1. Running basic quality checks...")
        basic_results = self._run_basic_checks(df, metrics_collector, error_tracker)

        # 2. Run anomaly detection
        anomaly_results = {}
        if self.enable_anomaly_detection:
            logger.info("\n2. Running anomaly detection...")
            anomaly_results = self._run_anomaly_detection(df, layer, metrics_collector)

        # 3. Run expectations
        expectation_results = {}
        if self.enable_expectations:
            logger.info("\n3. Running expectations...")
            expectation_results = self._run_expectations(df, table_name, expectations)

        # 4. Finalize metrics
        logger.info("\n4. Finalizing metrics...")
        metrics = metrics_collector.finalize()

        # 5. Save error tracking
        if self.enable_error_tracking and error_tracker:
            logger.info("\n5. Saving error tracking...")
            self._save_error_tracking(error_tracker)

        # 6. Generate report
        logger.info("\n6. Generating report...")
        self.reporter.save_metrics(metrics, format="json")
        self.reporter.save_metrics(metrics, format="txt")

        # Calculate validation duration
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Compile results
        error_summary = error_tracker.get_error_summary() if error_tracker else {}

        results = {
            "table_name": table_name,
            "layer": layer,
            "timestamp": start_time.isoformat(),
            "duration_seconds": round(duration, 2),
            "metrics": metrics.to_dict(),
            "basic_checks": basic_results,
            "anomaly_detection": anomaly_results,
            "expectations": expectation_results,
            "error_tracking": error_summary,
            "overall_success": metrics.overall_quality_score >= self.quality_config.get("min_quality_score", 70.0),
            # Backward-compatible aliases for existing consumers/tests
            "passed": metrics.overall_quality_score >= self.quality_config.get("min_quality_score", 70.0),
            "overall_score": metrics.overall_quality_score,
            "quality_level": metrics.quality_level,
            "total_errors": int(error_summary.get("total_errors", 0)),
            "anomaly_count": int(metrics.anomalies_detected),
        }

        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Overall Quality Score: {metrics.overall_quality_score:.2f}/100 ({metrics.quality_level})")
        logger.info(f"Duration: {duration:.2f}s")
        logger.info(f"Success: {results['overall_success']}")
        logger.info("=" * 80)

        # Check if we should fail
        if self.fail_on_error and not results["overall_success"]:
            from src.exceptions import DataQualityError

            raise DataQualityError(
                f"Data quality validation failed for {layer}.{table_name}: "
                f"score {metrics.overall_quality_score:.2f}/100 "
                f"(minimum required: {self.quality_config.get('min_quality_score', 70.0)})"
            )

        return results

    def _run_basic_checks(
        self, df: DataFrame, metrics_collector: DataQualityMetricsCollector, error_tracker: ErrorTracker | None
    ) -> dict[str, Any]:
        """Run basic quality checks (nulls, types, ranges)"""

        # Get column checks from config
        layer_config = self.quality_config.get("checks", {})
        column_checks = layer_config.get("columns", {})

        results = {"null_checks": {}, "type_checks": {}, "range_checks": {}}

        # For each column with checks defined
        for column, checks in column_checks.items():
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue

            # Null check
            if checks.get("allow_null", True) == False:
                null_count = df.filter(df[column].isNull()).count()
                total_count = df.count()

                metrics_collector.add_null_counts(column, null_count, total_count)

                results["null_checks"][column] = {
                    "null_count": null_count,
                    "null_percentage": round(null_count / total_count * 100, 2) if total_count > 0 else 0,
                    "passed": null_count == 0,
                }

                # Track errors
                if null_count > 0 and error_tracker:
                    # Track sample of null errors
                    null_rows = df.filter(df[column].isNull()).limit(100).collect()
                    for row in null_rows:
                        error_tracker.track_error(
                            error_type="NULL_VALUE",
                            error_message=f"Null value in {column}",
                            column_name=column,
                            row_data=row.asDict(),
                            severity="ERROR",
                            check_name="null_check",
                        )

            # Range check
            if "min" in checks or "max" in checks:
                min_val = checks.get("min")
                max_val = checks.get("max")

                # Count violations
                violation_count = 0
                if min_val is not None and max_val is not None:
                    violation_count = df.filter((df[column] < min_val) | (df[column] > max_val)).count()
                elif min_val is not None:
                    violation_count = df.filter(df[column] < min_val).count()
                elif max_val is not None:
                    violation_count = df.filter(df[column] > max_val).count()

                metrics_collector.add_range_violation(column, violation_count)

                results["range_checks"][column] = {
                    "violation_count": violation_count,
                    "min": min_val,
                    "max": max_val,
                    "passed": violation_count == 0,
                }

        # Calculate validity score
        metrics_collector.calculate_validity_score()

        return results

    def _run_anomaly_detection(
        self, df: DataFrame, layer: str, metrics_collector: DataQualityMetricsCollector
    ) -> dict[str, Any]:
        """Run anomaly detection"""

        # Get anomaly detection config
        anomaly_config = self.quality_config.get("anomaly_detection", {})

        # Get numeric and categorical columns
        numeric_columns = anomaly_config.get("numeric_columns", [])
        categorical_columns = anomaly_config.get("categorical_columns", [])

        # Run detection
        results = run_comprehensive_anomaly_detection(
            df=df, numeric_columns=numeric_columns, categorical_columns=categorical_columns, config=anomaly_config
        )

        # Add anomalies to metrics
        for anomaly in results.get("anomalies", []):
            metrics_collector.add_anomaly(
                column_name=anomaly["column"], anomaly_type=anomaly["method"], details=anomaly
            )

        # Calculate accuracy score
        metrics_collector.calculate_accuracy_score()

        return results

    def _run_expectations(
        self, df: DataFrame, table_name: str, custom_expectations: ExpectationSuite | None
    ) -> dict[str, Any]:
        """Run Great Expectations validation"""

        # Get or create expectation suite
        if custom_expectations:
            suite = custom_expectations
        else:
            # Try to get standard expectations for table type
            table_type = self.quality_config.get("table_type", "generic")
            if table_type == "taxi_trips":
                suite = create_standard_expectations("taxi_trips")
            else:
                # No expectations defined
                logger.info("No expectations defined for this table")
                return {}

        # Validate
        validator = ExpectationValidator(df, use_great_expectations=True)
        results = validator.validate_suite(suite)

        return results

    def _save_error_tracking(self, error_tracker: ErrorTracker):
        """Save error tracking data"""

        if not error_tracker.errors:
            logger.info("No errors to save")
            return

        # Save to CSV for easy viewing
        from pathlib import Path

        csv_dir = Path("logs/data_quality/errors")
        csv_dir.mkdir(parents=True, exist_ok=True)

        csv_path = (
            csv_dir
            / f"errors_{error_tracker.layer}_{error_tracker.table_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        )
        error_tracker.save_to_csv(str(csv_path))

        # Save to Iceberg if catalog available
        if self.catalog and self.quality_config.get("persist_errors_to_iceberg", True):
            try:
                persistence = ErrorPersistence(self.catalog)
                persistence.save_errors(error_tracker)
                logger.info("Errors persisted to Iceberg")
            except Exception as e:
                logger.error(f"Failed to persist errors to Iceberg: {e}")

    def reconcile_layers(
        self,
        source_table: str,
        target_table: str,
        source_layer: str,
        target_layer: str,
        partition_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Reconcile data between layers

        Args:
            source_table: Source table identifier
            target_table: Target table identifier
            source_layer: Source layer name
            target_layer: Target layer name
            partition_filters: Optional partition filters

        Returns:
            Reconciliation summary
        """
        logger.info(f"Reconciling {source_layer}.{source_table} → {target_layer}.{target_table}")

        checker = ReconciliationChecker(self.spark)

        # Get reconciliation config
        recon_config = self.quality_config.get("reconciliation", {})

        # Row count check
        checker.check_row_count(
            source_table,
            target_table,
            source_layer,
            target_layer,
            filters=partition_filters,
            tolerance_pct=recon_config.get("row_count_tolerance_pct", 1.0),
        )

        # Aggregation checks
        for agg_check in recon_config.get("aggregations", []):
            checker.check_aggregation(
                source_table,
                target_table,
                source_layer,
                target_layer,
                agg_column=agg_check["column"],
                agg_function=agg_check.get("function", "sum"),
                filters=partition_filters,
                tolerance_pct=agg_check.get("tolerance_pct", 0.1),
            )

        # Key integrity check
        if "key_columns" in recon_config:
            checker.check_key_integrity(
                source_table,
                target_table,
                source_layer,
                target_layer,
                key_columns=recon_config["key_columns"],
                filters=partition_filters,
            )

        summary = checker.get_summary()
        checker.print_summary()

        return summary
