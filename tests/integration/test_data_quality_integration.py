"""
Integration Tests for Data Quality Framework

Tests the integration between data quality components:
- Metrics + Error Tracking + Anomaly Detection
- Orchestrator + Individual Modules
- Persistence to storage (simulated)
"""

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.data_quality import (
    AnomalyDetector,
    DataQualityMetricsCollector,
    DataQualityOrchestrator,
    ErrorTracker,
    ReconciliationChecker,
)


@pytest.mark.integration
@pytest.mark.data_quality
class TestDataQualityIntegration:
    """Integration tests for data quality framework"""

    def test_orchestrator_end_to_end(
        self,
        spark_session: SparkSession,
        sample_taxi_data_with_quality_issues: pd.DataFrame,
        sample_lakehouse_config: dict,
        mock_pipeline_run_id: str,
        temp_output_dir,
    ):
        """Test complete orchestrator workflow with all components"""
        # Convert pandas to Spark DataFrame
        spark_df = spark_session.createDataFrame(sample_taxi_data_with_quality_issues)

        # Create orchestrator
        dq_config = sample_lakehouse_config["data_quality"]
        dq_config["metrics"] = {"save_to_disk": True, "output_directory": str(temp_output_dir)}

        orchestrator = DataQualityOrchestrator(
            spark=spark_session, config=dq_config, pipeline_run_id=mock_pipeline_run_id
        )

        # Run validation
        result = orchestrator.validate_table(df=spark_df, table_name="test.bronze.taxi_raw", layer="bronze")

        # Assert results structure
        assert "overall_score" in result
        assert "quality_level" in result
        assert "passed" in result
        assert "total_errors" in result
        assert "anomaly_count" in result

        # Assert quality score is calculated
        assert 0 <= result["overall_score"] <= 100

        # Assert errors were detected (we introduced quality issues)
        assert result["total_errors"] > 0

        # Assert quality level is set
        assert result["quality_level"] in ["EXCELLENT", "GOOD", "FAIR", "POOR", "CRITICAL"]

    def test_metrics_and_error_tracking_integration(
        self, spark_session: SparkSession, sample_taxi_data_with_quality_issues: pd.DataFrame, mock_pipeline_run_id: str
    ):
        """Test integration between metrics collector and error tracker"""
        # Convert to Spark DataFrame
        spark_df = spark_session.createDataFrame(sample_taxi_data_with_quality_issues)

        # Create metrics collector
        metrics_collector = DataQualityMetricsCollector()

        # Create error tracker
        error_tracker = ErrorTracker(max_errors=100)

        # Collect null metrics
        null_counts = {}
        for col in spark_df.columns:
            null_count = spark_df.filter(spark_df[col].isNull()).count()
            null_counts[col] = null_count
            if null_count > 0:
                # Track errors for null values
                error_tracker.track_error(
                    table_name="test_table",
                    column=col,
                    error_type="NULL_VALUE",
                    value=None,
                    message=f"{col} contains {null_count} null values",
                    severity="WARNING",
                )

        metrics_collector.metrics.null_counts = null_counts

        # Finalize metrics
        metrics_collector.finalize_metrics(
            table_name="test_table", layer="bronze", row_count=spark_df.count(), column_count=len(spark_df.columns)
        )

        # Assert metrics and errors are correlated
        assert len(error_tracker.errors) > 0
        assert metrics_collector.metrics.null_counts

        # Errors should exist for columns with nulls
        error_columns = {error.column for error in error_tracker.errors}
        null_columns = {col for col, count in null_counts.items() if count > 0}
        assert error_columns == null_columns

    def test_anomaly_detection_with_metrics(self, spark_session: SparkSession, sample_taxi_data: pd.DataFrame):
        """Test anomaly detection integration with metrics"""
        # Introduce outliers
        outlier_df = sample_taxi_data.copy()
        outlier_df.loc[0, "fare_amount"] = 10000.0  # Extreme outlier
        outlier_df.loc[1, "trip_distance"] = 999.0  # Extreme outlier

        spark_df = spark_session.createDataFrame(outlier_df)

        # Create anomaly detector
        detector = AnomalyDetector()

        # Detect anomalies
        fare_anomalies = detector.detect_numeric_iqr(spark_df, "fare_amount")
        distance_anomalies = detector.detect_numeric_iqr(spark_df, "trip_distance")

        # Create metrics collector and add anomalies
        metrics_collector = DataQualityMetricsCollector()

        for anomaly in fare_anomalies:
            metrics_collector.add_anomaly(
                column_name=anomaly.column,
                anomaly_type=anomaly.detection_method,
                details={"value": anomaly.value, "severity": anomaly.severity, "message": anomaly.message},
            )

        for anomaly in distance_anomalies:
            metrics_collector.add_anomaly(
                column_name=anomaly.column,
                anomaly_type=anomaly.detection_method,
                details={"value": anomaly.value, "severity": anomaly.severity, "message": anomaly.message},
            )

        # Finalize metrics
        metrics_collector.finalize_metrics(
            table_name="test_table", layer="bronze", row_count=spark_df.count(), column_count=len(spark_df.columns)
        )

        # Assert anomalies were recorded
        total_anomalies = len(fare_anomalies) + len(distance_anomalies)
        assert metrics_collector.metrics.anomaly_count == total_anomalies
        assert len(metrics_collector.metrics.anomaly_details) == total_anomalies

    def test_reconciliation_integration(self, spark_session: SparkSession, sample_taxi_data: pd.DataFrame):
        """Test reconciliation between source and target tables"""
        # Create source table
        source_df = spark_session.createDataFrame(sample_taxi_data)
        source_df.createOrReplaceTempView("test_source")

        # Create target table (filtering some records)
        target_df = source_df.filter(source_df.fare_amount > 10.0)
        target_df.createOrReplaceTempView("test_target")

        # Create reconciliation checker
        checker = ReconciliationChecker(spark_session)

        # Row count reconciliation
        row_count_result = checker.check_row_count(
            source_table="test_source",
            target_table="test_target",
            tolerance_pct=50.0,  # Allow 50% difference due to filtering
        )

        assert row_count_result.source_value == source_df.count()
        assert row_count_result.target_value == target_df.count()
        assert row_count_result.source_value > row_count_result.target_value
        assert row_count_result.passed  # Should pass with 50% tolerance

        # Aggregation reconciliation
        agg_result = checker.check_aggregation(
            source_table="test_source",
            target_table="test_target",
            column="trip_distance",
            aggregation="sum",
            tolerance_pct=100.0,  # High tolerance since we filtered rows
        )

        assert agg_result.source_value > 0
        assert agg_result.target_value > 0
        assert agg_result.source_value > agg_result.target_value


@pytest.mark.integration
@pytest.mark.config
class TestConfigurationIntegration:
    """Integration tests for configuration management"""

    def test_config_validation_with_schema(self, sample_lakehouse_config: dict):
        """Test configuration validation against JSON schema"""
        from src.config_validator import ConfigValidator

        # Create validator
        validator = ConfigValidator(schema_version="v1")

        # Validate configuration
        is_valid, errors = validator.validate(sample_lakehouse_config)

        # Assert validation passes
        assert is_valid, f"Validation errors: {errors}"
        assert len(errors) == 0

    def test_environment_config_merging(self, sample_lakehouse_config: dict, tmp_path):
        """Test environment-specific configuration merging"""
        import yaml

        from src.environment_config_manager import EnvironmentConfigManager

        # Create base config file
        base_config_path = tmp_path / "base_config.yaml"
        with open(base_config_path, "w") as f:
            yaml.dump(sample_lakehouse_config, f)

        # Create environment override
        dev_override = {
            "bronze": {
                "target": {
                    "database": "bronze_dev"  # Override for dev
                }
            }
        }
        dev_config_path = tmp_path / "dev_config.yaml"
        with open(dev_config_path, "w") as f:
            yaml.dump(dev_override, f)

        # Create manager
        manager = EnvironmentConfigManager(base_config_path=str(base_config_path), environment="dev")

        # Add environment config
        manager.add_environment_config("dev", str(dev_config_path))

        # Get merged config
        merged_config = manager.get_merged_config()

        # Assert override was applied
        assert merged_config["bronze"]["target"]["database"] == "bronze_dev"
        # Assert other values remain from base
        assert merged_config["pipeline"]["name"] == sample_lakehouse_config["pipeline"]["name"]


@pytest.mark.integration
@pytest.mark.lineage
class TestLineageIntegration:
    """Integration tests for data lineage tracking"""

    def test_lineage_end_to_end(self, spark_session: SparkSession, mock_pipeline_run_id: str):
        """Test end-to-end lineage tracking"""
        from src.data_quality.lineage import LineageEventType, LineageTracker

        # Create tracker
        tracker = LineageTracker(spark=spark_session, pipeline_run_id=mock_pipeline_run_id)

        # Record Bronze ingestion
        tracker.record_ingestion(
            source_url="https://example.com/data.parquet", target_table="test.bronze.taxi_raw", row_count=1000
        )

        # Record Silver transformation
        tracker.record_transformation(
            source_table="test.bronze.taxi_raw",
            target_table="test.silver.taxi_clean",
            layer="silver",
            event_type=LineageEventType.TRANSFORMATION,
            source_row_count=1000,
            target_row_count=950,
        )

        # Record Gold aggregation
        tracker.record_aggregation(
            source_table="test.silver.taxi_clean",
            target_table="test.gold.daily_stats",
            aggregation_logic="Daily aggregation",
            source_row_count=950,
            target_row_count=31,
        )

        # Assert events were recorded
        assert len(tracker.events) == 3

        # Get lineage graph
        graph = tracker.get_lineage_graph("test.silver.taxi_clean")

        # Assert graph has upstream and downstream
        assert len(graph["upstream"]) == 1
        assert len(graph["downstream"]) == 1
        assert graph["upstream"][0]["source"] == "test.bronze.taxi_raw"
        assert graph["downstream"][0]["target"] == "test.gold.daily_stats"

        # Impact analysis
        impacted = tracker.analyze_impact("test.bronze.taxi_raw")

        # Assert downstream tables are identified
        assert "test.silver.taxi_clean" in impacted
        assert "test.gold.daily_stats" in impacted
