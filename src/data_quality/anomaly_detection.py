"""
Anomaly Detection Module
=========================
Statistical anomaly detection for data quality monitoring.

Detection Methods:
- Z-Score (standard deviation based)
- IQR (Interquartile Range) - robust to outliers
- Moving average comparison
- Time series anomalies
"""

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class Anomaly:
    """Detected anomaly"""

    column_name: str
    detection_method: str
    value: Any
    expected_range: tuple[Any, Any]
    severity: str  # LOW, MEDIUM,HIGH, CRITICAL
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary"""
        return {
            "column": self.column_name,
            "method": self.detection_method,
            "value": float(self.value) if isinstance(self.value, (int, float)) else str(self.value),
            "expected_min": float(self.expected_range[0]) if isinstance(self.expected_range[0], (int, float)) else None,
            "expected_max": float(self.expected_range[1]) if isinstance(self.expected_range[1], (int, float)) else None,
            "severity": self.severity,
            "details": self.details,
        }


class AnomalyDetector:
    """Detect anomalies using statistical methods"""

    def __init__(self, method: str = "zscore", sensitivity: float = 3.0):
        """Initialize anomaly detector

        Args:
            method: Detection method ('zscore', 'iqr', 'moving_avg')
            sensitivity: Sensitivity threshold
                - For zscore: number of standard deviations (default 3.0)
                - For IQR: multiplier for IQR (default 1.5)
        """
        self.method = method
        self.sensitivity = sensitivity
        self.anomalies: list[Anomaly] = []

    def detect_zscore_anomalies(self, df: DataFrame, column: str) -> list[Anomaly]:
        """Detect anomalies using Z-score method

        Args:
            df: Spark DataFrame
            column: Numeric column to analyze

        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting Z-score anomalies in column: {column}")

        # Calculate statistics
        stats = df.select(
            F.mean(column).alias("mean"),
            F.stddev(column).alias("stddev"),
            F.min(column).alias("min"),
            F.max(column).alias("max"),
            F.count(column).alias("count"),
        ).collect()[0]

        mean = stats["mean"]
        stddev = stats["stddev"]

        if mean is None or stddev is None or stddev == 0:
            logger.warning(f"Cannot calculate Z-score for {column}: invalid statistics")
            return []

        # Define bounds
        lower_bound = mean - (self.sensitivity * stddev)
        upper_bound = mean + (self.sensitivity * stddev)

        # Find anomalies
        anomaly_df = df.filter((F.col(column) < lower_bound) | (F.col(column) > upper_bound))

        anomaly_count = anomaly_df.count()

        if anomaly_count > 0:
            # Calculate z-scores for anomalous values
            anomaly_values = anomaly_df.select(column).limit(100).collect()

            for row in anomaly_values:
                value = row[column]
                z_score = abs((value - mean) / stddev)

                # Determine severity
                if z_score > 5:
                    severity = "CRITICAL"
                elif z_score > 4:
                    severity = "HIGH"
                elif z_score > 3:
                    severity = "MEDIUM"
                else:
                    severity = "LOW"

                anomaly = Anomaly(
                    column_name=column,
                    detection_method="zscore",
                    value=value,
                    expected_range=(lower_bound, upper_bound),
                    severity=severity,
                    details={
                        "z_score": round(z_score, 2),
                        "mean": round(mean, 2),
                        "stddev": round(stddev, 2),
                        "threshold": self.sensitivity,
                    },
                )

                self.anomalies.append(anomaly)

            logger.warning(f"Found {anomaly_count} Z-score anomalies in {column}")

        return self.anomalies

    def detect_iqr_anomalies(self, df: DataFrame, column: str) -> list[Anomaly]:
        """Detect anomalies using Interquartile Range (IQR) method

        More robust to outliers than Z-score.

        Args:
            df: Spark DataFrame
            column: Numeric column to analyze

        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting IQR anomalies in column: {column}")

        # Calculate quartiles
        quartiles = df.approxQuantile(column, [0.25, 0.75], 0.01)

        if len(quartiles) < 2:
            logger.warning(f"Cannot calculate IQR for {column}: invalid quartiles")
            return []

        q1, q3 = quartiles[0], quartiles[1]
        iqr = q3 - q1

        # Define bounds (typically 1.5 * IQR for outliers, 3.0 * IQR for extreme outliers)
        multiplier = self.sensitivity if self.sensitivity > 0 else 1.5
        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)

        # Find anomalies
        anomaly_df = df.filter((F.col(column) < lower_bound) | (F.col(column) > upper_bound))

        anomaly_count = anomaly_df.count()

        if anomaly_count > 0:
            anomaly_values = anomaly_df.select(column).limit(100).collect()

            for row in anomaly_values:
                value = row[column]

                # Determine severity based on how far from bounds
                distance_from_bounds = min(abs(value - lower_bound), abs(value - upper_bound))
                iqr_distance = distance_from_bounds / iqr if iqr > 0 else 0

                if iqr_distance > 5:
                    severity = "CRITICAL"
                elif iqr_distance > 3:
                    severity = "HIGH"
                elif iqr_distance > 2:
                    severity = "MEDIUM"
                else:
                    severity = "LOW"

                anomaly = Anomaly(
                    column_name=column,
                    detection_method="iqr",
                    value=value,
                    expected_range=(lower_bound, upper_bound),
                    severity=severity,
                    details={"q1": round(q1, 2), "q3": round(q3, 2), "iqr": round(iqr, 2), "multiplier": multiplier},
                )

                self.anomalies.append(anomaly)

            logger.warning(f"Found {anomaly_count} IQR anomalies in {column}")

        return self.anomalies

    def detect_categorical_anomalies(self, df: DataFrame, column: str, min_frequency: float = 0.001) -> list[Anomaly]:
        """Detect anomalies in categorical columns

        Flags rare categories that appear less than min_frequency.

        Args:
            df: Spark DataFrame
            column: Categorical column to analyze
            min_frequency: Minimum frequency threshold (0-1)

        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting categorical anomalies in column: {column}")

        total_count = df.count()

        # Get value counts
        value_counts = df.groupBy(column).count().collect()

        rare_values = []
        for row in value_counts:
            value = row[column]
            count = row["count"]
            frequency = count / total_count

            if frequency < min_frequency:
                rare_values.append({"value": value, "count": count, "frequency": frequency})

                anomaly = Anomaly(
                    column_name=column,
                    detection_method="rare_category",
                    value=value,
                    expected_range=(min_frequency, 1.0),
                    severity="LOW",
                    details={
                        "frequency": round(frequency, 6),
                        "count": count,
                        "total": total_count,
                        "min_frequency_threshold": min_frequency,
                    },
                )

                self.anomalies.append(anomaly)

        if rare_values:
            logger.warning(f"Found {len(rare_values)} rare categories in {column}")

        return self.anomalies

    def detect_null_spike(
        self, df: DataFrame, column: str, historical_null_rate: float | None = None, spike_threshold: float = 2.0
    ) -> list[Anomaly]:
        """Detect sudden spike in null values

        Args:
            df: Spark DataFrame
            column: Column to analyze
            historical_null_rate: Historical null rate (0-1)
            spike_threshold: Multiplier for spike detection

        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting null spikes in column: {column}")

        total_count = df.count()
        null_count = df.filter(F.col(column).isNull()).count()
        current_null_rate = null_count / total_count if total_count > 0 else 0

        # If no historical rate provided, estimate from data
        if historical_null_rate is None:
            # For now, use a conservative estimate
            historical_null_rate = 0.05  # Assume 5% nulls is normal

        # Check for spike
        if current_null_rate > (historical_null_rate * spike_threshold):
            severity = "CRITICAL" if current_null_rate > 0.5 else "HIGH"

            anomaly = Anomaly(
                column_name=column,
                detection_method="null_spike",
                value=current_null_rate,
                expected_range=(0, historical_null_rate * spike_threshold),
                severity=severity,
                details={
                    "current_null_rate": round(current_null_rate, 4),
                    "historical_null_rate": round(historical_null_rate, 4),
                    "spike_threshold": spike_threshold,
                    "null_count": null_count,
                    "total_count": total_count,
                },
            )

            self.anomalies.append(anomaly)
            logger.warning(
                f"Null spike detected in {column}: {current_null_rate:.2%} (historical: {historical_null_rate:.2%})"
            )

        return self.anomalies

    def get_anomaly_summary(self) -> dict[str, Any]:
        """Get summary of detected anomalies"""
        if not self.anomalies:
            return {"total_anomalies": 0, "by_column": {}, "by_method": {}, "by_severity": {}}

        return {
            "total_anomalies": len(self.anomalies),
            "by_column": {
                col: len([a for a in self.anomalies if a.column_name == col])
                for col in set(a.column_name for a in self.anomalies)
            },
            "by_method": {
                method: len([a for a in self.anomalies if a.detection_method == method])
                for method in set(a.detection_method for a in self.anomalies)
            },
            "by_severity": {
                severity: len([a for a in self.anomalies if a.severity == severity])
                for severity in set(a.severity for a in self.anomalies)
            },
        }

    def clear(self):
        """Clear detected anomalies"""
        self.anomalies.clear()


class TimeSeriesAnomalyDetector:
    """Detect anomalies in time series data"""

    def __init__(self, window_size: int = 7, sensitivity: float = 2.0):
        """Initialize time series anomaly detector

        Args:
            window_size: Size of moving window for baseline calculation
            sensitivity: Number of standard deviations for anomaly threshold
        """
        self.window_size = window_size
        self.sensitivity = sensitivity
        self.anomalies: list[Anomaly] = []

    def detect_moving_average_anomalies(
        self, df: pd.DataFrame, value_column: str, time_column: str = "timestamp"
    ) -> list[Anomaly]:
        """Detect anomalies using moving average method

        Args:
            df: Pandas DataFrame with time series data
            value_column: Column with values to analyze
            time_column: Column with timestamps

        Returns:
            List of detected anomalies
        """
        logger.info(f"Detecting moving average anomalies in {value_column}")

        # Sort by time
        df = df.sort_values(time_column)

        # Calculate moving average and rolling std
        df["moving_avg"] = df[value_column].rolling(window=self.window_size, min_periods=1).mean()
        df["rolling_std"] = df[value_column].rolling(window=self.window_size, min_periods=1).std()

        # Calculate bounds
        df["lower_bound"] = df["moving_avg"] - (self.sensitivity * df["rolling_std"])
        df["upper_bound"] = df["moving_avg"] + (self.sensitivity * df["rolling_std"])

        # Detect anomalies
        anomaly_mask = (df[value_column] < df["lower_bound"]) | (df[value_column] > df["upper_bound"])
        anomalies_df = df[anomaly_mask]

        for idx, row in anomalies_df.iterrows():
            value = row[value_column]
            moving_avg = row["moving_avg"]
            rolling_std = row["rolling_std"]

            # Calculate deviation
            deviation = abs(value - moving_avg) / rolling_std if rolling_std > 0 else 0

            # Determine severity
            if deviation > 4:
                severity = "CRITICAL"
            elif deviation > 3:
                severity = "HIGH"
            elif deviation > 2:
                severity = "MEDIUM"
            else:
                severity = "LOW"

            anomaly = Anomaly(
                column_name=value_column,
                detection_method="moving_average",
                value=value,
                expected_range=(row["lower_bound"], row["upper_bound"]),
                severity=severity,
                details={
                    "timestamp": str(row[time_column]),
                    "moving_avg": round(moving_avg, 2),
                    "rolling_std": round(rolling_std, 2),
                    "deviation": round(deviation, 2),
                    "window_size": self.window_size,
                },
            )

            self.anomalies.append(anomaly)

        if len(anomalies_df) > 0:
            logger.warning(f"Found {len(anomalies_df)} moving average anomalies in {value_column}")

        return self.anomalies

    def get_anomaly_summary(self) -> dict[str, Any]:
        """Get summary of detected anomalies"""
        if not self.anomalies:
            return {"total_anomalies": 0, "by_severity": {}}

        return {
            "total_anomalies": len(self.anomalies),
            "by_severity": {
                severity: len([a for a in self.anomalies if a.severity == severity])
                for severity in set(a.severity for a in self.anomalies)
            },
        }


def run_comprehensive_anomaly_detection(
    df: DataFrame,
    numeric_columns: list[str],
    categorical_columns: list[str] | None = None,
    time_column: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run comprehensive anomaly detection on a DataFrame

    Args:
        df: Spark DataFrame
        numeric_columns: List of numeric columns to analyze
        categorical_columns: List of categorical columns to analyze
        time_column: Optional time column for time series analysis
        config: Optional configuration with detection parameters

    Returns:
        Summary of all detected anomalies
    """
    all_anomalies = []

    # Default config
    default_config = {
        "numeric_method": "iqr",  # or 'zscore'
        "sensitivity": 3.0,
        "categorical_min_frequency": 0.001,
        "detect_null_spikes": True,
    }

    config = {**default_config, **(config or {})}

    # Detect numeric anomalies
    detector = AnomalyDetector(method=config["numeric_method"], sensitivity=config["sensitivity"])

    for column in numeric_columns:
        try:
            if config["numeric_method"] == "zscore":
                detector.detect_zscore_anomalies(df, column)
            else:  # iqr
                detector.detect_iqr_anomalies(df, column)

            # Null spike detection
            if config["detect_null_spikes"]:
                detector.detect_null_spike(df, column)

        except Exception as e:
            logger.error(f"Failed to detect anomalies in {column}: {e}")

    # Detect categorical anomalies
    if categorical_columns:
        for column in categorical_columns:
            try:
                detector.detect_categorical_anomalies(df, column, min_frequency=config["categorical_min_frequency"])
            except Exception as e:
                logger.error(f"Failed to detect categorical anomalies in {column}: {e}")

    # Get summary
    summary = detector.get_anomaly_summary()
    summary["anomalies"] = [a.to_dict() for a in detector.anomalies]

    return summary
