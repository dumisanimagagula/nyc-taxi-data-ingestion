"""
DAG Factory Pattern for Medallion Architecture
===============================================
Dynamically generates DAGs based on configuration files

Benefits:
- Single source of truth for DAG configuration
- Easy to add new pipelines without code changes
- Consistent structure across all pipelines
- Environment-specific configurations
- DRY principle (Don't Repeat Yourself)

Usage:
    Place YAML configuration files in config/dags/ directory.
    Each file will generate a corresponding DAG automatically.
"""

import os
from datetime import datetime, timedelta
from typing import Any

import yaml
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

from airflow import DAG


class MedallionDAGFactory:
    """
    Factory for generating Medallion Architecture DAGs

    Reads YAML configuration and creates fully-configured Airflow DAG
    """

    def __init__(self, config_path: str):
        """
        Initialize DAG factory with configuration

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.environment = Variable.get("AIRFLOW_ENV", default_var="dev")

    def _load_config(self) -> dict[str, Any]:
        """Load DAG configuration from YAML file"""
        with open(self.config_path) as f:
            return yaml.safe_load(f)

    def _get_default_args(self) -> dict[str, Any]:
        """Build default arguments from configuration"""
        config_defaults = self.config.get("default_args", {})

        return {
            "owner": config_defaults.get("owner", "data-engineering"),
            "depends_on_past": config_defaults.get("depends_on_past", False),
            "email_on_failure": config_defaults.get("email_on_failure", True),
            "email_on_retry": config_defaults.get("email_on_retry", False),
            "email": config_defaults.get("email", ["data-engineering@company.com"]),
            "retries": config_defaults.get("retries", 2),
            "retry_delay": timedelta(minutes=config_defaults.get("retry_delay_minutes", 5)),
            "execution_timeout": timedelta(hours=config_defaults.get("timeout_hours", 2)),
        }

    def _get_spark_config(self) -> dict[str, str]:
        """Build Spark configuration"""
        spark_conf = self.config.get("spark", {}).get("conf", {})

        # Add environment-specific values
        spark_conf.update(
            {
                "spark.sql.catalog.lakehouse.uri": Variable.get(
                    "HIVE_METASTORE_URI", default_var="thrift://hive-metastore:9083"
                ),
                "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000"),
                "spark.hadoop.fs.s3a.access.key": Variable.get("S3_ACCESS_KEY", default_var="minio"),
                "spark.hadoop.fs.s3a.secret.key": Variable.get("S3_SECRET_KEY", default_var="minio123"),
            }
        )

        return spark_conf

    def _create_health_checks(self) -> TaskGroup:
        """Create health check task group"""

        with TaskGroup("health_checks", tooltip="Pre-flight health checks") as group:
            checks_config = self.config.get("health_checks", {})

            if checks_config.get("spark_master", True):
                PythonSensor(
                    task_id="check_spark",
                    python_callable=self._check_spark_health,
                    timeout=300,
                    poke_interval=30,
                )

            if checks_config.get("metastore", True):
                PythonSensor(
                    task_id="check_metastore",
                    python_callable=self._check_metastore_health,
                    timeout=300,
                    poke_interval=30,
                )

            if checks_config.get("storage", True):
                PythonSensor(
                    task_id="check_storage",
                    python_callable=self._check_storage_health,
                    timeout=300,
                    poke_interval=30,
                )

        return group

    def _create_bronze_layer(self) -> TaskGroup:
        """Create Bronze layer ingestion task group"""

        with TaskGroup("bronze_layer", tooltip="Raw data ingestion") as group:
            bronze_config = self.config.get("layers", {}).get("bronze", {})
            datasets = bronze_config.get("datasets", [])

            for dataset in datasets:
                if dataset.get("enabled", True):
                    PythonOperator(
                        task_id=f"ingest_{dataset['name']}",
                        python_callable=self._run_bronze_ingestion,
                        op_kwargs={"dataset": dataset},
                    )

        return group

    def _create_silver_layer(self) -> TaskGroup:
        """Create Silver layer transformation task group"""

        with TaskGroup("silver_layer", tooltip="Data transformation and cleaning") as group:
            silver_config = self.config.get("layers", {}).get("silver", {})
            datasets = silver_config.get("datasets", [])
            spark_conf = self._get_spark_config()

            for dataset in datasets:
                if dataset.get("enabled", True):
                    SparkSubmitOperator(
                        task_id=f"transform_{dataset['name']}",
                        application=silver_config.get("application", "/opt/spark/jobs/bronze_to_silver.py"),
                        conn_id=self.config.get("spark", {}).get("conn_id", "spark_default"),
                        conf=spark_conf,
                        packages=",".join(self.config.get("spark", {}).get("packages", [])),
                        application_args=[
                            "--config",
                            self.config.get("pipeline_config_path", ""),
                            "--dataset",
                            dataset["name"],
                            "--environment",
                            self.environment,
                        ],
                        name=f"silver_{dataset['name']}",
                        verbose=True,
                    )

        return group

    def _create_gold_layer(self) -> TaskGroup:
        """Create Gold layer aggregation task group"""

        with TaskGroup("gold_layer", tooltip="Analytics models") as group:
            gold_config = self.config.get("layers", {}).get("gold", {})
            spark_conf = self._get_spark_config()

            SparkSubmitOperator(
                task_id="build_gold_models",
                application=gold_config.get("application", "/opt/spark/jobs/build_gold_layer.py"),
                conn_id=self.config.get("spark", {}).get("conn_id", "spark_default"),
                conf=spark_conf,
                packages=",".join(self.config.get("spark", {}).get("packages", [])),
                application_args=[
                    "--config",
                    self.config.get("pipeline_config_path", ""),
                    "--environment",
                    self.environment,
                ],
                name="gold_aggregation",
                verbose=True,
            )

        return group

    def _create_data_quality_checks(self) -> TaskGroup:
        """Create data quality validation task group"""

        with TaskGroup("data_quality", tooltip="Data quality validation") as group:
            dq_config = self.config.get("data_quality", {})

            if dq_config.get("enabled", True):
                spark_conf = self._get_spark_config()

                SparkSubmitOperator(
                    task_id="run_quality_checks",
                    application=dq_config.get("application", "/opt/spark/jobs/run_data_quality.py"),
                    conn_id=self.config.get("spark", {}).get("conn_id", "spark_default"),
                    conf=spark_conf,
                    packages=",".join(self.config.get("spark", {}).get("packages", [])),
                    application_args=[
                        "--config",
                        self.config.get("pipeline_config_path", ""),
                        "--environment",
                        self.environment,
                        "--run-id",
                        "{{ run_id }}",
                    ],
                    name="data_quality_checks",
                    verbose=True,
                )
            else:
                DummyOperator(task_id="skip_data_quality")

        return group

    def create_dag(self) -> DAG:
        """
        Create complete DAG from configuration

        Returns:
            Configured Airflow DAG instance
        """
        dag_config = self.config.get("dag", {})

        dag = DAG(
            dag_id=dag_config.get("id", "medallion_pipeline"),
            default_args=self._get_default_args(),
            description=dag_config.get("description", "Medallion architecture pipeline"),
            schedule_interval=dag_config.get("schedule", "0 2 * * *"),
            start_date=datetime.fromisoformat(dag_config.get("start_date", "2024-01-01")),
            catchup=dag_config.get("catchup", False),
            max_active_runs=dag_config.get("max_active_runs", 1),
            tags=dag_config.get("tags", []),
        )

        with dag:
            # Create task groups
            health_checks = self._create_health_checks()
            bronze = self._create_bronze_layer()
            silver = self._create_silver_layer()
            gold = self._create_gold_layer()
            data_quality = self._create_data_quality_checks()

            # Success notification
            notify = PythonOperator(
                task_id="notify_success",
                python_callable=self._send_notification,
            )

            # Define dependencies
            health_checks >> bronze >> silver >> gold >> data_quality >> notify

        return dag

    # ========================================================================
    # Health Check Methods
    # ========================================================================

    @staticmethod
    def _check_spark_health() -> bool:
        """Check Spark master availability"""
        import socket

        try:
            spark_master = Variable.get("SPARK_MASTER_URL", "spark://spark-master:7077")
            host, port = spark_master.replace("spark://", "").split(":")

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()

            return result == 0
        except Exception as e:
            print(f"Spark health check failed: {e}")
            return False

    @staticmethod
    def _check_metastore_health() -> bool:
        """Check Hive Metastore availability"""
        import socket

        try:
            metastore_uri = Variable.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
            host, port = metastore_uri.replace("thrift://", "").split(":")

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()

            return result == 0
        except Exception as e:
            print(f"Metastore health check failed: {e}")
            return False

    @staticmethod
    def _check_storage_health() -> bool:
        """Check MinIO storage availability"""
        import requests

        try:
            minio_endpoint = Variable.get("MINIO_ENDPOINT", "http://minio:9000")
            response = requests.get(f"{minio_endpoint}/minio/health/live", timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"Storage health check failed: {e}")
            return False

    @staticmethod
    def _run_bronze_ingestion(dataset: dict[str, Any], **context):
        """Execute bronze layer ingestion"""
        print(f"Ingesting dataset: {dataset['name']}")
        # Import and execute ingestion logic
        # In production, this would call the actual ingestion module

    @staticmethod
    def _send_notification(**context):
        """Send pipeline completion notification"""
        print(f"Pipeline {context['dag'].dag_id} completed successfully")


# ============================================================================
# DAG GENERATION - Auto-discover and create DAGs from config files
# ============================================================================


def create_dags_from_configs():
    """
    Scan config directory and create DAGs for each configuration file

    Place YAML files in config/dags/ and they will automatically become DAGs
    """
    config_dir = Variable.get("DAG_CONFIG_DIR", default_var="/app/config/dags")

    if not os.path.exists(config_dir):
        print(f"Config directory not found: {config_dir}")
        return {}

    dags = {}

    for filename in os.listdir(config_dir):
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            config_path = os.path.join(config_dir, filename)

            try:
                factory = MedallionDAGFactory(config_path)
                dag = factory.create_dag()

                # Add to globals so Airflow can discover it
                dag_id = dag.dag_id
                dags[dag_id] = dag
                globals()[dag_id] = dag

                print(f"✓ Created DAG: {dag_id} from {filename}")

            except Exception as e:
                print(f"✗ Failed to create DAG from {filename}: {str(e)}")

    return dags


# Auto-generate DAGs on module import
if Variable.get("ENABLE_DAG_FACTORY", default_var=True, deserialize_json=True):
    generated_dags = create_dags_from_configs()
    print(f"DAG Factory generated {len(generated_dags)} DAGs")
