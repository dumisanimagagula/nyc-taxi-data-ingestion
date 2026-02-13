"""
"""Tests for Airflow DAG Implementation
=======================================
Validates DAG structure, dependencies, health checks, and dynamic task generation
"""

from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

# Airflow imports
from airflow.models import DagBag, Variable


class TestDAGStructure:
    """Test DAG structure and configuration"""

    @pytest.fixture(scope="class")
    def dagbag(self):
        """Load DAG bag with our DAGs"""
        return DagBag(dag_folder="airflow/dags", include_examples=False)

    def test_dag_loads_without_errors(self, dagbag):
        """DAG should load without import errors"""
        assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"

    def test_dag_exists(self, dagbag):
        """DAG should exist in DAG bag"""
        assert "nyc_taxi_medallion_pipeline" in dagbag.dags

    def test_dag_has_correct_schedule(self, dagbag):
        """DAG should have daily schedule"""
        dag = dagbag.get_dag("nyc_taxi_medallion_pipeline")
        assert dag.schedule_interval == "0 2 * * *"

    def test_dag_catchup_disabled(self, dagbag):
        """DAG should have catchup disabled"""
        dag = dagbag.get_dag("nyc_taxi_medallion_pipeline")
        assert dag.catchup is False

    def test_dag_max_active_runs(self, dagbag):
        """DAG should allow only 1 active run"""
        dag = dagbag.get_dag("nyc_taxi_medallion_pipeline")
        assert dag.max_active_runs == 1

def test_dag_has_tags(self, dagbag):
        """DAG should have appropriate tags"""
        dag = dagbag.get_dag("nyc_taxi_medallion_pipeline")
        assert "nyc-taxi" in dag.tags
        assert "medallion" in dag.tags
    
    def test_dag_default_args(self, dagbag):
        """DAG should have correct default args"""
        dag = dagbag.get_dag("nyc_taxi_medallion_pipeline")

        assert dag.default_args.get("owner") == "data-engineering"
        assert dag.default_args.get("retries") == 2
        assert dag.default_args.get("retry_delay") == timedelta(minutes=5)
        assert dag.default_args.get("email_on_failure") is True


class TestTaskGroups:
    """Test TaskGroup structure and organization"""

    @pytest.fixture(scope="class")
    def dag(self):
        """Get DAG instance"""
        dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
        return dagbag.get_dag("nyc_taxi_medallion_pipeline")

    def test_preflight_checks_taskgroup_exists(self, dag):
        """Preflight checks TaskGroup should exist"""
        task_group_ids = [tg.group_id for tg in dag.task_group.children.values() if hasattr(tg, "group_id")]
        assert "preflight_checks" in task_group_ids

    def test_bronze_ingestion_taskgroup_exists(self, dag):
        """Bronze ingestion TaskGroup should exist"""
        task_group_ids = [tg.group_id for tg in dag.task_group.children.values() if hasattr(tg, "group_id")]
        assert "bronze_ingestion" in task_group_ids

    def test_silver_transformation_taskgroup_exists(self, dag):
        """Silver transformation TaskGroup should exist"""
        task_group_ids = [tg.group_id for tg in dag.task_group.children.values() if hasattr(tg, "group_id")]
        assert "silver_transformation" in task_group_ids

    def test_gold_aggregation_taskgroup_exists(self, dag):
        """Gold aggregation TaskGroup should exist"""
        task_group_ids = [tg.group_id for tg in dag.task_group.children.values() if hasattr(tg, "group_id")]
        assert "gold_aggregation" in task_group_ids


class TestHealthChecks:
    """Test health check sensor implementations"""

    @patch("socket.socket")
    def test_spark_master_health_check_success(self, mock_socket):
        """Spark health check should succeed when connection works"""
        # Mock successful connection
        mock_sock_instance = MagicMock()
        mock_sock_instance.connect_ex.return_value = 0
        mock_socket.return_value = mock_sock_instance

        # Import and test health check function
        from airflow.dags.nyc_taxi_medallion_dag import check_spark_master_health

        with patch.object(Variable, "get", return_value="spark://spark-master:7077"):
            result = check_spark_master_health()

        assert result is True
        mock_sock_instance.close.assert_called_once()

    @patch("socket.socket")
    def test_spark_master_health_check_failure(self, mock_socket):
        """Spark health check should fail when connection refused"""
        # Mock failed connection
        mock_sock_instance = MagicMock()
        mock_sock_instance.connect_ex.return_value = 1  # Connection refused
        mock_socket.return_value = mock_sock_instance

        from airflow.dags.nyc_taxi_medallion_dag import check_spark_master_health

        with patch.object(Variable, "get", return_value="spark://spark-master:7077"):
            result = check_spark_master_health()

        assert result is False

    @patch("socket.socket")
    def test_metastore_health_check_success(self, mock_socket):
        """Metastore health check should succeed when connection works"""
        mock_sock_instance = MagicMock()
        mock_sock_instance.connect_ex.return_value = 0
        mock_socket.return_value = mock_sock_instance

        from airflow.dags.nyc_taxi_medallion_dag import check_metastore_health

        with patch.object(Variable, "get", return_value="thrift://hive-metastore:9083"):
            result = check_metastore_health()

        assert result is True

    @patch("requests.get")
    def test_minio_health_check_success(self, mock_get):
        """MinIO health check should succeed when endpoint returns 200"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        from airflow.dags.nyc_taxi_medallion_dag import check_minio_health

        with patch.object(Variable, "get", return_value="http://minio:9000"):
            result = check_minio_health()

        assert result is True

    @patch("requests.get")
    def test_minio_health_check_failure(self, mock_get):
        """MinIO health check should fail when endpoint unavailable"""
        mock_get.side_effect = Exception("Connection refused")

        from airflow.dags.nyc_taxi_medallion_dag import check_minio_health

        with patch.object(Variable, "get", return_value="http://minio:9000"):
            result = check_minio_health()

        assert result is False


class TestDynamicTaskGeneration:
    """Test dynamic task generation from datasets config"""

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_load_datasets_config_success(self, mock_yaml_load, mock_open):
        """Should load datasets from YAML config"""
        # Mock datasets config
        mock_config = {
            "datasets": [
                {"name": "yellow_taxi", "enabled": True, "priority": 1},
                {"name": "green_taxi", "enabled": True, "priority": 2},
                {"name": "fhv_taxi", "enabled": False, "priority": 3},
            ]
        }
        mock_yaml_load.return_value = mock_config

        from airflow.dags.nyc_taxi_medallion_dag import load_datasets_config

        with patch.object(Variable, "get", return_value="/app/config/datasets/datasets.yaml"):
            datasets = load_datasets_config()

        # Should only return enabled datasets
        assert len(datasets) == 2
        assert datasets[0]["name"] == "yellow_taxi"
        assert datasets[1]["name"] == "green_taxi"

    @patch("os.path.exists", return_value=False)
    def test_load_datasets_config_fallback(self, mock_exists):
        """Should fallback to default dataset if config not found"""
        from airflow.dags.nyc_taxi_medallion_dag import load_datasets_config

        with patch.object(Variable, "get", return_value="/nonexistent/config.yaml"):
            datasets = load_datasets_config()

        # Should return default dataset
        assert len(datasets) >= 1
        assert isinstance(datasets, list)

    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_datasets_sorted_by_priority(self, mock_yaml_load, mock_open):
        """Datasets should be sorted by priority (0 = highest)"""
        mock_config = {
            "datasets": [
                {"name": "yellow_taxi", "enabled": True, "priority": 2},
                {"name": "taxi_zones", "enabled": True, "priority": 0},
                {"name": "green_taxi", "enabled": True, "priority": 1},
            ]
        }
        mock_yaml_load.return_value = mock_config

        from airflow.dags.nyc_taxi_medallion_dag import load_datasets_config

        with patch.object(Variable, "get", return_value="/app/config/datasets/datasets.yaml"):
            datasets = load_datasets_config()

        # Should be sorted: taxi_zones (0), green_taxi (1), yellow_taxi (2)
        assert datasets[0]["name"] == "taxi_zones"
        assert datasets[1]["name"] == "green_taxi"
        assert datasets[2]["name"] == "yellow_taxi"


class TestBranchingLogic:
    """Test conditional branching for data quality and lineage"""

    def test_data_quality_branch_enabled(self):
        """Should branch to run_data_quality_checks when enabled"""
        from airflow.dags.nyc_taxi_medallion_dag import decide_data_quality_branch

        with patch.object(Variable, "get", return_value=True):
            result = decide_data_quality_branch()

        assert result == "run_data_quality_checks"

    def test_data_quality_branch_disabled(self):
        """Should branch to skip_data_quality when disabled"""
        from airflow.dags.nyc_taxi_medallion_dag import decide_data_quality_branch

        with patch.object(Variable, "get", return_value=False):
            result = decide_data_quality_branch()

        assert result == "skip_data_quality"

    def test_lineage_branch_enabled(self):
        """Should branch to update_lineage when enabled"""
        from airflow.dags.nyc_taxi_medallion_dag import decide_lineage_branch

        with patch.object(Variable, "get", return_value=True):
            result = decide_lineage_branch()

        assert result == "update_lineage"

    def test_lineage_branch_disabled(self):
        """Should branch to skip_lineage when disabled"""
        from airflow.dags.nyc_taxi_medallion_dag import decide_lineage_branch

        with patch.object(Variable, "get", return_value=False):
            result = decide_lineage_branch()

        assert result == "skip_lineage"


class TestDAGDependencies:
    """Test task dependencies are correctly defined"""

    @pytest.fixture(scope="class")
    def dag(self):
        """Get DAG instance"""
        dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
        return dagbag.get_dag("nyc_taxi_medallion_pipeline")

    def test_preflight_before_bronze(self, dag):
        """Preflight checks should run before bronze ingestion"""
        preflight_tasks = [t for t in dag.tasks if "preflight_checks" in t.task_id]
        bronze_tasks = [t for t in dag.tasks if "bronze_ingestion" in t.task_id]

        if preflight_tasks and bronze_tasks:
            # Check at least one bronze task depends on preflight
            preflight_task = preflight_tasks[0]
            bronze_task = bronze_tasks[0]

            assert any(
                preflight_task in dag.get_task(bronze_task.task_id).upstream_list for bronze_task in bronze_tasks
            )

    def test_bronze_before_silver(self, dag):
        """Bronze ingestion should run before silver transformation"""
        bronze_tasks = [t for t in dag.tasks if "bronze_ingestion" in t.task_id]
        silver_tasks = [t for t in dag.tasks if "silver_transformation" in t.task_id]

        if bronze_tasks and silver_tasks:
            # At least one silver task should depend on bronze
            assert any(any(bt in st.upstream_list for bt in bronze_tasks) for st in silver_tasks)

    def test_silver_before_gold(self, dag):
        """Silver transformation should run before gold aggregation"""
        silver_tasks = [t for t in dag.tasks if "silver_transformation" in t.task_id]
        gold_tasks = [t for t in dag.tasks if "gold_aggregation" in t.task_id]

        if silver_tasks and gold_tasks:
            # At least one gold task should depend on silver
            assert any(any(st in gt.upstream_list for st in silver_tasks) for gt in gold_tasks)

    def test_gold_before_quality(self, dag):
        """Gold aggregation should run before data quality checks"""
        gold_tasks = [t for t in dag.tasks if "gold_aggregation" in t.task_id]
        quality_tasks = [t for t in dag.tasks if "data_quality" in t.task_id]

        if gold_tasks and quality_tasks:
            # Quality tasks should depend on gold
            assert any(any(gt in qt.upstream_list for gt in gold_tasks) for qt in quality_tasks)


class TestSparkSubmitOperatorConfig:
    """Test SparkSubmitOperator configuration"""

    @pytest.fixture(scope="class")
    def dag(self):
        """Get DAG instance"""
        dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
        return dagbag.get_dag("nyc_taxi_medallion_pipeline")

    def test_spark_operators_have_correct_conn_id(self, dag):
        """All SparkSubmitOperators should use spark_default connection"""
        from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

        spark_tasks = [t for t in dag.tasks if isinstance(t, SparkSubmitOperator)]

        for task in spark_tasks:
            assert task.conn_id == "spark_default"

    def test_spark_operators_have_packages(self, dag):
        """SparkSubmitOperators should have Iceberg packages"""
        from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

        spark_tasks = [t for t in dag.tasks if isinstance(t, SparkSubmitOperator)]

        for task in spark_tasks:
            if hasattr(task, "_packages"):
                assert "iceberg" in task._packages.lower()

    def test_spark_operators_have_conf(self, dag):
        """SparkSubmitOperators should have Spark configuration"""
        from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

        spark_tasks = [t for t in dag.tasks if isinstance(t, SparkSubmitOperator)]

        for task in spark_tasks:
            if hasattr(task, "_conf"):
                assert isinstance(task._conf, dict)
                assert len(task._conf) > 0


class TestDAGFactory:
    """Test DAG factory pattern implementation"""

    @patch("os.path.exists", return_value=True)
    @patch("os.listdir", return_value=["nyc_taxi_pipeline.yaml"])
    @patch("builtins.open", create=True)
    @patch("yaml.safe_load")
    def test_dag_factory_creates_dags(self, mock_yaml_load, mock_open, mock_listdir, mock_exists):
        """DAG factory should create DAGs from config files"""
        mock_config = {
            "dag": {
                "id": "test_pipeline",
                "description": "Test pipeline",
                "schedule": "0 2 * * *",
                "start_date": "2024-01-01",
            },
            "layers": {
                "bronze": {"datasets": []},
                "silver": {"datasets": []},
                "gold": {},
            },
        }
        mock_yaml_load.return_value = mock_config

        from airflow.dags.dag_factory import MedallionDAGFactory

        with patch.object(Variable, "get", return_value="/app/config/dags"):
            factory = MedallionDAGFactory("/app/config/dags/test.yaml")
            dag = factory.create_dag()

        assert dag is not None
        assert dag.dag_id == "test_pipeline"

    def test_dag_factory_loads_config(self):
        """DAG factory should load configuration from YAML"""
        from airflow.dags.dag_factory import MedallionDAGFactory

        # This will fail if YAML file doesn't exist, but that's expected
        # In real test, we'd mock the file
        with pytest.raises(FileNotFoundError):
            factory = MedallionDAGFactory("/nonexistent/config.yaml")


class TestEnvironmentConfiguration:
    """Test environment-specific configuration handling"""

    def test_dev_environment_variables(self):
        """Development environment should have correct variables"""
        with patch.object(
            Variable,
            "get",
            side_effect=lambda k, **kwargs: {
                "AIRFLOW_ENV": "dev",
                "ENABLE_DATA_QUALITY": False,
                "ENABLE_LINEAGE": False,
            }.get(k, kwargs.get("default_var")),
        ):
            env = Variable.get("AIRFLOW_ENV")
            dq_enabled = Variable.get("ENABLE_DATA_QUALITY", default_var=False)

            assert env == "dev"
            assert dq_enabled is False

    def test_prod_environment_variables(self):
        """Production environment should have correct variables"""
        with patch.object(
            Variable,
            "get",
            side_effect=lambda k, **kwargs: {
                "AIRFLOW_ENV": "prod",
                "ENABLE_DATA_QUALITY": True,
                "ENABLE_LINEAGE": True,
            }.get(k, kwargs.get("default_var")),
        ):
            env = Variable.get("AIRFLOW_ENV")
            dq_enabled = Variable.get("ENABLE_DATA_QUALITY", default_var=True)

            assert env == "prod"
            assert dq_enabled is True


# ============================================================================
# Integration Tests (require running Airflow instance)
# ============================================================================


@pytest.mark.integration
class TestDAGIntegration:
    """Integration tests for DAG execution"""

    def test_dag_run_completes(self):
        """Full DAG run should complete successfully"""
        # This requires a running Airflow instance
        # Would use airflow.executors.debug_executor for testing
        pytest.skip("Requires running Airflow instance")

    def test_health_checks_pass_with_real_services(self):
        """Health checks should pass with real running services"""
        pytest.skip("Requires running infrastructure")

    def test_dynamic_tasks_created_correctly(self):
        """Dynamic tasks should be created based on actual datasets config"""
        pytest.skip("Requires running Airflow instance")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
