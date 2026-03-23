# NYC Taxi Data Lakehouse - Copilot Instructions

## Project summary

- End-to-end, config-driven NYC Taxi lakehouse using a medallion architecture.
- Bronze: Python ingest to Iceberg on MinIO.
- Silver: Spark transforms and data quality checks from YAML.
- Gold: dbt models for analytics, queried via Trino and visualized in Superset.
- Airflow orchestrates when things run; it does not transform data.

## Core principles

- Prefer configuration changes over code changes. The main config is `config/pipelines/lakehouse_config.yaml`.
- Keep the pipeline config-driven. Avoid adding hard-coded filters or parameters to code.
- Respect layer responsibilities: ingest in Bronze, transform in Silver, aggregate in Gold.
- Keep data append-only in Bronze. Do not add destructive operations unless explicitly requested.
- Favor reproducibility and idempotency for pipeline steps.

## Key locations

- Airflow DAG: `airflow/dags/nyc_taxi_medallion_dag.py`.
- Bronze ingestor: `bronze/ingestors/ingest_to_iceberg.py`.
- Silver Spark job: `silver/jobs/bronze_to_silver.py`.
- Gold dbt models: `gold/models/analytics/` and `gold/models/schema.yml`.
- Config loader: `src/config_loader.py`.
- Pipeline config: `config/pipelines/lakehouse_config.yaml`.
- Tests: `tests/unit/`, `tests/integration/`, `tests/e2e/`, `tests/airflow/`.
- Documentation: `docs/`.
- Legacy/archived scripts: `archive/`.

## Code change guidance

- If you add a new config option, update `src/config_loader.py` and relevant docs.
- Keep Python code compatible with the existing runtime and dependencies in `requirements.txt`.
- For Spark jobs, keep transformations declarative and driven by YAML fields.
- For dbt, add new models under `gold/models/analytics/` and update `schema.yml`.
- Avoid non-ASCII characters in code and docs unless the file already uses them.

## Run and verify

- Use Docker Compose for local runs. Windows uses PowerShell scripts in `scripts/`.
- Typical commands live in `README.md` and `docs/GETTING_STARTED.md`.
- If you add tests or change validation logic, update or extend `tests/unit/test_validation.py`.
