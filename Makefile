# ==============================================================================
# NYC Taxi Data Lakehouse - Makefile
# ==============================================================================
# Unified task runner for the medallion-architecture lakehouse platform.
# Usage: make help
# ==============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Environment: override with  make <target> ENV=staging
ENV ?= dev
COMPOSE := docker compose
COMPOSE_FILE := docker-compose.yaml
OVERRIDE_FILE := docker-compose.override.yaml
ENV_OVERRIDE := docker-compose.override.$(ENV).yaml

# Build compose command with available override files
COMPOSE_CMD := $(COMPOSE) -f $(COMPOSE_FILE)
ifneq (,$(wildcard $(OVERRIDE_FILE)))
  COMPOSE_CMD += -f $(OVERRIDE_FILE)
endif
ifneq (,$(wildcard $(ENV_OVERRIDE)))
  COMPOSE_CMD += -f $(ENV_OVERRIDE)
endif

# ==============================================================================
# Help
# ==============================================================================

.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "NYC Taxi Data Lakehouse - Available targets"
	@echo "============================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Environment: ENV=$(ENV)  (override with ENV=staging|prod)"
	@echo ""

# ==============================================================================
# Environment
# ==============================================================================

.PHONY: env-dev env-staging env-prod
env-dev: ## Copy .env.dev to .env
	cp .env.dev .env
	@echo "Switched to dev environment"

env-staging: ## Copy .env.staging to .env
	cp .env.staging .env
	@echo "Switched to staging environment"

env-prod: ## Copy .env.prod to .env
	cp .env.prod .env
	@echo "Switched to prod environment"

# ==============================================================================
# Docker Compose Lifecycle
# ==============================================================================

.PHONY: build up down restart ps logs
build: ## Build all Docker images
	$(COMPOSE_CMD) build

up: ## Start all services (detached)
	$(COMPOSE_CMD) up -d

down: ## Stop and remove all services
	$(COMPOSE_CMD) down

restart: down up ## Restart all services

ps: ## Show running service status
	$(COMPOSE_CMD) ps

logs: ## Tail logs for all services (Ctrl-C to stop)
	$(COMPOSE_CMD) logs -f

# Start individual service groups
.PHONY: up-infra up-airflow up-spark
up-infra: ## Start infrastructure only (MinIO, Metastore, Trino)
	$(COMPOSE_CMD) up -d minio minio-setup metastore-db hive-metastore trino

up-airflow: ## Start Airflow services
	$(COMPOSE_CMD) up -d airflow-db airflow-init airflow-webserver airflow-scheduler

up-spark: ## Start Spark cluster
	$(COMPOSE_CMD) up -d spark-master spark-worker-1 spark-worker-2

# ==============================================================================
# Pipeline Operations
# ==============================================================================

.PHONY: ingest transform dbt-run dbt-test verify
ingest: ## Run Bronze ingestion
	$(COMPOSE_CMD) run --rm ingestor python -m bronze.ingestors.ingest_to_iceberg

transform: ## Run Silver Spark transformation
	$(COMPOSE_CMD) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/silver/jobs/bronze_to_silver.py

dbt-run: ## Run dbt Gold models
	$(COMPOSE_CMD) run --rm dbt run

dbt-test: ## Run dbt tests
	$(COMPOSE_CMD) run --rm dbt test

verify: ## Run all verification scripts
	python scripts/verify_bronze_layer.py
	python scripts/verify_silver_layer.py
	python scripts/verify_gold_layer.py

# ==============================================================================
# Testing
# ==============================================================================

.PHONY: test test-unit test-integration test-e2e test-cov
test: ## Run all tests
	pytest

test-unit: ## Run unit tests only
	pytest -m unit

test-integration: ## Run integration tests only
	pytest -m integration --timeout=600

test-e2e: ## Run end-to-end tests only
	pytest -m e2e --timeout=900

test-cov: ## Run tests with coverage report
	pytest --cov=src --cov=bronze --cov=silver --cov=gold \
		--cov-branch --cov-report=term-missing --cov-report=html:htmlcov

# ==============================================================================
# Code Quality
# ==============================================================================

.PHONY: lint format
lint: ## Run flake8 linter
	flake8 src/ bronze/ silver/ gold/ \
		--max-line-length=120 --extend-ignore=E203,W503

format: ## Check formatting with black (dry-run)
	black --check --line-length 120 src/ bronze/ silver/ gold/

# ==============================================================================
# Setup & Teardown
# ==============================================================================

.PHONY: setup teardown clean
setup: env-dev build up ## Full setup: set dev env, build, and start services
	@echo "Lakehouse platform is starting..."
	@echo "MinIO Console: http://localhost:9001"
	@echo "Spark Master:  http://localhost:8080"
	@echo "Trino UI:      http://localhost:8086"
	@echo "Superset:      http://localhost:8088"
	@echo "Airflow:       http://localhost:8089"

teardown: ## Stop services and remove volumes
	$(COMPOSE_CMD) down -v
	@echo "All services stopped and volumes removed"

clean: teardown ## Full cleanup: volumes, build cache, coverage artifacts
	$(COMPOSE_CMD) down --rmi local -v --remove-orphans
	rm -rf htmlcov/ .pytest_cache/ coverage.xml .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup complete"

# ==============================================================================
# Utilities
# ==============================================================================

.PHONY: shell-ingestor shell-spark shell-trino healthcheck
shell-ingestor: ## Open a shell in the ingestor container
	$(COMPOSE_CMD) exec ingestor bash

shell-spark: ## Open a shell in the Spark master container
	$(COMPOSE_CMD) exec spark-master bash

shell-trino: ## Open a Trino CLI session
	$(COMPOSE_CMD) exec trino trino

healthcheck: ## Run the PowerShell healthcheck script
	powershell -File scripts/healthcheck.ps1
