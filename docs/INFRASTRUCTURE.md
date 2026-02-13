# Infrastructure Documentation

## Overview

This document describes the infrastructure setup for the NYC Taxi Data Lakehouse project, including service architecture, resource management, environment configurations, and deployment procedures.

## Architecture

### Service Layers

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Analytics Layer                          │
│  ┌──────────────┐                            ┌──────────────┐  │
│  │   Superset   │◄───────────────────────────┤   Airflow    │  │
│  └──────┬───────┘                            └──────┬───────┘  │
│         │                                            │          │
└─────────┼────────────────────────────────────────────┼──────────┘
          │                                            │
┌─────────┼────────────────────────────────────────────┼──────────┐
│         │            Query Engine Layer              │          │
│         ▼                                            ▼          │
│  ┌──────────────┐                            ┌──────────────┐  │
│  │    Trino     │◄───────────────────────────┤  dbt (Gold)  │  │
│  └──────┬───────┘                            └──────────────┘  │
│         │                                                       │
└─────────┼───────────────────────────────────────────────────────┘
          │
┌─────────┼───────────────────────────────────────────────────────┐
│         │          Processing Layer (Silver)                    │
│         ▼                                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Spark Cluster                               │   │
│  │  ┌────────┐  ┌──────────┐  ┌──────────┐                │   │
│  │  │ Master │  │ Worker-1 │  │ Worker-2 │                │   │
│  │  └────────┘  └──────────┘  └──────────┘                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
          │
┌─────────┼───────────────────────────────────────────────────────┐
│         │          Ingestion Layer (Bronze)                     │
│         ▼                                                        │
│  ┌──────────────┐                                               │
│  │   Ingestor   │                                               │
│  └──────┬───────┘                                               │
│         │                                                        │
└─────────┼────────────────────────────────────────────────────────┘
          │
┌─────────┼────────────────────────────────────────────────────────┐
│         │           Storage & Metadata Layer                     │
│         ▼                                                        │
│  ┌──────────────┐       ┌─────────────────┐                    │
│  │    MinIO     │       │ Hive Metastore  │                    │
│  │   (S3-API)   │       │  (Metadata DB)  │                    │
│  └──────────────┘       └─────────────────┘                    │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Metastore DB │  │ Superset DB  │  │  Airflow DB  │         │
│  │ (PostgreSQL) │  │ (PostgreSQL) │  │ (PostgreSQL) │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└────────────────────────────────────────────────────────────────┘
```text

## Service Details

### Core Services

#### 1. MinIO (Object Storage)

- **Purpose**: S3-compatible object storage for Iceberg tables
- **Ports**: 9000 (API), 9001 (Console)
- **Resource Limits**:
  - Dev: 0.5 CPU, 512MB RAM
  - Staging: 1.0 CPU, 2GB RAM
  - Prod: 2.0 CPU, 4GB RAM
- **Health Check**: `mc ready local`
- **Dependencies**: None

#### 2. Hive Metastore

- **Purpose**: Metadata catalog for Iceberg tables
- **Port**: 9083 (Thrift)
- **Resource Limits**:
  - Dev: 0.5 CPU, 512MB RAM
  - Staging: 1.0 CPU, 2GB RAM
  - Prod: 2.0 CPU, 4GB RAM
- **Health Check**: `nc -z localhost 9083`
- **Dependencies**: metastore-db (PostgreSQL)

#### 3. Spark Cluster

- **Purpose**: Silver layer transformations
- **Components**:
  - Spark Master: Port 7077 (cluster), 8080 (UI)
  - Spark Workers (2): Dynamic resource allocation
- **Resource Limits**:
  - Dev: 1 CPU / 2GB (master), 2 CPU / 3GB (workers)
  - Staging: 2 CPU / 4GB (master), 4 CPU / 10GB (workers)
  - Prod: 4 CPU / 8GB (master), 8 CPU / 20GB (workers)
- **Health Check**: HTTP UI check on port 8080
- **Dependencies**: hive-metastore

#### 4. Trino (Query Engine)

- **Purpose**: SQL query engine for all layers
- **Port**: 8086 (UI/API)
- **Resource Limits**:
  - Dev: 2 CPU, 4GB RAM
  - Staging: 4 CPU, 8GB RAM
  - Prod: 8 CPU, 16GB RAM
- **Health Check**: `curl -f http://localhost:8080/v1/info`
- **Dependencies**: hive-metastore, minio

#### 5. Superset (BI Tool)

- **Purpose**: Data visualization and analytics
- **Port**: 8088
- **Resource Limits**:
  - Dev: 1 CPU, 1GB RAM
  - Staging: 2 CPU, 4GB RAM
  - Prod: 4 CPU, 8GB RAM
- **Health Check**: `curl -f http://localhost:8088/health`
- **Dependencies**: superset-db, trino

#### 6. Airflow (Orchestration)

- **Purpose**: Workflow orchestration
- **Components**:
  - Webserver: Port 8089
  - Scheduler: Background process
  - Init: One-time setup
- **Resource Limits**:
  - Webserver:
    - Dev: 0.5 CPU, 1GB RAM
    - Staging: 1 CPU, 2GB RAM
    - Prod: 2 CPU, 4GB RAM
  - Scheduler:
    - Dev: 1 CPU, 1GB RAM
    - Staging: 2 CPU, 2GB RAM
    - Prod: 4 CPU, 4GB RAM
- **Health Check**: `curl --fail http://localhost:8080/health`
- **Dependencies**: airflow-db

#### 7. Ingestor (Bronze Layer)

- **Purpose**: Config-driven data extraction
- **Resource Limits**:
  - All environments: 1 CPU, 2GB RAM
- **Dependencies**: minio, hive-metastore

#### 8. dbt (Gold Layer)

- **Purpose**: Business transformations
- **Resource Limits**:
  - All environments: 1 CPU, 2GB RAM
- **Dependencies**: trino

### Database Services

All PostgreSQL databases use the same resource allocation:

- **Resource Limits**:
  - Dev: 0.5 CPU, 512MB RAM
  - Staging: 1 CPU, 2GB RAM
  - Prod: 2 CPU, 4GB RAM
- **Health Check**: `pg_isready -U <user> -d <database>`

#### Databases:

1. **metastore-db**: Hive Metastore metadata
2. **superset-db**: Superset application data
3. **airflow-db**: Airflow metadata and state

## Environment Configurations

### Development (.env.dev)

**Characteristics:**
- Minimal resource usage (50% of production)
- Simple credentials for easy setup
- No auto-restart (for debugging)
- Fast health checks (15s interval)
- Reduced logging (5MB max, 2 files)
- No HTTPS enforcement
- Monitoring disabled

**Total Resource Footprint:**
- CPUs: ~12-14 cores
- Memory: ~20-25GB RAM

**Usage:**
```bash

# Copy environment file

cp .env.dev .env

# Start stack

docker-compose -f docker-compose.yaml -f docker-compose.override.dev.yaml up
```text

### Staging (.env.staging)

**Characteristics:**
- Production-like resources (75% of production)
- Strict passwords (placeholders)
- Auto-restart on failure
- Standard health checks (30s interval)
- Standard logging (10MB max, 5 files)
- HTTPS enabled (if certificates available)
- Monitoring enabled

**Total Resource Footprint:**
- CPUs: ~20-24 cores
- Memory: ~35-40GB RAM

**Usage:**
```bash

# Copy environment file

cp .env.staging .env

# Start stack

docker-compose -f docker-compose.yaml -f docker-compose.override.staging.yaml up
```

### Production (.env.prod)

**Characteristics:**
- Maximum resources (100%)
- Secure passwords (MUST CHANGE before deployment)
- Always restart (unless-stopped policy)
- Conservative health checks (30s interval, longer start period)
- Maximum logging (20MB max, 10 files)
- HTTPS enforced
- Monitoring enabled
- Alerting configured

**Total Resource Footprint:**
- CPUs: ~30-35 cores
- Memory: ~50-60GB RAM

**Usage:**
```bash

# Copy and CUSTOMIZE environment file

cp .env.prod .env

# EDIT .env to change ALL passwords marked with CHANGE_THIS

# Start stack

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml up -d
```text

## Startup Sequence

The services have strict dependencies and health checks to ensure proper startup order:

```text
1. Storage Layer (parallel)
   ├── MinIO (health check)
   └── PostgreSQL DBs (health checks)
       ├── metastore-db
       ├── superset-db
       └── airflow-db

2. Metadata Layer (sequential)
   └── Hive Metastore (depends on metastore-db health)

3. Processing Layer (sequential)
   ├── Spark Master (depends on hive-metastore health)
   └── Spark Workers (depend on spark-master health)

4. Query Engine (sequential)
   └── Trino (depends on hive-metastore + minio health)

5. Analytics Layer (sequential)
   ├── Superset (depends on superset-db + trino health)
   └── Airflow Init (depends on airflow-db health)
       ├── Airflow Webserver
       └── Airflow Scheduler

6. Application Layer (parallel)
   ├── Ingestor (depends on minio + hive-metastore health)
   └── dbt (depends on trino health)
```text

**Expected Startup Time:**
- Development: ~3-5 minutes
- Staging: ~5-7 minutes
- Production: ~7-10 minutes

## Resource Management

### CPU Allocation

| Service | Dev | Staging | Production |
|---------|-----|---------|------------|
| MinIO | 0.5 | 1.0 | 2.0 |
| PostgreSQL (each) | 0.5 | 1.0 | 2.0 |
| Hive Metastore | 0.5 | 1.0 | 2.0 |
| Spark Master | 1.0 | 2.0 | 4.0 |
| Spark Workers (each) | 2.0 | 4.0 | 8.0 |
| Trino | 2.0 | 4.0 | 8.0 |
| Superset | 1.0 | 2.0 | 4.0 |
| Airflow Webserver | 0.5 | 1.0 | 2.0 |
| Airflow Scheduler | 1.0 | 2.0 | 4.0 |
| Ingestor | 1.0 | 1.0 | 1.0 |
| dbt | 1.0 | 1.0 | 1.0 |

### Memory Allocation

| Service | Dev | Staging | Production |
|---------|-----|---------|------------|
| MinIO | 512MB | 2GB | 4GB |
| PostgreSQL (each) | 512MB | 2GB | 4GB |
| Hive Metastore | 512MB | 2GB | 4GB |
| Spark Master | 2GB | 4GB | 8GB |
| Spark Workers (each) | 3GB | 10GB | 20GB |
| Trino | 4GB | 8GB | 16GB |
| Superset | 1GB | 4GB | 8GB |
| Airflow Webserver | 1GB | 2GB | 4GB |
| Airflow Scheduler | 1GB | 2GB | 4GB |
| Ingestor | 2GB | 2GB | 2GB |
| dbt | 2GB | 2GB | 2GB |

## Health Checks

All services have health checks configured with environment-specific settings:

**Development:**
- Interval: 15s
- Timeout: 5s
- Retries: 3
- Start Period: 20s

**Staging/Production:**
- Interval: 30s
- Timeout: 10s
- Retries: 5
- Start Period: 60-90s

## Logging

Centralized logging configuration using JSON file driver:

**Development:**
- Max Size: 5MB per file
- Max Files: 2 files
- Total per service: ~10MB

**Staging:**
- Max Size: 10MB per file
- Max Files: 5 files
- Total per service: ~50MB

**Production:**
- Max Size: 20MB per file
- Max Files: 10 files
- Total per service: ~200MB

**Log Locations:**
- Container logs: `/var/lib/docker/containers/`
- Airflow logs: `./airflow/logs/`
- dbt logs: `./gold/logs/`

## Networking

All services communicate via a single Docker bridge network: `lakehouse-net`

**Internal Communication:**
- Services use container names for DNS resolution
- Example: `http://minio:9000`, `thrift://hive-metastore:9083`

**External Access (Development):**
- MinIO Console: http://localhost:9001
- Spark Master UI: http://localhost:8080
- Trino UI: http://localhost:8086
- Superset: http://localhost:8088
- Airflow: http://localhost:8089
- PostgreSQL: localhost:5432 (metastore), :5433 (superset), :5434 (airflow)

**External Access (Production):**
- Only essential ports exposed
- HTTPS enforced where possible
- No database ports exposed externally

## Security

### Development

- Simple passwords (e.g., minio/minio123)
- No HTTPS enforcement
- Database ports exposed for debugging
- Relaxed security policies

### Staging

- Strong passwords (placeholders)
- HTTPS enabled (if certificates available)
- Database ports not exposed
- Production-like security

### Production

- **MUST CHANGE ALL PASSWORDS** marked with `CHANGE_THIS`
- HTTPS enforced
- No debug ports exposed
- Secure cookies enabled
- CSRF protection enabled
- Session security enabled

**Critical Production Passwords:**
- MinIO: `MINIO_ROOT_PASSWORD`
- Metastore DB: `METASTORE_DB_PASSWORD`
- Superset: `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_PASSWORD`
- Airflow: `AIRFLOW_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`
- Grafana: `GRAFANA_ADMIN_PASSWORD`

## Volumes

Persistent data stored in Docker volumes:

**Data Volumes:**
- `minio-data`: Object storage (lakehouse data)
- `spark-warehouse`: Spark temporary data
- `spark-checkpoints`: Spark checkpoints

**Database Volumes:**
- `metastore-db`: Hive Metastore metadata
- `superset-db`: Superset application data
- `airflow-db`: Airflow state and metadata

**Application Volumes:**
- `airflow-logs`: Airflow task logs
- `dbt-logs`: dbt run logs

**Backup Strategy:**
- Database backups: Daily (configured in .env)
- Retention: 7 days (staging), 30 days (production)
- Location: `./backups/` (mounted in production overrides)

## Monitoring (Optional)

Production override includes optional Prometheus + Grafana stack:

**To Enable:**
1. Uncomment monitoring services in `docker-compose.override.prod.yaml`
2. Set `ENABLE_MONITORING=true` in `.env.prod`
3. Create monitoring configs:
   - `./monitoring/prometheus.yml`
   - `./monitoring/grafana/dashboards/`
   - `./monitoring/grafana/datasources/`

**Metrics Collected:**
- Container metrics (cAdvisor)
- Host metrics (node-exporter)
- MinIO metrics (built-in Prometheus endpoint)
- Custom application metrics

**Access:**
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

## Troubleshooting

### Common Issues

**1. Services not starting in order:**
- Check health checks: `docker-compose ps`
- View service logs: `docker-compose logs <service-name>`
- Verify dependencies in docker-compose.yaml

**2. Out of memory errors:**
- Check current usage: `docker stats`
- Increase resource limits in `.env` file
- Reduce worker count in development

**3. Port conflicts:**
- Check port usage: `netstat -an | findstr :<port>`
- Modify port mappings in `.env` file
- Use override files for environment-specific ports

**4. Health check failures:**
- Increase `HEALTHCHECK_START_PERIOD` for slow services
- Check service logs for errors
- Verify network connectivity between services

### Diagnostic Commands

```bash

# View all container status

docker-compose ps

# View logs for specific service

docker-compose logs -f <service-name>

# Check resource usage

docker stats

# Verify network connectivity

docker-compose exec <service> ping <other-service>

# Check health status

docker inspect <container-name> | grep -A 10 Health

# Restart specific service

docker-compose restart <service-name>
```

## See Also

- [Deployment Guide](./DEPLOYMENT.md) - Deployment procedures
- [Architecture](./ARCHITECTURE.md) - System architecture details
- [Configuration](./CONFIGURATION.md) - Configuration management
- [Quick Start](./QUICK_START.md) - Getting started guide
