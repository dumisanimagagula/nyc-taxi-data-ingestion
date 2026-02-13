# Deployment Guide

## Overview

This guide provides step-by-step deployment procedures for the NYC Taxi Data Lakehouse across different environments.

## Prerequisites

### System Requirements

**Development:**
- 16GB RAM minimum (32GB recommended)
- 8 CPU cores minimum
- 50GB free disk space
- Docker Desktop or Docker Engine
- Docker Compose v2.0+

**Staging:**
- 32GB RAM minimum (64GB recommended)
- 16 CPU cores minimum
- 100GB free disk space
- Docker Desktop or Docker Engine
- Docker Compose v2.0+

**Production:**
- 64GB RAM minimum (128GB recommended)
- 32 CPU cores minimum
- 500GB free disk space (1TB+ for data storage)
- Docker Engine (Linux host recommended)
- Docker Compose v2.0+

### Software Requirements

- Docker Engine 20.10+
- Docker Compose 2.0+
- Git
- PowerShell (Windows) or Bash (Linux/Mac)

### Pre-Deployment Checklist

- [ ] Docker daemon running
- [ ] Sufficient disk space available
- [ ] Required ports available (9000, 9001, 8080, 8086, 8088, 8089, 9083)
- [ ] Network connectivity to internet (for pulling images)
- [ ] SSL certificates prepared (for production HTTPS)
- [ ] Passwords changed in `.env` file (for staging/production)

## Development Deployment

### Quick Start

```powershell

# 1. Clone repository (if not already done)

git clone <repository-url>
cd nyc-taxi-data-ingestion

# 2. Copy development environment file

Copy-Item .env.dev .env

# 3. Start development stack

docker-compose -f docker-compose.yaml -f docker-compose.override.dev.yaml up -d

# 4. Verify all services are healthy

docker-compose ps

# 5. Access services

# MinIO Console: http://localhost:9001 (minio/minio123)

# Spark UI: http://localhost:8080

# Trino UI: http://localhost:8086

# Superset: http://localhost:8088 (admin/admin)

# Airflow: http://localhost:8089 (airflow/airflow)

```text

### Development Configuration

**Environment Variables (.env.dev):**
- Simple credentials (minio/minio123, admin/admin)
- Reduced resources (50% of production)
- No auto-restart (easier debugging)
- Fast health checks (15s interval)
- Minimal logging (5MB files)

**Development Features:**
- Only 1 Spark worker (reduced footprint)
- All database ports exposed for inspection
- Debug logging enabled in Airflow
- Hot reload for code changes

### Stopping Development Stack

```powershell

# Stop all services (preserves data)

docker-compose -f docker-compose.yaml -f docker-compose.override.dev.yaml down

# Stop and remove all data (clean slate)

docker-compose -f docker-compose.yaml -f docker-compose.override.dev.yaml down -v
```text

## Staging Deployment

### Pre-Deployment Steps

```powershell

# 1. Copy staging environment file

Copy-Item .env.staging .env

# 2. Review and customize passwords

notepad .env  # Change passwords marked with "staging"

# 3. (Optional) Prepare SSL certificates

# If using HTTPS, place certificates in ./certs/

# - cert.pem

# - key.pem

# 4. Create backup directory

New-Item -ItemType Directory -Path backups/metastore
New-Item -ItemType Directory -Path backups/superset
New-Item -ItemType Directory -Path backups/airflow
```text

### Deployment

```powershell

# 1. Pull latest images

docker-compose -f docker-compose.yaml -f docker-compose.override.staging.yaml pull

# 2. Build custom images

docker-compose -f docker-compose.yaml -f docker-compose.override.staging.yaml build

# 3. Start staging stack

docker-compose -f docker-compose.yaml -f docker-compose.override.staging.yaml up -d

# 4. Monitor startup progress

docker-compose logs -f

# Wait for all health checks to pass (5-7 minutes expected)

# 5. Verify all services

docker-compose ps
```

### Post-Deployment Verification

```powershell

# 1. Check service health

docker-compose ps | Select-String "healthy"

# 2. Verify MinIO buckets

docker-compose exec minio mc ls myminio/

# Expected: lakehouse, bronze, silver, gold, warehouse

# 3. Verify Hive Metastore connection

docker-compose exec trino trino --execute "SHOW CATALOGS;"

# Expected: iceberg catalog listed

# 4. Verify Airflow DAGs

# Access http://localhost:8089

# Login with credentials from .env

# Verify DAG appears and can be unpause

# 5. Check logs for errors

docker-compose logs | Select-String "ERROR"
```text

## Production Deployment

### Pre-Deployment Steps

```powershell

# 1. Copy production environment file

Copy-Item .env.prod .env

# 2. CRITICAL: Change ALL passwords

notepad .env

# Search for "CHANGE_THIS" and replace all occurrences

# Use strong passwords (minimum 16 characters, mixed case, numbers, symbols)

# 3. Prepare SSL certificates

# Place production certificates in ./certs/prod/

# - minio.crt, minio.key

# - trino.crt, trino.key

# - superset.crt, superset.key

# 4. Set up backup storage

New-Item -ItemType Directory -Path backups/metastore
New-Item -ItemType Directory -Path backups/superset
New-Item -ItemType Directory -Path backups/airflow

# 5. Configure monitoring (optional)

New-Item -ItemType Directory -Path monitoring/grafana/dashboards
New-Item -ItemType Directory -Path monitoring/grafana/datasources

# Copy Prometheus config: monitoring/prometheus.yml

```text

### Security Checklist

- [ ] All `CHANGE_THIS` passwords replaced
- [ ] `SUPERSET_SECRET_KEY` is minimum 32 characters random string
- [ ] Different passwords for each service
- [ ] SSL certificates installed and valid
- [ ] `ENFORCE_HTTPS=true` set
- [ ] Firewall rules configured (only expose necessary ports)
- [ ] Database ports NOT exposed externally
- [ ] Admin emails updated in `.env`

### Deployment

```powershell

# 1. Final environment variable check

Get-Content .env | Select-String "CHANGE_THIS"

# Should return NO results

# 2. Pull latest images

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml pull

# 3. Build custom images

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml build --no-cache

# 4. Start production stack

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml up -d

# 5. Monitor startup (expect 7-10 minutes)

docker-compose logs -f

# 6. Wait for all health checks

Start-Sleep -Seconds 600  # 10 minutes

# 7. Verify deployment

docker-compose ps
```text

### Post-Deployment Verification

```powershell

# 1. Verify all services healthy

$healthy = docker-compose ps | Select-String "healthy" | Measure-Object
Write-Host "Healthy services: $($healthy.Count)/16"

# 2. Test MinIO access

docker-compose exec minio mc admin info myminio

# 3. Test Trino queries

docker-compose exec trino trino --execute "SELECT 1;"

# 4. Test Superset login

# Navigate to https://localhost:8088

# Verify HTTPS certificate

# Login with admin credentials

# 5. Test Airflow access

# Navigate to https://localhost:8089 (if HTTPS configured)

# Verify authentication works

# 6. Check for errors

docker-compose logs --since=10m | Select-String "ERROR|FATAL"

# 7. Verify resource usage

docker stats --no-stream
```

### Enabling Monitoring (Production)

```powershell

# 1. Uncomment monitoring services in docker-compose.override.prod.yaml

# Edit file and uncomment prometheus and grafana sections

# 2. Create Prometheus configuration

New-Item -Path monitoring -ItemType Directory -Force
$prometheusConfig = @"
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'minio'
    static_configs:
      - targets: ['minio:9000']
    metrics_path: /minio/v2/metrics/cluster

  - job_name: 'docker'
    static_configs:
      - targets: ['host.docker.internal:9323']
"@
Set-Content -Path monitoring/prometheus.yml -Value $prometheusConfig

# 3. Restart stack with monitoring

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml up -d

# 4. Access monitoring

# Prometheus: http://localhost:9090

# Grafana: http://localhost:3000

```text

## Backup Procedures

### Manual Backup

```powershell

# 1. Create backup directory

$backupDate = Get-Date -Format "yyyyMMdd_HHmmss"
$backupDir = "backups/manual/$backupDate"
New-Item -ItemType Directory -Path $backupDir -Force

# 2. Backup PostgreSQL databases

docker-compose exec metastore-db pg_dump -U hive metastore > "$backupDir/metastore.sql"
docker-compose exec superset-db pg_dump -U superset superset > "$backupDir/superset.sql"
docker-compose exec airflow-db pg_dump -U airflow airflow > "$backupDir/airflow.sql"

# 3. Backup MinIO data (optional - can be large)

docker-compose exec minio mc mirror myminio/lakehouse "$backupDir/lakehouse"

# 4. Backup configurations

Copy-Item .env "$backupDir/env"
Copy-Item docker-compose.yaml "$backupDir/"
Copy-Item config/ "$backupDir/config" -Recurse

# 5. Create backup archive

Compress-Archive -Path $backupDir -DestinationPath "$backupDir.zip"
```text

### Automated Backup (Production)

Backups are configured via environment variables:
- `BACKUP_ENABLED=true`
- `BACKUP_RETENTION_DAYS=30`
- `BACKUP_SCHEDULE="0 2 * * *"` (daily at 2 AM)

Backups are stored in mounted volume: `./backups/`

## Restore Procedures

### Restoring from Backup

```powershell

# 1. Stop stack

docker-compose down

# 2. Extract backup

$backupFile = "backups/manual/20250211_120000.zip"
Expand-Archive -Path $backupFile -DestinationPath temp_restore/

# 3. Restore environment

Copy-Item temp_restore/env .env

# 4. Remove old volumes

docker volume rm lakehouse-prod_metastore-db
docker volume rm lakehouse-prod_superset-db
docker volume rm lakehouse-prod_airflow-db

# 5. Start databases only

docker-compose up -d metastore-db superset-db airflow-db

# Wait for health checks

Start-Sleep -Seconds 30

# 6. Restore database dumps

Get-Content temp_restore/20250211_120000/metastore.sql | docker-compose exec -T metastore-db psql -U hive -d metastore
Get-Content temp_restore/20250211_120000/superset.sql | docker-compose exec -T superset-db psql -U superset -d superset
Get-Content temp_restore/20250211_120000/airflow.sql | docker-compose exec -T airflow-db psql -U airflow -d airflow

# 7. Start full stack

docker-compose up -d

# 8. Verify restoration

docker-compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg;"
```text

## Rolling Updates

### Updating Services Without Downtime

```powershell

# 1. Pull latest images

docker-compose pull <service-name>

# 2. Recreate service

docker-compose up -d --no-deps --force-recreate <service-name>

# 3. Verify health

docker-compose ps <service-name>

# 4. Check logs

docker-compose logs -f <service-name>
```

### Full Stack Update

```powershell

# 1. Create backup (see above)

# 2. Pull all latest images

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml pull

# 3. Recreate all services

docker-compose -f docker-compose.yaml -f docker-compose.override.prod.yaml up -d --force-recreate

# 4. Monitor startup

docker-compose logs -f

# 5. Verify all services

docker-compose ps
```text

## Rollback Procedures

### Rolling Back Failed Deployment

```powershell

# 1. Identify failing service

docker-compose ps
docker-compose logs <failing-service>

# 2. Restore previous version

# Option A: Use specific image tag

# Edit docker-compose.yaml to use previous image version

docker-compose up -d <service-name>

# Option B: Full rollback from backup

# Follow "Restore Procedures" above

# 3. Verify rollback

docker-compose ps
docker-compose logs <service-name>
```text

## Scaling

### Scaling Spark Workers

```powershell

# Add more workers

docker-compose up -d --scale spark-worker=4

# Or edit docker-compose.yaml to add spark-worker-3, spark-worker-4, etc.

```text

### Scaling Airflow (Future: Celery Executor)

For high-volume production, consider migrating to Celery Executor:
1. Add Redis service
2. Add multiple workers
3. Configure Celery executor in `.env`

## Monitoring and Alerting

### Key Metrics to Monitor

**Resource Usage:**
- CPU usage per service (< 80%)
- Memory usage per service (< 90%)
- Disk usage (< 80%)
- Network I/O

**Application Metrics:**
- Airflow DAG success rate
- Trino query latency
- Spark job duration
- MinIO throughput

**Health:**
- Service health check status
- Container restart count
- Failed health checks

### Alerting Rules (if monitoring enabled)

Configure Prometheus alerts for:
- Service down (any service unhealthy > 5 minutes)
- High CPU usage (> 90% for 10 minutes)
- High memory usage (> 95% for 5 minutes)
- Disk space low (< 20%)
- Failed Airflow DAG runs

## Troubleshooting

### Common Deployment Issues

**1. Port Already in Use**
```powershell

# Find process using port

netstat -ano | findstr :<port>

# Kill process

taskkill /PID <pid> /F

# Or change port in .env file

```

**2. Out of Memory**
```powershell

# Check Docker memory limit

docker info | Select-String "Memory"

# Increase Docker Desktop memory (Settings > Resources > Memory)

# Or reduce worker count in .env

```text

**3. Health Check Failures**
```powershell

# Increase startup period

# Edit .env: HEALTHCHECK_START_PERIOD=120s

# Check service logs

docker-compose logs <service-name>

# Manually test health check

docker-compose exec <service> <health-check-command>
```text

**4. Permission Denied Errors (Windows)**
```powershell

# Run PowerShell as Administrator

# Check Docker Desktop file sharing settings

# Ensure workspace directory is in allowed paths

```text

### Emergency Procedures

**If Stack Becomes Unresponsive:**
```powershell

# 1. Force stop all containers

docker-compose kill

# 2. Remove containers (preserves volumes)

docker-compose rm -f

# 3. Restart stack

docker-compose up -d

# 4. If still failing, check Docker daemon

Restart-Service docker
```

**If Data Corruption Suspected:**
```powershell

# 1. Stop stack

docker-compose down

# 2. Restore from last backup

# (See Restore Procedures above)

# 3. If no backup, reset databases

docker volume rm <database-volume>
docker-compose up -d
```text

## Best Practices

### Development

- Commit `.env.dev` to version control
- Use `docker-compose down` (not `down -v`) to preserve data
- Check logs regularly: `docker-compose logs -f`
- Run validation tests before pushing changes

### Staging

- Mirror production configuration
- Test deployment procedures before production
- Run data quality checks after deployment
- Document any issues for production deployment

### Production

- **NEVER commit `.env.prod` to version control**
- Always create backup before deployment
- Deploy during low-traffic windows
- Monitor for 24 hours after deployment
- Have rollback plan ready
- Keep deployment logs for audit trail

## See Also

- [Infrastructure Documentation](./INFRASTRUCTURE.md) - Detailed service specs
- [Architecture](./ARCHITECTURE.md) - System design
- [Configuration](./CONFIGURATION.md) - Config management
- [Quick Start](./QUICK_START.md) - Getting started
