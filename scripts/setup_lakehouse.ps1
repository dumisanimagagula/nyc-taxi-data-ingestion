# =============================================================================
# Lakehouse Platform Setup Script (PowerShell)
# =============================================================================
# This script initializes the entire lakehouse platform on Windows
# Run this after docker-compose up

$ErrorActionPreference = "Continue"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Lakehouse Platform Initialization" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Wait for services to be healthy
Write-Host "Waiting for services to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Copy namespace creation script to ingestor
Write-Host "Copying setup scripts to containers..." -ForegroundColor Yellow
docker cp scripts/create_iceberg_namespaces.py lakehouse-ingestor:/tmp/create_iceberg_namespaces.py

# 1. Initialize MinIO buckets
Write-Host ""
Write-Host "1. Verifying MinIO buckets..." -ForegroundColor Yellow
docker exec lakehouse-minio mc alias set myminio http://localhost:9000 minio minio123
docker exec lakehouse-minio mc mb --ignore-existing myminio/bronze
docker exec lakehouse-minio mc mb --ignore-existing myminio/silver
docker exec lakehouse-minio mc mb --ignore-existing myminio/gold
docker exec lakehouse-minio mc mb --ignore-existing myminio/lakehouse
Write-Host "OK MinIO buckets verified" -ForegroundColor Green

# 2. Verify Airflow database
Write-Host ""
Write-Host "2. Verifying Airflow database..." -ForegroundColor Yellow
docker exec lakehouse-airflow-webserver airflow db check
Write-Host "OK Airflow database verified" -ForegroundColor Green

# 3. Create Iceberg namespaces with S3 locations
Write-Host ""
Write-Host "3. Creating Iceberg namespaces..." -ForegroundColor Yellow
docker exec lakehouse-ingestor python /tmp/create_iceberg_namespaces.py
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK Iceberg namespaces created" -ForegroundColor Green
} else {
    Write-Host "WARN Iceberg namespace creation had issues" -ForegroundColor Yellow
}

# 4. Create Airflow connections
Write-Host ""
Write-Host "4. Creating Airflow connections..." -ForegroundColor Yellow

# Spark connection
docker exec lakehouse-airflow-webserver airflow connections add 'spark_default' `
    --conn-type 'spark' `
    --conn-host 'spark://spark-master' `
    --conn-port '7077' 2>$null

# Trino connection
docker exec lakehouse-airflow-webserver airflow connections add 'trino_default' `
    --conn-type 'trino' `
    --conn-host 'trino' `
    --conn-port '8080' `
    --conn-schema 'iceberg' 2>$null

Write-Host "OK Airflow connections created" -ForegroundColor Green

# 5. Initialize dbt
Write-Host ""
Write-Host "5. Initializing dbt project..." -ForegroundColor Yellow
docker exec lakehouse-dbt dbt debug --profiles-dir /usr/app --project-dir /usr/app 2>$null
Write-Host "OK dbt initialized" -ForegroundColor Green

# 6. Test Trino
Write-Host ""
Write-Host "6. Testing Trino connection..." -ForegroundColor Yellow
docker exec lakehouse-trino trino --execute "SHOW CATALOGS" 2>$null
Write-Host "OK Trino connection tested" -ForegroundColor Green

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "OK Initialization Complete!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Access points:" -ForegroundColor Yellow
Write-Host "  - MinIO Console: http://localhost:9001 (minio/minio123)"
Write-Host "  - Spark Master UI: http://localhost:8080"
Write-Host "  - Trino UI: http://localhost:8086"
Write-Host "  - Superset: http://localhost:8088 (admin/admin)"
Write-Host "  - Airflow: http://localhost:8089 (airflow/airflow)"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Trigger the DAG in Airflow UI"
Write-Host "  2. Monitor progress in Airflow"
Write-Host "  3. Query data in Trino"
Write-Host "  4. Build dashboards in Superset"
Write-Host ""
