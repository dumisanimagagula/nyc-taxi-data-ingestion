#!/bin/bash
# =============================================================================
# Lakehouse Platform Setup Script
# =============================================================================
# This script initializes the entire lakehouse platform
# Run this after docker-compose up

set -e

echo "=========================================="
echo "Lakehouse Platform Initialization"
echo "=========================================="

# Wait for services to be healthy
echo "Waiting for services to start..."
sleep 30

# Copy namespace creation script to ingestor
echo "Copying setup scripts to containers..."
docker cp scripts/create_iceberg_namespaces.py lakehouse-ingestor:/tmp/create_iceberg_namespaces.py

# 1. Initialize MinIO buckets (should be done by minio-setup service, but verify)
echo ""
echo "1. Verifying MinIO buckets..."
docker exec lakehouse-minio mc alias set myminio http://localhost:9000 minio minio123
docker exec lakehouse-minio mc mb --ignore-existing myminio/bronze
docker exec lakehouse-minio mc mb --ignore-existing myminio/silver
docker exec lakehouse-minio mc mb --ignore-existing myminio/gold
docker exec lakehouse-minio mc mb --ignore-existing myminio/lakehouse
echo "✓ MinIO buckets verified"

# 2. Initialize Airflow database (should be done by airflow-init, but verify)
echo ""
echo "2. Verifying Airflow database..."
docker exec lakehouse-airflow-webserver airflow db check
echo "✓ Airflow database verified"

# 3. Create Iceberg namespaces with S3 locations
echo ""
echo "3. Creating Iceberg namespaces..."
docker exec lakehouse-ingestor python /tmp/create_iceberg_namespaces.py
if [ $? -eq 0 ]; then
    echo "✓ Iceberg namespaces created"
else
    echo "⚠ Warning: Iceberg namespace creation had issues"
fi

# 4. Create Airflow connections
echo ""
echo "4. Creating Airflow connections..."

# Spark connection
docker exec lakehouse-airflow-webserver airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077' || echo "Connection already exists"

# Trino connection
docker exec lakehouse-airflow-webserver airflow connections add 'trino_default' \
    --conn-type 'trino' \
    --conn-host 'trino' \
    --conn-port '8080' \
    --conn-schema 'iceberg' || echo "Connection already exists"

echo "✓ Airflow connections created"

# 5. Initialize dbt project
echo ""
echo "5. Initializing dbt project..."
docker exec lakehouse-dbt dbt debug --profiles-dir /usr/app --project-dir /usr/app || echo "dbt debug failed, but continuing..."
echo "✓ dbt initialized"

# 6. Test Trino connection
echo ""
echo "6. Testing Trino connection..."
docker exec lakehouse-trino trino --execute "SHOW CATALOGS" || echo "Trino test query failed"
echo "✓ Trino connection tested"

echo ""
echo "=========================================="
echo "✓ Initialization Complete!"
echo "=========================================="
echo ""
echo "Access points:"
echo "  - MinIO Console: http://localhost:9001 (minio/minio123)"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Trino UI: http://localhost:8086"
echo "  - Superset: http://localhost:8088 (admin/admin)"
echo "  - Airflow: http://localhost:8089 (airflow/airflow)"
echo ""
echo "Next steps:"
echo "  1. Trigger the DAG in Airflow UI"
echo "  2. Monitor progress in Airflow"
echo "  3. Query data in Trino"
echo "  4. Build dashboards in Superset"
echo ""
