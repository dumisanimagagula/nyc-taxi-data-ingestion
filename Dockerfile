# Dockerfile for Lakehouse Bronze Layer Ingestor
# Production-ready image with Iceberg support

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY bronze/ ./bronze/
COPY config/ ./config/
COPY config.yaml .
COPY config.examples/ ./config.examples/

# Set Python path to include src
ENV PYTHONPATH=/app/src:/app:$PYTHONPATH

# Default command (override in docker-compose)
CMD ["python", "bronze/ingestors/ingest_to_iceberg.py", "--config", "config/pipelines/lakehouse_config.yaml"]
