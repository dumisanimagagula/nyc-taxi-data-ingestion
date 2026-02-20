# Superset Dashboard Configurations

This directory contains pre-configured Superset dashboard definitions for monitoring the NYC Taxi lakehouse data quality and lineage.

## Available Dashboards

### 1. Data Quality Monitoring (`data_quality_monitoring.json`)

Comprehensive data quality metrics dashboard with 10 charts:

**Charts Included:**

1. **Overall Quality Score Trend** (Line Chart)
   - Track quality scores over time by layer
   - Helps identify quality degradation early

2. **Quality Dimensions Breakdown** (Radar Chart)
   - Multi-dimensional view: Completeness, Validity, Consistency, Accuracy, Timeliness
   - Compare current state across all quality dimensions

3. **Error Count by Type** (Donut Chart)
   - Distribution of error types (NULL_VALUE, TYPE_MISMATCH, RANGE_VIOLATION, etc.)
   - Identify most common data quality issues

4. **Error Severity Heatmap** (Heatmap)
   - Severity distribution across tables
   - Quickly identify critical issues

5. **Top 10 Error-Prone Columns** (Bar Chart)
   - Columns with most quality errors
   - Focus improvement efforts on problematic columns

6. **Anomaly Detection Alerts** (Table)
   - Recent anomalies detected
   - Real-time alert view

7. **Data Completeness by Column** (Distributed Bar Chart)
   - Null percentage distribution
   - Monitor data completeness trends

8. **Quality Score Distribution** (Histogram)
   - Distribution of overall quality scores
   - Understand typical quality ranges

9. **Daily Quality Metrics Summary** (Big Number)
   - Latest overall quality score
   - At-a-glance health metric

10. **Error Trend - Last 30 Days** (Line Chart)
    - Error count trends by severity
    - Long-term quality monitoring

**Refresh Rate:** 5 minutes
**Data Sources:** `data_quality.metrics`, `data_quality.row_errors`

---

### 2. Data Lineage & Pipeline Flow (`data_lineage_flow.json`)

Track data transformations and dependencies across layers with 10 charts:

**Charts Included:**

1. **Lineage Flow - Sankey Diagram** (Sankey)
   - Visual data flow from Bronze → Silver → Gold
   - Understand transformation paths

2. **Lineage Events by Type** (Pie Chart)
   - Distribution of event types (INGESTION, TRANSFORMATION, AGGREGATION, etc.)
   - Pipeline operation breakdown

3. **Row Count Changes by Layer** (Mixed Time Series)
   - Track source/target row counts
   - Monitor data volume changes

4. **Transformation Timeline** (Line Chart)
   - Timeline of transformation events
   - Track pipeline execution patterns

5. **Table Dependency Matrix** (Heatmap)
   - Table-to-table dependencies
   - Impact analysis visualization

6. **Recent Lineage Events** (Table)
   - Latest transformation events with details
   - Detailed lineage audit trail

7. **Layer Processing Volume** (Bar Chart)
   - Data volume processed by each layer
   - Capacity planning insights

8. **Column-Level Transformations** (Table)
   - Detailed column lineage information
   - Understand derived column origins

9. **Pipeline Run Statistics** (Big Number)
   - Total transformations in last run
   - Quick pipeline health check

10. **Data Loss/Gain Analysis** (Paired T-Test)
    - Analyze data volume changes
    - Detect unexpected data loss/gain

**Refresh Rate:** 10 minutes
**Data Sources:** `data_quality.lineage_events`
**Features:** Cross-filtering enabled

---

## Installation

### Prerequisites

1. **Superset Running**: Ensure Apache Superset is running (port 8088)
2. **Database Connection**: Configure Trino connection in Superset
3. **Tables Created**: Ensure data quality tables exist:
   - `data_quality.metrics`
   - `data_quality.row_errors`
   - `data_quality.lineage_events`

### Import Method 1: Superset UI (Recommended)

1. **Login to Superset**: Navigate to `http://localhost:8088`
2. **Navigate to Dashboards**: Click "Dashboards" in top menu
3. **Import Dashboard**:
   - Click "⋮" (three dots) → "Import dashboard"
   - Select dashboard JSON file
   - Click "Import"
4. **Configure Data Sources**:
   - After import, edit each chart
   - Update datasource connection to your Trino instance
   - Save changes

### Import Method 2: Superset CLI

```bash

# From superset container

docker exec -it superset superset import-dashboards \
  -p /app/superset/dashboards/data_quality_monitoring.json

docker exec -it superset superset import-dashboards \
  -p /app/superset/dashboards/data_lineage_flow.json
```text

### Import Method 3: API (Automated)

```python
import requests
import json

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# Login

login_response = requests.post(
    f"{SUPERSET_URL}/api/v1/security/login",
    json={"username": USERNAME, "password": PASSWORD}
)
access_token = login_response.json()["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Import dashboard

with open("data_quality_monitoring.json") as f:
    dashboard_config = json.load(f)

response = requests.post(
    f"{SUPERSET_URL}/api/v1/dashboard/import/",
    headers=headers,
    files={"formData": json.dumps(dashboard_config)}
)

print(f"Import status: {response.status_code}")
```text

---

## Data Source Configuration

### Required Trino Tables

Ensure these tables exist in your Trino catalog:

```sql
-- Data quality metrics table
CREATE TABLE IF NOT EXISTS data_quality.metrics (
    timestamp TIMESTAMP,
    table_name VARCHAR,
    layer VARCHAR,
    overall_quality_score DOUBLE,
    completeness_score DOUBLE,
    validity_score DOUBLE,
    consistency_score DOUBLE,
    accuracy_score DOUBLE,
    timeliness_score DOUBLE,
    null_percentages VARCHAR,  -- JSON
    anomaly_count BIGINT,
    partition_day DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['partition_day', 'layer']
);

-- Row errors table
CREATE TABLE IF NOT EXISTS data_quality.row_errors (
    error_id VARCHAR,
    table_name VARCHAR,
    column_name VARCHAR,
    error_type VARCHAR,
    severity VARCHAR,
    timestamp TIMESTAMP,
    value VARCHAR,
    message VARCHAR,
    layer VARCHAR,
    partition_day DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['partition_day', 'layer']
);

-- Lineage events table
CREATE TABLE IF NOT EXISTS data_quality.lineage_events (
    event_id VARCHAR,
    event_type VARCHAR,
    source_table VARCHAR,
    target_table VARCHAR,
    pipeline_run_id VARCHAR,
    layer VARCHAR,
    timestamp TIMESTAMP,
    source_row_count BIGINT,
    target_row_count BIGINT,
    rows_added BIGINT,
    rows_deleted BIGINT,
    transformation_logic VARCHAR,
    column_lineage VARCHAR,  -- JSON
    partition_day DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['partition_day', 'layer']
);
```text

### Add Trino Connection in Superset

1. Navigate to **Data** → **Databases** → **+ Database**
2. Select **Trino** from supported databases
3. Enter connection details:
   ```

   Host: trino
   Port: 8080
   Catalog: lakehouse
   Schema: data_quality

   ```
4. Test connection and save

---

## Customization

### Modify Chart Queries

Edit dashboard JSON files and update `query_context` sections:

```json
{
  "queries": [
    {
      "metrics": [
        {
          "label": "Avg Quality Score",
          "expressionType": "SQL",
          "sqlExpression": "AVG(overall_quality_score)"
        }
      ],
      "groupby": ["layer"],
      "time_range": "Last 30 days"
    }
  ]
}
```

### Add New Charts

1. Create chart in Superset UI
2. Export dashboard JSON
3. Extract new chart configuration
4. Add to dashboard JSON file

### Change Colors

Update `json_metadata`:

```json
{
  "color_scheme": "supersetColors",
  "label_colors": {
    "bronze": "#1f77b4",
    "silver": "#ff7f0e",
    "gold": "#2ca02c"
  }
}
```text

---

## Troubleshooting

### Dashboard Not Loading

**Issue:** Dashboard shows errors after import

**Solutions:**
1. Check Trino connection: Test in **Data** → **Databases**
2. Verify tables exist: Run queries in SQL Lab
3. Check permissions: Ensure user has access to tables

### No Data Showing

**Issue:** Charts are empty

**Solutions:**
1. Verify data exists:
   ```sql
   SELECT COUNT(*) FROM data_quality.metrics;
   SELECT COUNT(*) FROM data_quality.row_errors;
   SELECT COUNT(*) FROM data_quality.lineage_events;
   ```
1. Check time range filters: Adjust in chart settings
2. Run pipeline: Generate some quality events first

### Charts Not Refreshing

**Issue:** Dashboard shows stale data

**Solutions:**

1. Manual refresh: Click refresh button in dashboard
2. Set auto-refresh: Edit dashboard metadata
3. Clear cache: **Settings** → **Manage** → **Clear cache**

### Connection Timeout

**Issue:** Trino queries timeout

**Solutions:**

1. Increase timeout: Edit database connection → **SQL Lab** → increase timeout
2. Optimize queries: Add partition filters
3. Check Trino resources: Ensure sufficient memory/CPU

---

## Best Practices

1. **Filter by Date**: Always use partition_day filters for performance
2. **Limit Row Counts**: Use row limits for table charts
3. **Cache Results**: Enable caching for frequently accessed charts
4. **Schedule Reports**: Set up email reports for daily quality summaries
5. **Create Alerts**: Configure SQL alerts for critical quality issues

---

## Maintenance

### Regular Tasks

- **Weekly**: Review dashboard performance, optimize slow queries
- **Monthly**: Update color schemes and labels for clarity
- **Quarterly**: Add new charts based on emerging quality patterns

### Backup Dashboards

Export regularly:

```bash

# Export dashboard

docker exec -it superset superset export-dashboards \
  -f /app/backups/dashboards_$(date +%Y%m%d).json
```text

---

## Additional Resources

- [Superset Documentation](https://superset.apache.org/docs/intro)
- [Data Quality Framework](../docs/DATA_QUALITY.md)
- [Trino SQL Reference](https://trino.io/docs/current/sql.html)
- [Chart Types Guide](https://superset.apache.org/docs/creating-charts-dashboards/creating-your-first-dashboard)

---

## Support

For issues or questions:
- Check [TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md)
- Review Superset logs: `docker logs superset`
- Open GitHub issue with dashboard export and error details
