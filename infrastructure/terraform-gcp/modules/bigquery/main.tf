# ============================================================================
# BIGQUERY MODULE — Free Tier (10 GB storage + 1 TB queries/month)
# ============================================================================

resource "google_bigquery_dataset" "warehouse" {
  dataset_id    = "${replace(var.project_name, "-", "_")}_${var.environment}"
  friendly_name = "NYC Taxi Lakehouse (${var.environment})"
  description   = "Gold-layer analytics warehouse for NYC Taxi data"
  location      = var.location
  project       = var.project_id

  # Free tier: 10 GB storage included
  default_table_expiration_ms     = null
  default_partition_expiration_ms = null

  labels = {
    layer       = "gold"
    environment = var.environment
    project     = replace(var.project_name, "-", "_")
  }
}

# --- Analytics tables -------------------------------------------------------

resource "google_bigquery_table" "daily_trip_stats" {
  dataset_id          = google_bigquery_dataset.warehouse.dataset_id
  table_id            = "daily_trip_stats"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "pickup_date"
  }

  clustering = ["pickup_location_id"]

  schema = jsonencode([
    { name = "pickup_date", type = "DATE", mode = "REQUIRED" },
    { name = "pickup_location_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "year", type = "INTEGER", mode = "NULLABLE" },
    { name = "month", type = "INTEGER", mode = "NULLABLE" },
    { name = "day_of_week", type = "STRING", mode = "NULLABLE" },
    { name = "total_trips", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_passengers", type = "INTEGER", mode = "NULLABLE" },
    { name = "avg_trip_distance", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_fare_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "total_fare_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_trip_duration", type = "FLOAT", mode = "NULLABLE" },
  ])

  labels = {
    layer = "gold"
    model = "daily_trip_stats"
  }
}

resource "google_bigquery_table" "revenue_by_payment_type" {
  dataset_id          = google_bigquery_dataset.warehouse.dataset_id
  table_id            = "revenue_by_payment_type"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "period_start"
  }

  clustering = ["payment_type"]

  schema = jsonencode([
    { name = "period_start", type = "DATE", mode = "REQUIRED" },
    { name = "payment_type", type = "INTEGER", mode = "NULLABLE" },
    { name = "year", type = "INTEGER", mode = "NULLABLE" },
    { name = "month", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_trips", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_revenue", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_fare", type = "FLOAT", mode = "NULLABLE" },
    { name = "total_tips", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_tip_percentage", type = "FLOAT", mode = "NULLABLE" },
  ])

  labels = {
    layer = "gold"
    model = "revenue_by_payment_type"
  }
}

resource "google_bigquery_table" "hourly_location_analysis" {
  dataset_id          = google_bigquery_dataset.warehouse.dataset_id
  table_id            = "hourly_location_analysis"
  project             = var.project_id
  deletion_protection = false

  clustering = ["pickup_location_id", "hour_of_day"]

  schema = jsonencode([
    { name = "pickup_location_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "dropoff_location_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "hour_of_day", type = "INTEGER", mode = "NULLABLE" },
    { name = "day_of_week", type = "STRING", mode = "NULLABLE" },
    { name = "total_trips", type = "INTEGER", mode = "NULLABLE" },
    { name = "avg_trip_distance", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_fare_amount", type = "FLOAT", mode = "NULLABLE" },
    { name = "avg_trip_duration", type = "FLOAT", mode = "NULLABLE" },
    { name = "total_passengers", type = "INTEGER", mode = "NULLABLE" },
  ])

  labels = {
    layer = "gold"
    model = "hourly_location_analysis"
  }
}
