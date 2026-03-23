output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.warehouse.dataset_id
}

output "dataset_friendly_name" {
  description = "BigQuery dataset friendly name"
  value       = google_bigquery_dataset.warehouse.friendly_name
}

output "daily_trip_stats_table_id" {
  description = "daily_trip_stats table ID"
  value       = google_bigquery_table.daily_trip_stats.table_id
}

output "revenue_by_payment_type_table_id" {
  description = "revenue_by_payment_type table ID"
  value       = google_bigquery_table.revenue_by_payment_type.table_id
}

output "hourly_location_analysis_table_id" {
  description = "hourly_location_analysis table ID"
  value       = google_bigquery_table.hourly_location_analysis.table_id
}
