output "bronze_bucket_name" {
  description = "Bronze layer GCS bucket name"
  value       = google_storage_bucket.bronze.name
}

output "silver_bucket_name" {
  description = "Silver layer GCS bucket name"
  value       = google_storage_bucket.silver.name
}

output "gold_bucket_name" {
  description = "Gold layer GCS bucket name"
  value       = google_storage_bucket.gold.name
}

output "bronze_bucket_url" {
  description = "Bronze layer GCS bucket URL"
  value       = google_storage_bucket.bronze.url
}

output "silver_bucket_url" {
  description = "Silver layer GCS bucket URL"
  value       = google_storage_bucket.silver.url
}

output "gold_bucket_url" {
  description = "Gold layer GCS bucket URL"
  value       = google_storage_bucket.gold.url
}
