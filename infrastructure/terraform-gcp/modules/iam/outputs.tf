output "service_account_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline.email
}

output "service_account_key" {
  description = "Pipeline service account key (base64-encoded JSON)"
  value       = google_service_account_key.pipeline_key.private_key
  sensitive   = true
}
