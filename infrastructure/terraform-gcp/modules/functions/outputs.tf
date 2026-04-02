output "function_name" {
  description = "Name of the Cloud Function"
  value       = google_cloudfunctions2_function.trigger_silver.name
}

output "function_uri" {
  description = "URI of the deployed Cloud Function"
  value       = google_cloudfunctions2_function.trigger_silver.service_config[0].uri
}
