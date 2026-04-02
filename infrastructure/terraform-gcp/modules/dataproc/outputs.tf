output "cluster_name" {
  description = "Name of the Dataproc cluster"
  value       = google_dataproc_cluster.spark.name
}

output "cluster_region" {
  description = "Region of the Dataproc cluster"
  value       = google_dataproc_cluster.spark.region
}

output "master_instance" {
  description = "Master instance name"
  value       = google_dataproc_cluster.spark.cluster_config[0].master_config[0].instance_names[0]
}
