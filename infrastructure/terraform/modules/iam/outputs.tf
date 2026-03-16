output "airflow_role_arn" {
  description = "IAM role ARN for Airflow service account"
  value       = aws_iam_role.airflow.arn
}

output "bronze_ingestor_role_arn" {
  description = "IAM role ARN for Bronze ingestor service account"
  value       = aws_iam_role.bronze_ingestor.arn
}

output "spark_role_arn" {
  description = "IAM role ARN for Spark service account"
  value       = aws_iam_role.spark.arn
}

output "trino_role_arn" {
  description = "IAM role ARN for Trino service account"
  value       = aws_iam_role.trino.arn
}

output "dbt_role_arn" {
  description = "IAM role ARN for dbt service account"
  value       = aws_iam_role.dbt.arn
}
