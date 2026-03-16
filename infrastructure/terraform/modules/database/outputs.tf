output "endpoint" {
  description = "RDS endpoint (host:port)"
  value       = aws_db_instance.metastore.endpoint
}

output "address" {
  description = "RDS hostname"
  value       = aws_db_instance.metastore.address
}

output "port" {
  description = "RDS port"
  value       = aws_db_instance.metastore.port
}

output "security_group_id" {
  description = "Security group ID for the RDS instance"
  value       = aws_security_group.metastore.id
}
