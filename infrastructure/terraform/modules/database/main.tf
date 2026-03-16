# ============================================================================
# DATABASE MODULE - RDS PostgreSQL for Hive Metastore
# ============================================================================
# Replaces the docker-compose metastore-db and airflow-db containers with
# a managed RDS Multi-AZ instance.
# ============================================================================

resource "aws_db_subnet_group" "metastore" {
  name       = "${var.name_prefix}-metastore"
  subnet_ids = var.private_subnet_ids
  tags       = { Name = "${var.name_prefix}-metastore-subnet-group" }
}

resource "aws_security_group" "metastore" {
  name_prefix = "${var.name_prefix}-metastore-"
  vpc_id      = var.vpc_id
  description = "Security group for Hive Metastore RDS"

  tags = { Name = "${var.name_prefix}-metastore-sg" }

  lifecycle { create_before_destroy = true }
}

resource "aws_security_group_rule" "metastore_ingress" {
  count = length(var.allowed_security_group_ids)

  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.metastore.id
  source_security_group_id = var.allowed_security_group_ids[count.index]
  description              = "PostgreSQL from EKS nodes"
}

resource "aws_security_group_rule" "metastore_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.metastore.id
  cidr_blocks       = ["0.0.0.0/0"]
  description       = "Allow all outbound"
}

resource "aws_db_instance" "metastore" {
  identifier = "${var.name_prefix}-metastore"

  engine                = "postgres"
  engine_version        = "15"
  instance_class        = var.db_instance_class
  allocated_storage     = var.allocated_storage
  max_allocated_storage = var.allocated_storage * 4
  storage_encrypted     = true

  db_name  = "metastore"
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.metastore.name
  vpc_security_group_ids = [aws_security_group.metastore.id]

  multi_az                  = var.environment == "prod"
  backup_retention_period   = var.environment == "prod" ? 7 : 1
  skip_final_snapshot       = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${var.name_prefix}-metastore-final" : null
  deletion_protection       = var.environment == "prod"

  performance_insights_enabled = true
  monitoring_interval          = 60

  tags = { Name = "${var.name_prefix}-metastore" }
}
