# ============================================================================
# EKS MODULE - Managed Kubernetes Cluster with Two Node Groups
# ============================================================================
# 1. "general" node group  - Airflow, Trino, Superset, Hive Metastore
# 2. "spark"   node group  - Memory-optimised, scales to zero between jobs
# ============================================================================

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
    tls = { source = "hashicorp/tls", version = ">= 4.0" }
  }
}

# ---- EKS Cluster IAM Role --------------------------------------------------
resource "aws_iam_role" "cluster" {
  name = "${var.name_prefix}-eks-cluster"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "${var.name_prefix}-eks-cluster-role" }
}

resource "aws_iam_role_policy_attachment" "cluster_policy" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role_policy_attachment" "cluster_vpc_controller" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
}

# ---- EKS Cluster -----------------------------------------------------------
resource "aws_eks_cluster" "main" {
  name     = "${var.name_prefix}-eks"
  version  = var.cluster_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids              = var.private_subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    security_group_ids      = [aws_security_group.cluster.id]
  }

  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  tags = { Name = "${var.name_prefix}-eks" }

  depends_on = [
    aws_iam_role_policy_attachment.cluster_policy,
    aws_iam_role_policy_attachment.cluster_vpc_controller,
  ]
}

# ---- Cluster Security Group ------------------------------------------------
resource "aws_security_group" "cluster" {
  name_prefix = "${var.name_prefix}-eks-cluster-"
  vpc_id      = var.vpc_id
  description = "EKS cluster security group"

  tags = { Name = "${var.name_prefix}-eks-cluster-sg" }

  lifecycle { create_before_destroy = true }
}

resource "aws_security_group_rule" "cluster_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.cluster.id
  cidr_blocks       = ["0.0.0.0/0"]
  description       = "Allow all outbound"
}

# ---- Node IAM Role ---------------------------------------------------------
resource "aws_iam_role" "node" {
  name = "${var.name_prefix}-eks-node"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Name = "${var.name_prefix}-eks-node-role" }
}

resource "aws_iam_role_policy_attachment" "node_worker" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# ---- Node Security Group ---------------------------------------------------
resource "aws_security_group" "node" {
  name_prefix = "${var.name_prefix}-eks-node-"
  vpc_id      = var.vpc_id
  description = "EKS worker node security group"

  tags = {
    Name                                                 = "${var.name_prefix}-eks-node-sg"
    "kubernetes.io/cluster/${aws_eks_cluster.main.name}" = "owned"
  }

  lifecycle { create_before_destroy = true }
}

resource "aws_security_group_rule" "node_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.node.id
  cidr_blocks       = ["0.0.0.0/0"]
  description       = "Allow all outbound"
}

resource "aws_security_group_rule" "node_self" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "-1"
  security_group_id        = aws_security_group.node.id
  source_security_group_id = aws_security_group.node.id
  description              = "Node-to-node communication"
}

resource "aws_security_group_rule" "node_from_cluster" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.node.id
  source_security_group_id = aws_security_group.cluster.id
  description              = "Cluster API to nodes"
}

resource "aws_security_group_rule" "node_kubelet_from_cluster" {
  type                     = "ingress"
  from_port                = 10250
  to_port                  = 10250
  protocol                 = "tcp"
  security_group_id        = aws_security_group.node.id
  source_security_group_id = aws_security_group.cluster.id
  description              = "Cluster to kubelet"
}

# ---- General Node Group (Airflow, Trino, Superset) -------------------------
resource "aws_eks_node_group" "general" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.name_prefix}-general"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.private_subnet_ids
  instance_types  = var.node_instance_types

  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    workload = "general"
  }

  tags = { Name = "${var.name_prefix}-general-node" }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}

# ---- Spark Node Group (memory-optimised, scales to zero) -------------------
resource "aws_eks_node_group" "spark" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.name_prefix}-spark"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.private_subnet_ids
  instance_types  = var.spark_instance_types

  scaling_config {
    desired_size = var.spark_node_desired_size
    min_size     = var.spark_node_min_size
    max_size     = var.spark_node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    workload = "spark"
  }

  taint {
    key    = "workload"
    value  = "spark"
    effect = "NO_SCHEDULE"
  }

  tags = { Name = "${var.name_prefix}-spark-node" }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
  ]
}

# ---- Cluster Auth (for Kubernetes provider) --------------------------------
data "aws_eks_cluster_auth" "main" {
  name = aws_eks_cluster.main.name
}

# ---- OIDC Provider for IRSA ------------------------------------------------
data "tls_certificate" "eks" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]

  tags = { Name = "${var.name_prefix}-eks-oidc" }
}
