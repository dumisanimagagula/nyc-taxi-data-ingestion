variable "name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "lakehouse_bucket_arns" {
  description = "ARNs of lakehouse S3 buckets [bronze, silver, gold, warehouse]"
  type        = list(string)
}

variable "eks_oidc_provider_arn" {
  description = "ARN of the EKS OIDC provider for IRSA"
  type        = string
}

variable "eks_oidc_provider_url" {
  description = "URL of the EKS OIDC provider (without https://)"
  type        = string
}
