variable "name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "cluster_version" {
  type    = string
  default = "1.29"
}

# General node group
variable "node_instance_types" {
  type    = list(string)
  default = ["m5.xlarge"]
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "node_min_size" {
  type    = number
  default = 2
}

variable "node_max_size" {
  type    = number
  default = 10
}

# Spark node group
variable "spark_instance_types" {
  type    = list(string)
  default = ["r5.2xlarge"]
}

variable "spark_node_desired_size" {
  type    = number
  default = 2
}

variable "spark_node_min_size" {
  type    = number
  default = 0
}

variable "spark_node_max_size" {
  type    = number
  default = 20
}
