variable "name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "bronze_lifecycle_glacier_days" {
  type    = number
  default = 90
}

variable "silver_lifecycle_ia_days" {
  type    = number
  default = 60
}
