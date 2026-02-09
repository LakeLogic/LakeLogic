variable "resource_group_name" {
  description = "The name of the Resource Group."
  type        = string
}

variable "prefix" {
  description = "A unique prefix for naming the PostgreSQL server."
  type        = string
}

variable "location" {
  description = "The Azure region for the database server."
  type        = string
}

variable "sku_name" {
  description = "The SKU for the PostgreSQL Flexible Server (e.g., Standard_B1ms, Standard_D2ds_v4)."
  type        = string
  default     = "Standard_B1ms" # Cost-effective default for dev
}

variable "db_admin_login" {
  description = "The administrator username for the PostgreSQL server."
  type        = string
  default     = "dbadmin"
}

variable "db_admin_password" {
  description = "The administrator password for the PostgreSQL server (should be sourced from a secure variable file)."
  type        = string
  sensitive   = true
}

variable "subnet_id" {
  description = "The ID of the subnet for the database."
  type        = string
}

variable "private_dns_zone_id" {
  description = "The ID of the private DNS zone for the database."
  type        = string
}