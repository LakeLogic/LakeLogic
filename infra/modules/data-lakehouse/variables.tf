variable "resource_group_name" {
  description = "The name of the Resource Group created by the core-vnet module."
  type        = string
}

variable "prefix" {
  description = "A unique prefix for naming the storage account."
  type        = string
}

variable "location" {
  description = "The Azure region for the storage account."
  type        = string
}

variable "account_replication_type" {
  description = "The storage redundancy level (e.g., LRS, GRS)."
  type        = string
  default     = "LRS"
}