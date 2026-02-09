variable "prefix" {
  description = "A unique prefix for naming resources."
  type        = string
}

variable "location" {
  description = "The Azure region."
  type        = string
}

variable "resource_group_name" {
  description = "The name of the resource group."
  type        = string
}

variable "vnet_subnet_id" {
  description = "The ID of the subnet for the ACA environment."
  type        = string
}

variable "container_image_api" {
  description = "The container image for the API."
  type        = string
  default     = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest"
}

variable "container_image_worker" {
  description = "The container image for the worker."
  type        = string
  default     = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest"
}

variable "cpu" {
  description = "Individual container CPU allocation."
  type        = number
  default     = 0.5
}

variable "memory" {
  description = "Individual container memory allocation."
  type        = string
  default     = "1Gi"
}

variable "database_url" {
  description = "The database connection string."
  type        = string
  sensitive   = true
}

variable "jwt_secret" {
  description = "The secret key for signing JWT tokens."
  type        = string
  sensitive   = true
  default     = "dev-secret-keep-it-safe-123"
}
