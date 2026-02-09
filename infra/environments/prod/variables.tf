variable "project_name" {
  description = "The name of the project."
  type        = string
  default     = "lineagelogic"
}

variable "environment" {
  description = "The environment name (e.g., dev, stage, prod)."
  type        = string
  default     = "prod"
}

variable "location" {
  description = "The Azure region to deploy to."
  type        = string
  default     = "uksouth"
}

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  type        = string
}

variable "github_sp_object_id" {
  description = "The Object ID of the GitHub Actions Service Principal."
  type        = string
  default     = ""
}

variable "db_admin_login" {
  description = "Admin username for the database."
  type        = string
  default     = "lladmin"
}

variable "db_admin_password" {
  description = "Admin password for the database."
  type        = string
  sensitive   = true
}

variable "db_sku" {
  description = "The database SKU."
  type        = string
  default     = "GP_Standard_D2ds_v4"
}

variable "tags" {
  description = "A mapping of tags to assign to the resource."
  type        = map(string)
  default     = {
    Project     = "LineageLogic"
    ManagedBy   = "Terraform"
    Architecture = "Sole-Azure"
  }
}

variable "networking_config" {
  description = "Configuration for virtual network and subnets"
  type = object({
    vnet_address_space             = list(string)
    aca_subnet_prefix             = list(string)
    db_subnet_prefix              = list(string)
    private_endpoint_subnet_prefix = list(string)
  })
  default = {
    vnet_address_space             = ["10.0.0.0/16"]
    aca_subnet_prefix             = ["10.0.0.0/23"]
    db_subnet_prefix              = ["10.0.2.0/24"]
    private_endpoint_subnet_prefix = ["10.0.3.0/24"]
  }
}

variable "compute_config" {
  description = "Configuration for container compute"
  type = object({
    cpu    = number
    memory = string
  })
  default = {
    cpu    = 0.5
    memory = "1.0Gi"
  }
}
