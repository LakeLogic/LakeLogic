variable "resource_group_name" {
  description = "The name of the Resource Group."
  type        = string
}

variable "prefix" {
  description = "A unique prefix for naming the Key Vault."
  type        = string
}

variable "location" {
  description = "The Azure region for the Key Vault."
  type        = string
}

variable "tenant_id" {
  description = "The Azure AD Tenant ID."
  type        = string
}

variable "principal_id_app" {
  description = "The Azure Managed Identity (Object ID) for the FastAPI/ETL Container Apps."
  type        = string
}

variable "principal_id_github" {
  description = "The Object ID of the GitHub Actions Service Principal."
  type        = string
  default     = "" # Optional
}

variable "subnet_id" {
  description = "The ID of the subnet for the private endpoint."
  type        = string
}

variable "vnet_id" {
  description = "The ID of the virtual network for DNS linking."
  type        = string
}