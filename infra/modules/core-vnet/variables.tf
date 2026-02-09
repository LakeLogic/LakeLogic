variable "prefix" {
  description = "A unique prefix for naming all resources in this environment (e.g., tpai-dev)."
  type        = string
}

variable "location" {
  description = "The Azure region to deploy resources into."
  type        = string
}

variable "vnet_address_space" {
  description = "The address space for the virtual network."
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "aca_subnet_prefix" {
  description = "The address prefix for the ACA subnet."
  type        = list(string)
  default     = ["10.0.1.0/23"]
}

variable "db_subnet_prefix" {
  description = "The address prefix for the database subnet."
  type        = list(string)
  default     = ["10.0.4.0/24"]
}

variable "private_endpoint_subnet_prefix" {
  description = "The address prefix for the private endpoint subnet."
  type        = list(string)
  default     = ["10.0.5.0/24"]
}

variable "bastion_subnet_prefix" {
  description = "The address prefix for Azure Bastion subnet (min /26 required)"
  type        = list(string)
  default     = ["10.0.6.0/26"]  # /26 is minimum size for Bastion
}

variable "enable_bastion" {
  description = "Enable Azure Bastion (costs ~$140/month). Set to false to save costs when not using jump server."
  type        = bool
  default     = false  # Disabled by default to save costs
}
