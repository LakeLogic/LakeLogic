variable "project_name" {
  description = "The name of the project."
  type        = string
  default     = "lineagelogic"
}

variable "environment" {
  description = "The environment name (e.g., dev, stage, prod)."
  type        = string
  default     = "dev"
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
  default     = "B_Standard_B1ms" # Cost-effective for Dev
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
    bastion_subnet_prefix          = list(string)
  })
  default = {
    vnet_address_space             = ["10.1.0.0/16"] # Unique IP space for Dev
    aca_subnet_prefix             = ["10.1.0.0/23"]
    db_subnet_prefix              = ["10.1.2.0/24"]
    private_endpoint_subnet_prefix = ["10.1.3.0/24"]
    bastion_subnet_prefix          = ["10.1.4.0/26"]  # Min /26 for Bastion
  }
}

variable "compute_config" {
  description = "Configuration for container compute"
  type = object({
    cpu    = number
    memory = string
  })
  default = {
    cpu    = 0.25
    memory = "0.5Gi"
  }
}

# Windows Jump Server Configuration
variable "jumpbox_admin_username" {
  description = "Admin username for the Windows jump server (RDP access)"
  type        = string
  default     = "azureadmin"
}

variable "jumpbox_admin_password" {
  description = "Admin password for Windows jump server (min 12 chars, must meet complexity requirements)"
  type        = string
  sensitive   = true
  # Password requirements: min 12 chars, uppercase, lowercase, number, special char
}

variable "jumpbox_vm_size" {
  description = "VM size for Windows jump server"
  type        = string
  default     = "Standard_D2s_v3"  # 2 vCPU, 8GB RAM - good for Windows GUI
}

variable "enable_bastion" {
  description = "Enable Azure Bastion for jump server access (costs ~$140/month). Set to true only when you need to use the jump server."
  type        = bool
  default     = false  # Disabled by default to save $140/month
}

variable "jumpbox_allowed_rdp_ip" {
  description = "The public IP address allowed to RDP into the jumpbox (CIDR format, e.g. 1.2.3.4/32)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "swa_sku" {
  description = "The SKU tier and size for the Static Web App (Free or Standard)."
  type        = string
  default     = "Free"
}

variable "jwt_secret" {
  description = "The secret key for signing JWT tokens."
  type        = string
  sensitive   = true
  default     = "dev-secret-keep-it-safe-123"
}
