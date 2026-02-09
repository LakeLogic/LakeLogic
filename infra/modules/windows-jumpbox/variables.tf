variable "prefix" {
  description = "Resource prefix"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for the jump server"
  type        = string
}

variable "postgres_host" {
  description = "PostgreSQL server hostname for connection info"
  type        = string
}

variable "vm_size" {
  description = "VM size for the jump server"
  type        = string
  default     = "Standard_D2s_v3"  # 2 vCPU, 8GB RAM - good for Windows GUI
}

variable "admin_username" {
  description = "Admin username for RDP access"
  type        = string
  default     = "azureadmin"
}

variable "admin_password" {
  description = "Admin password for RDP access (min 12 chars, complexity requirements)"
  type        = string
  sensitive   = true
}

variable "allowed_rdp_ip" {
  description = "The public IP address allowed to RDP into the jumpbox (CIDR format, e.g. 1.2.3.4/32)"
  type        = string
  default     = "0.0.0.0/0" # Safety: User should override this in tfvars
}

variable "enable_public_ip" {
  description = "Whether to assign a public IP to the jumpbox"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
