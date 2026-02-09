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
  description = "PostgreSQL server hostname for welcome message"
  type        = string
}

variable "vm_size" {
  description = "VM size for the jump server"
  type        = string
  default     = "Standard_B1s"
}

variable "admin_username" {
  description = "Admin username for SSH access"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key" {
  description = "SSH public key for authentication"
  type        = string
}

variable "allowed_ssh_ips" {
  description = "List of IP addresses allowed to SSH to the jump server"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Change this to your IP for better security
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}
