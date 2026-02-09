# Production Environment Configuration
# This file contains non-sensitive configuration for the production environment
# Secrets (passwords, SSH keys) are provided via environment variables or GitHub Secrets

# Project & Environment
project_name = "lineagelogic"
environment  = "prod"
location     = "uksouth"

# Database Configuration
db_admin_login = "lladmin"
db_sku        = "GP_Standard_D4s_v3"  # Production-grade, high availability

# Networking Configuration
networking_config = {
  vnet_address_space             = ["10.0.0.0/16"]
  aca_subnet_prefix             = ["10.0.1.0/24"]
  db_subnet_prefix              = ["10.0.2.0/24"]
  private_endpoint_subnet_prefix = ["10.0.3.0/24"]
}

# Container Compute Configuration
compute_config = {
  cpu    = 2.0      # More resources for production
  memory = "4.0Gi"  # More memory for production workloads
}

# Jump Server Configuration
jumpbox_admin_username = "azureuser"
jumpbox_allowed_ips    = [
  # PRODUCTION: Only allow specific, known IPs
  # NEVER use 0.0.0.0/0 in production!
  # "203.0.113.10/32",   # Office IP
  # "198.51.100.0/24",   # Corporate VPN range
]

# Tags
tags = {
  Environment = "production"
  ManagedBy   = "Terraform"
  Project     = "LineageLogic"
  CostCenter  = "Engineering"
  Criticality = "High"
  BackupPolicy = "Daily"
}
