# Test/Staging Environment Configuration
# This file contains non-sensitive configuration for the test environment
# Secrets (passwords, SSH keys) are provided via environment variables or GitHub Secrets

# Project & Environment
project_name = "lineagelogic"
environment  = "test"
location     = "uksouth"

# Database Configuration
db_admin_login = "lladmin"
db_sku        = "GP_Standard_D2s_v3"  # General Purpose for test

# Networking Configuration
networking_config = {
  vnet_address_space             = ["10.2.0.0/16"]
  aca_subnet_prefix             = ["10.2.1.0/24"]
  db_subnet_prefix              = ["10.2.2.0/24"]
  private_endpoint_subnet_prefix = ["10.2.3.0/24"]
}

# Container Compute Configuration
compute_config = {
  cpu    = 1.0
  memory = "2.0Gi"
}

# Jump Server Configuration
jumpbox_admin_username = "azureuser"
jumpbox_allowed_ips    = [
  # Only allow specific IPs in test environment
  # "1.2.3.4/32",    # Team member 1
  # "5.6.7.8/32",    # Team member 2
  # "10.0.0.0/24",   # Corporate VPN
]

# Tags
tags = {
  Environment = "test"
  ManagedBy   = "Terraform"
  Project     = "LineageLogic"
  CostCenter  = "Engineering"
}
