# Development Environment Configuration
# This file contains non-sensitive configuration for the dev environment
# Secrets (passwords, RDP credentials) are provided via environment variables or GitHub Secrets

# Project & Environment
project_name = "lineagelogic"
environment  = "dev"
location     = "uksouth"

# Database Configuration
db_admin_login = "lladmin" # p LineageLogic2024!Dev@Azure
db_sku        = "B_Standard_B1ms"  # Burstable, cost-effective for dev

# Networking Configuration
networking_config = {
  vnet_address_space             = ["10.1.0.0/16"]
  aca_subnet_prefix             = ["10.1.1.0/24"]
  db_subnet_prefix              = ["10.1.2.0/24"]
  private_endpoint_subnet_prefix = ["10.1.3.0/24"]
  bastion_subnet_prefix          = ["10.1.4.0/26"]  # /26 minimum for Bastion
}

# Container Compute Configuration
compute_config = {
  cpu    = 1.0      # Increased from 0.25 to avoid OOM
  memory = "2.0Gi"  # Increased from 0.5Gi to avoid OOM
}

# Windows Jump Server Configuration (access via Azure Bastion)
jumpbox_admin_username = "azureadmin"
jumpbox_vm_size        = "Standard_D2s_v3"  # 2 vCPU, 8GB RAM for Windows GUI
# jumpbox_admin_password is provided via TF_VAR_jumpbox_admin_password env var (EMInemDONjazzy12!@)

# Azure Bastion (costs $140/month - only enable when needed!)
enable_bastion = false

# Direct RDP Access (Public IP with Whitelist)
# REPLACE "0.0.0.0/32" with your actual home/office IP from "What is my IP"
jumpbox_allowed_rdp_ip = "86.25.227.31/32"

# Tags
tags = {
  Environment = "dev"
  ManagedBy   = "Terraform"
  Project     = "LineageLogic"
  CostCenter  = "Engineering"
}

# Static Web App SKU (Standard required for Container App linking)
swa_sku = "Standard"
