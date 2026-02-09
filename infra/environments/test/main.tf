terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

locals {
  prefix = "${var.project_name}-${var.environment}"
  common_tags = merge(var.tags, {
    Environment = var.environment
  })
}

# 1. Network Foundation
module "vnet" {
  source   = "../../modules/core-vnet"
  prefix   = local.prefix
  location = var.location
  
  vnet_address_space             = var.networking_config.vnet_address_space
  aca_subnet_prefix             = var.networking_config.aca_subnet_prefix
  db_subnet_prefix              = var.networking_config.db_subnet_prefix
  private_endpoint_subnet_prefix = var.networking_config.private_endpoint_subnet_prefix
}

# 2. Database Layer
module "db" {
  source              = "../../modules/rdb-db"
  prefix              = local.prefix
  location            = var.location
  resource_group_name = module.vnet.resource_group_name
  subnet_id           = module.vnet.db_subnet_id
  private_dns_zone_id = module.vnet.postgres_dns_zone_id
  
  db_admin_login    = var.db_admin_login
  db_admin_password = var.db_admin_password
  sku_name          = var.db_sku
}

# 3. Compute Layer
module "compute" {
  source              = "../../modules/container-compute"
  prefix              = local.prefix
  location            = var.location
  resource_group_name = module.vnet.resource_group_name
  vnet_subnet_id      = module.vnet.aca_subnet_id
  
  cpu    = var.compute_config.cpu
  memory = var.compute_config.memory
  
  container_image_api    = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest"
  container_image_worker = "mcr.microsoft.com/azuredocs/containerapps-helloworld:latest"

  database_url = "postgresql://${var.db_admin_login}:${var.db_admin_password}@${module.db.db_hostname}:5432/lineagelogic"
}

# 4. Data Lakehouse
module "lakehouse" {
  source              = "../../modules/data-lakehouse"
  prefix              = local.prefix
  location            = var.location
  resource_group_name = module.vnet.resource_group_name
}

# 5. Key Vault
module "key_vault" {
  source              = "../../modules/key-vault"
  prefix              = local.prefix
  location            = var.location
  resource_group_name = module.vnet.resource_group_name
  tenant_id           = var.tenant_id
  vnet_id             = module.vnet.vnet_id
  subnet_id           = module.vnet.private_subnet_id
  principal_id_github = var.github_sp_object_id
  principal_id_app    = "00000000-0000-0000-0000-000000000000" 
}

# 6. Frontend Layer
module "frontend" {
  source              = "../../modules/static-webapp"
  prefix              = local.prefix
  location            = "East Asia"
  resource_group_name = module.vnet.resource_group_name
  sku_tier            = "Free" # Save cost in Test
  sku_size            = "Free"
}
