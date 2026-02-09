# Container Compute Module for Azure Container Apps

# 1. Log Analytics Workspace for monitoring
resource "azurerm_log_analytics_workspace" "logs" {
  name                = "${var.prefix}-logs"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

# 2. Azure Container Registry
resource "azurerm_container_registry" "acr" {
  name                = replace("${var.prefix}acr", "-", "")
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = true # For demo/simple setup, ideally use Managed Identity
}

# 3. Container App Environment
resource "azurerm_container_app_environment" "env" {
  name                           = "${var.prefix}-aca-env"
  location                       = var.location
  resource_group_name            = var.resource_group_name
  log_analytics_workspace_id     = azurerm_log_analytics_workspace.logs.id
  infrastructure_subnet_id       = var.vnet_subnet_id
  internal_load_balancer_enabled = false # Set to true if you want the whole environment internal
  
  lifecycle {
    ignore_changes = [
      infrastructure_resource_group_name  # Azure manages this, changes shouldn't force replacement
    ]
  }
}

# 4. API Container App (Publicly Accessible)
resource "azurerm_container_app" "api" {
  name                         = "${var.prefix}-api"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"

  identity {
    type = "SystemAssigned"
  }

  registry {
    server               = azurerm_container_registry.acr.login_server
    username             = azurerm_container_registry.acr.admin_username
    password_secret_name = "acr-password"
  }

  secret {
    name  = "acr-password"
    value = azurerm_container_registry.acr.admin_password
  }

  template {
    container {
      name   = "api"
      image  = var.container_image_api
      cpu    = var.cpu
      memory = var.memory

      env {
        name  = "ENVIRONMENT"
        value = "production"
      }

      env {
        name  = "DATABASE_URL"
        value = var.database_url
      }

      env {
        name  = "JWT_SECRET"
        value = var.jwt_secret
      }
    }
  }

  ingress {
    allow_insecure_connections = false
    external_enabled           = true
    target_port                = 8000
    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }
}

# 5. Worker Container App (Internal Only)
resource "azurerm_container_app" "worker" {
  name                         = "${var.prefix}-worker"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"

  identity {
    type = "SystemAssigned"
  }

  registry {
    server               = azurerm_container_registry.acr.login_server
    username             = azurerm_container_registry.acr.admin_username
    password_secret_name = "acr-password"
  }

  secret {
    name  = "acr-password"
    value = azurerm_container_registry.acr.admin_password
  }

  template {
    container {
      name   = "worker"
      image  = var.container_image_worker
      cpu    = var.cpu
      memory = var.memory

      env {
        name  = "ENVIRONMENT"
        value = "production"
      }
      
      env {
        name  = "DATABASE_URL"
        value = var.database_url
      }
      
      # Use an environment variable to tell the app to run as a worker
      env {
        name = "RUN_MODE"
        value = "worker"
      }
    }
  }
}

# 6. Database Migration Job (One-off execution)
resource "azurerm_container_app_job" "migrate" {
  name                         = "${var.prefix}-migrate-job"
  location                     = var.location
  resource_group_name          = var.resource_group_name
  container_app_environment_id = azurerm_container_app_environment.env.id

  replica_timeout_in_seconds = 300
  replica_retry_limit        = 1

  manual_trigger_config {}

  identity {
    type = "SystemAssigned"
  }

  registry {
    server               = azurerm_container_registry.acr.login_server
    username             = azurerm_container_registry.acr.admin_username
    password_secret_name = "acr-password"
  }

  secret {
    name  = "acr-password"
    value = azurerm_container_registry.acr.admin_password
  }

  template {
    container {
      image  = var.container_image_api
      name   = "migrate"
      cpu    = 0.5
      memory = "1Gi"

      # Override the command to run migrations
      command = ["alembic", "upgrade", "head"]

      env {
        name  = "ENVIRONMENT"
        value = "production"
      }

      env {
        name  = "DATABASE_URL"
        value = var.database_url
      }

      env {
        name  = "JWT_SECRET"
        value = var.jwt_secret
      }
    }
  }
}

output "migrate_job_name" {
  value = azurerm_container_app_job.migrate.name
}

# 7. Role Assignments for ACR Pull (Commented out: requires 'User Access Administrator' or 'Owner' permissions)
# resource "azurerm_role_assignment" "acr_pull_api" {
#   scope                = azurerm_container_registry.acr.id
#   role_definition_name = "AcrPull"
#   principal_id         = azurerm_container_app.api.identity[0].principal_id
# }

# resource "azurerm_role_assignment" "acr_pull_worker" {
#   scope                = azurerm_container_registry.acr.id
#   role_definition_name = "AcrPull"
#   principal_id         = azurerm_container_app.worker.identity[0].principal_id
# }

# resource "azurerm_role_assignment" "acr_pull_migrate" {
#   scope                = azurerm_container_registry.acr.id
#   role_definition_name = "AcrPull"
#   principal_id         = azurerm_container_app_job.migrate.identity[0].principal_id
# }

