# Provisions the Azure PostgreSQL Flexible Server with Private Networking

resource "azurerm_postgresql_flexible_server" "db" {
  name                   = "${var.prefix}-postgres"
  resource_group_name    = var.resource_group_name
  location               = var.location
  version                = "16"
  delegated_subnet_id    = var.subnet_id
  private_dns_zone_id    = var.private_dns_zone_id
  administrator_login    = var.db_admin_login
  administrator_password = var.db_admin_password
  sku_name               = var.sku_name
  storage_mb             = 32768
  backup_retention_days  = 7
  zone                   = "1"
  public_network_access_enabled = false

  lifecycle {
    ignore_changes = [zone, high_availability]
  }
}

resource "azurerm_postgresql_flexible_server_database" "lineagelogic" {
  name      = "lineagelogic"
  server_id = azurerm_postgresql_flexible_server.db.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# Optional: Disable SSL if needed for specific legacy reasons, 
# but for production it's highly recommended to keep it on.
resource "azurerm_postgresql_flexible_server_configuration" "require_secure_transport" {
  name      = "require_secure_transport"
  server_id = azurerm_postgresql_flexible_server.db.id
  value     = "on"
}