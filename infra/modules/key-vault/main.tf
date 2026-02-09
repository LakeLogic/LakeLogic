# Creates the Key Vault with Private Endpoint for superior security.

resource "azurerm_key_vault" "kv" {
  name                       = "${var.prefix}-kv"
  location                   = var.location
  resource_group_name        = var.resource_group_name
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  # Critical: Restrict public access
  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
  }
}

# Access policy for the application's Managed Identity
resource "azurerm_key_vault_access_policy" "app_policy" {
  count        = var.principal_id_app != "00000000-0000-0000-0000-000000000000" && var.principal_id_app != "" ? 1 : 0
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = var.tenant_id
  object_id    = var.principal_id_app

  secret_permissions = ["Get", "List"]
}

# Access policy for GitHub Actions (Dynamic Secret Fetching)
resource "azurerm_key_vault_access_policy" "github_policy" {
  count        = var.principal_id_github != "" ? 1 : 0
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = var.tenant_id
  object_id    = var.principal_id_github

  secret_permissions = ["Get", "List"]
}

# Private Endpoint for Key Vault
resource "azurerm_private_endpoint" "kv" {
  name                = "${var.prefix}-kv-pe"
  location            = var.location
  resource_group_name = var.resource_group_name
  subnet_id           = var.subnet_id

  private_service_connection {
    name                           = "kv-connection"
    private_connection_resource_id = azurerm_key_vault.kv.id
    is_manual_connection           = false
    subresource_names              = ["vault"]
  }
}

# Private DNS Zone for Key Vault
resource "azurerm_private_dns_zone" "kv" {
  name                = "privatelink.vaultcore.azure.net"
  resource_group_name = var.resource_group_name
}

resource "azurerm_private_dns_zone_virtual_network_link" "kv" {
  name                  = "kv-dns-link"
  resource_group_name   = var.resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.kv.name
  virtual_network_id    = var.vnet_id
}

resource "azurerm_private_dns_a_record" "kv" {
  name                = azurerm_key_vault.kv.name
  zone_name           = azurerm_private_dns_zone.kv.name
  resource_group_name = var.resource_group_name
  ttl                 = 300
  records             = [azurerm_private_endpoint.kv.private_service_connection[0].private_ip_address]
}