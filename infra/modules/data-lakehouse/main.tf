# Creates the ADLS Gen2 Storage Account and the necessary containers (Bronze/Silver/Gold/Reference).

# Variables: resource_group_name, prefix, location

resource "azurerm_storage_account" "adls" {
  name                     = "adls${replace(var.prefix, "-", "")}ai" # Must be globally unique, alphanumeric, and lowercase
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # Use GRS for Production/Prod tiering
  is_hns_enabled           = true  # Enables Hierarchical Namespace (ADLS Gen2)
}

# Containers for the Medallion Architecture
resource "azurerm_storage_container" "containers" {
  for_each              = toset(["bronze", "silver", "gold", "reference"])
  name                  = each.key
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# Folder structure in Bronze container
# ADLS Gen2 with hierarchical namespace enabled supports real directories
resource "azurerm_storage_data_lake_gen2_path" "bronze_finance_domain" {
  path               = "domain_finance"
  filesystem_name    = azurerm_storage_container.containers["bronze"].name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"

  depends_on = [azurerm_storage_container.containers]
}

resource "azurerm_storage_data_lake_gen2_path" "bronze_finance_sap_system" {
  path               = "domain_finance/system_sap"
  filesystem_name    = azurerm_storage_container.containers["bronze"].name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"

  depends_on = [azurerm_storage_data_lake_gen2_path.bronze_finance_domain]
}