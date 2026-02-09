output "storage_account_name" {
  description = "The name of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.adls.name
}

output "storage_account_id" {
  description = "The ID of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.adls.id
}

output "storage_account_primary_key" {
  description = "The primary access key for the storage account."
  value       = azurerm_storage_account.adls.primary_access_key
  sensitive   = true
}
