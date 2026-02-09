output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "location" {
  value = azurerm_resource_group.rg.location
}

output "vnet_id" {
  value = azurerm_virtual_network.vnet.id
}

output "aca_subnet_id" {
  value = azurerm_subnet.aca.id
}

output "db_subnet_id" {
  value = azurerm_subnet.db.id
}

output "private_subnet_id" {
  value = azurerm_subnet.private.id
}

output "postgres_dns_zone_id" {
  value = azurerm_private_dns_zone.postgres.id
}

output "postgres_dns_zone_name" {
  value = azurerm_private_dns_zone.postgres.name
}
