resource "azurerm_static_web_app" "swa" {
  name                = "${var.prefix}-swa"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku_tier            = var.sku_tier
  sku_size            = var.sku_size
}

output "static_webapp_id" {
  value = azurerm_static_web_app.swa.id
}

output "static_webapp_url" {
  value = azurerm_static_web_app.swa.default_host_name
}

output "api_key" {
  value     = azurerm_static_web_app.swa.api_key
  sensitive = true
}
