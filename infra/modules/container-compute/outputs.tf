output "acr_login_server" {
  value = azurerm_container_registry.acr.login_server
}

output "acr_name" {
  value = azurerm_container_registry.acr.name
}

output "api_url" {
  value = azurerm_container_app.api.ingress[0].fqdn
}

output "environment_id" {
  value = azurerm_container_app_environment.env.id
}
