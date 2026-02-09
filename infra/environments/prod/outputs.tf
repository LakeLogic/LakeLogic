output "api_endpoint" {
  description = "The public URL of the Trading API."
  value       = module.compute.api_url
}

output "acr_login_server" {
  description = "The login server for the Azure Container Registry."
  value       = module.compute.acr_login_server
}

output "acr_name" {
  description = "The name of the Azure Container Registry."
  value       = module.compute.acr_name
}

output "db_hostname" {
  description = "The internal hostname of the Postgres server."
  value       = module.db.db_hostname
}

output "storage_account_name" {
  description = "The name of the ADLS Gen2 storage account."
  value       = module.lakehouse.storage_account_name
}

output "frontend_url" {
  value = module.frontend.static_webapp_url
}

output "frontend_deployment_token" {
  value     = module.frontend.api_key
  sensitive = true
}

output "migrate_job_name" {
  value = module.compute.migrate_job_name
}
