output "api_endpoint" {
  description = "The public URL of the Test Trading API."
  value       = module.compute.api_url
}

output "acr_login_server" {
  value = module.compute.acr_login_server
}

output "acr_name" {
  value = module.compute.acr_name
}

output "db_hostname" {
  value = module.db.db_hostname
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
