output "api_endpoint" {
  description = "The public URL of the Dev Trading API."
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

# Windows Jump Server Outputs
output "jumpbox_name" {
  value       = module.windows_jumpbox.vm_name
  description = "Name of the Windows jump server"
}

output "jumpbox_private_ip" {
  value       = module.windows_jumpbox.private_ip
  description = "Private IP address of the Windows jump server"
}

output "jumpbox_connection_instructions" {
  value       = module.windows_jumpbox.connection_instructions
  description = "How to connect via Azure Bastion"
}
