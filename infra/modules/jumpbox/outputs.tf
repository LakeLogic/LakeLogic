output "public_ip" {
  description = "Public IP address of the jump server"
  value       = azurerm_public_ip.jumpbox.ip_address
}

output "ssh_command" {
  description = "SSH command to connect to the jump server"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.jumpbox.ip_address}"
}

output "vm_id" {
  description = "Virtual machine resource ID"
  value       = azurerm_linux_virtual_machine.jumpbox.id
}

output "vm_name" {
  description = "Virtual machine name"
  value       = azurerm_linux_virtual_machine.jumpbox.name
}
