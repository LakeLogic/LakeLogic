output "vm_id" {
  description = "Virtual machine resource ID"
  value       = azurerm_windows_virtual_machine.jumpbox.id
}

output "vm_name" {
  description = "Virtual machine name"
  value       = azurerm_windows_virtual_machine.jumpbox.name
}

output "private_ip" {
  description = "Private IP address of the jump server"
  value       = azurerm_network_interface.jumpbox.private_ip_address
}

output "admin_username" {
  description = "Admin username for RDP"
  value       = var.admin_username
}

output "connection_instructions" {
  description = "How to connect via Azure Bastion"
  value       = <<-EOT
    Connect via Azure Bastion:
    1. Go to Azure Portal
    2. Navigate to: ${azurerm_windows_virtual_machine.jumpbox.name}
    3. Click "Connect" → "Bastion"
    4. Enter credentials:
       Username: ${var.admin_username}
       Password: <your-password>
    5. Click "Connect"
    
    Database tools are pre-installed and ready to use!
  EOT
}
