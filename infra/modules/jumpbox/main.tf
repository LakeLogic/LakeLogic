/**
 * Jump Server / Bastion Host Module
 * 
 * Creates a small Linux VM for administrative tasks like database access.
 * The VM is deployed in the VNet and has access to private resources.
 */

resource "azurerm_public_ip" "jumpbox" {
  name                = "${var.prefix}-jumpbox-pip"
  location            = var.location
  resource_group_name = var.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
  
  tags = var.tags
}

resource "azurerm_network_security_group" "jumpbox" {
  name                = "${var.prefix}-jumpbox-nsg"
  location            = var.location
  resource_group_name = var.resource_group_name

  # Allow SSH from specified IPs only
  security_rule {
    name                       = "AllowSSH"
    priority                   = 1000
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefixes    = var.allowed_ssh_ips
    destination_address_prefix = "*"
  }

  # Allow all outbound (for package installation, DB access)
  security_rule {
    name                       = "AllowAllOutbound"
    priority                   = 1000
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = var.tags
}

resource "azurerm_network_interface" "jumpbox" {
  name                = "${var.prefix}-jumpbox-nic"
  location            = var.location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = var.subnet_id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.jumpbox.id
  }

  tags = var.tags
}

resource "azurerm_network_interface_security_group_association" "jumpbox" {
  network_interface_id      = azurerm_network_interface.jumpbox.id
  network_security_group_id = azurerm_network_security_group.jumpbox.id
}

resource "azurerm_linux_virtual_machine" "jumpbox" {
  name                = "${var.prefix}-jumpbox"
  location            = var.location
  resource_group_name = var.resource_group_name
  size                = var.vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.jumpbox.id,
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = 30
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  # Install PostgreSQL client tools automatically
  custom_data = base64encode(<<-EOF
    #!/bin/bash
    set -e
    
    # Update package list
    apt-get update
    
    # Install PostgreSQL client
    apt-get install -y postgresql-client-14
    
    # Install useful tools
    apt-get install -y curl wget vim git jq
    
    # Create welcome message
    cat > /etc/motd << 'WELCOME'
    
    ╔════════════════════════════════════════════════════════════╗
    ║         LineageLogic Jump Server - Database Admin           ║
    ╚════════════════════════════════════════════════════════════╝
    
    Available tools:
      - psql (PostgreSQL client)
      - curl, wget (download tools)
      - vim, nano (editors)
    
    Database connection:
      psql "host=${var.postgres_host} port=5432 dbname=lineagelogic user=lladmin sslmode=require"
    
    WELCOME
    
    echo "Jump server setup complete!" > /var/log/jumpbox-init.log
  EOF
  )

  tags = merge(var.tags, {
    Purpose = "Database Admin / Jump Server"
  })
}
