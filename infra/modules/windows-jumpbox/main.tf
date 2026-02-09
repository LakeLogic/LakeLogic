/**
 * Windows Jump Server Module with Azure Bastion
 * 
 * Creates a Windows Server VM with GUI database tools and Azure Bastion for secure RDP access.
 * Access via Azure Portal -> Bastion -> Connect (no VPN or public IP needed)
 */

# Network Security Group for Windows Jumpbox
resource "azurerm_network_security_group" "jumpbox" {
  name                = "${var.prefix}-windows-jumpbox-nsg"
  location            = var.location
  resource_group_name = var.resource_group_name

  # Allow RDP from specific Whitelisted IP
  security_rule {
    name                       = "AllowWhitelistedRDPInbound"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "3389"
    source_address_prefix      = var.allowed_rdp_ip
    destination_address_prefix = "*"
  }

  # Allow all outbound for tools installation and DB access
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

# Public IP for Jumpbox (Direct Access)
resource "azurerm_public_ip" "jumpbox" {
  count               = var.enable_public_ip ? 1 : 0
  name                = "${var.prefix}-win-jumpbox-pip"
  location            = var.location
  resource_group_name = var.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = var.tags
}

# Network Interface
resource "azurerm_network_interface" "jumpbox" {
  name                = "${var.prefix}-windows-jumpbox-nic"
  location            = var.location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = var.subnet_id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = var.enable_public_ip ? azurerm_public_ip.jumpbox[0].id : null
  }

  tags = var.tags
}

resource "azurerm_network_interface_security_group_association" "jumpbox" {
  network_interface_id      = azurerm_network_interface.jumpbox.id
  network_security_group_id = azurerm_network_security_group.jumpbox.id
}

# Windows Server 2022 VM
resource "azurerm_windows_virtual_machine" "jumpbox" {
  name                = "${var.prefix}-win-jumpbox"
  computer_name       = "ll-jumpbox"  # Max 15 chars for Windows
  location            = var.location
  resource_group_name = var.resource_group_name
  size                = var.vm_size
  admin_username      = var.admin_username
  admin_password      = var.admin_password

  network_interface_ids = [
    azurerm_network_interface.jumpbox.id,
  ]

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = 128  # Larger for Windows + GUI tools
  }

  source_image_reference {
    publisher = "MicrosoftWindowsServer"
    offer     = "WindowsServer"
    sku       = "2022-datacenter-azure-edition"
    version   = "latest"
  }

  tags = merge(var.tags, {
    Purpose = "Database Admin / Windows Jump Server"
    OS      = "Windows Server 2022"
  })
}

# Note: Auto-stop removed - doesn't work in GitHub Actions (Linux environment)
# Manually stop the VM after creation using: scripts\stop_jumpbox.bat
# Or via Azure Portal: Virtual Machines → Stop

# Install database tools via Custom Script Extension
resource "azurerm_virtual_machine_extension" "install_tools" {
  name                 = "InstallDatabaseTools"
  virtual_machine_id   = azurerm_windows_virtual_machine.jumpbox.id
  publisher            = "Microsoft.Compute"
  type                 = "CustomScriptExtension"
  type_handler_version = "1.10"

  settings = jsonencode({
    commandToExecute = <<-EOT
      powershell -ExecutionPolicy Unrestricted -Command "
        # Set error action
        $ErrorActionPreference = 'Stop'
        
        # Create temp directory
        $tempDir = 'C:\Temp\DBTools'
        New-Item -ItemType Directory -Force -Path $tempDir
        Set-Location $tempDir
        
        # Install Chocolatey (package manager)
        Set-ExecutionPolicy Bypass -Scope Process -Force
        [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
        iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
        
        # Refresh environment
        refreshenv
        
        # Install database tools via Chocolatey
        choco install -y pgadmin4
        choco install -y azure-data-studio
        choco install -y sql-server-management-studio
        choco install -y dbeaver
        choco install -y notepadplusplus
        choco install -y 7zip
        choco install -y googlechrome
        
        # Create desktop shortcuts info file
        $infoFile = 'C:\Users\Public\Desktop\Database_Connection_Info.txt'
        @'
LineageLogic Database Connection Information
==========================================

PostgreSQL Server: ${var.postgres_host}
Port: 5432
Database: lineagelogic
Username: lladmin
SSL Mode: Require

Tools Installed:
- pgAdmin 4 (PostgreSQL GUI)
- Azure Data Studio (Multi-DB tool)
- SQL Server Management Studio (SSMS)
- DBeaver (Universal DB tool)

All tools are in the Start Menu.
'@ | Out-File -FilePath $infoFile -Encoding UTF8
        
        # Log completion
        'Database tools installation completed!' | Out-File 'C:\Temp\jumpbox-setup.log'
      "
    EOT
  })

  tags = var.tags
}
