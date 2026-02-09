terraform {
  backend "azurerm" {
    resource_group_name  = "lineagelogic-tfstate-rg"
    storage_account_name = "lineagelogictfstate01"
    container_name       = "tfstate"
    key                  = "prod.terraform.tfstate"
    use_oidc             = true
  }
}
