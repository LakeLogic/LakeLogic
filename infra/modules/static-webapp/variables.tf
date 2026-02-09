variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "prefix" {
  type = string
}

variable "sku_tier" {
  type    = string
  default = "Standard" # Use Standard for production features like custom domains/private endpoints
}

variable "sku_size" {
  type    = string
  default = "Standard"
}
