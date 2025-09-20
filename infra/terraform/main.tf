resource "azurerm_resource_group" "faers_lakehouse_rg" {
  name     = "faers-lakehouse-rg"
  location = "Germany West Central"
}

resource "azurerm_storage_account" "faerslakehouse" {
  name                     = "faerslakehouse"
  resource_group_name      = azurerm_resource_group.faers_lakehouse_rg.name
  location                 = azurerm_resource_group.faers_lakehouse_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}