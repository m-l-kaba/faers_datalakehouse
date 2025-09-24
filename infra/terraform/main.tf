# ==============================================================================
# RESOURCE GROUP
# ==============================================================================

resource "azurerm_resource_group" "faers_lakehouse_rg" {
  name     = "faers-lakehouse-rg"
  location = "Germany West Central"
}

# ==============================================================================
# AZURE STORAGE ACCOUNT AND CONTAINER
# ==============================================================================

resource "azurerm_storage_account" "faerslakehouse" {
  name                     = "faerslakehouse"
  resource_group_name      = azurerm_resource_group.faers_lakehouse_rg.name
  location                 = azurerm_resource_group.faers_lakehouse_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "faers_data" {
  name                  = "faers-datalake"
  storage_account_id    = azurerm_storage_account.faerslakehouse.id
  container_access_type = "private"
}

# ==============================================================================
# DATABRICKS WORKSPACE AND ACCESS CONNECTOR
# ==============================================================================

resource "azurerm_databricks_workspace" "fears_databricks_ws" {
  name                        = "faers-databricks-ws"
  resource_group_name         = azurerm_resource_group.faers_lakehouse_rg.name
  location                    = azurerm_resource_group.faers_lakehouse_rg.location
  sku                         = "premium"
  managed_resource_group_name = "faers-databricks-mrg"
}

resource "azurerm_databricks_access_connector" "uc" {
  name                = "faers-dbx-access-connector"
  resource_group_name = azurerm_resource_group.faers_lakehouse_rg.name
  location            = azurerm_resource_group.faers_lakehouse_rg.location
  identity {
    type = "SystemAssigned"
  }
}

# ==============================================================================
# DATABRICKS STORAGE CREDENTIAL AND RBAC
# ==============================================================================

resource "databricks_storage_credential" "faers_cred" {
  name          = "faers-sc"
  provider      = databricks.ws
  force_destroy = true
  force_update  = true
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc.id
  }
}

resource "azurerm_role_assignment" "uc_storage_rbac" {
  scope                = azurerm_storage_account.faerslakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# ==============================================================================
# DATABRICKS EXTERNAL LOCATION AND CATALOG
# ==============================================================================

resource "databricks_external_location" "faers_ext_loc" {
  name            = "faers-external-location"
  provider        = databricks.ws
  force_destroy   = true
  url             = "abfss://${azurerm_storage_container.faers_data.name}@${azurerm_storage_account.faerslakehouse.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.faers_cred.name
  comment         = "External location for FAERS data"
}

resource "databricks_catalog" "faers_prod" {
  provider      = databricks.ws
  name          = "production"
  comment       = "Production catalog for FAERS data"
  storage_root  = "abfss://${azurerm_storage_container.faers_data.name}@${azurerm_storage_account.faerslakehouse.name}.dfs.core.windows.net/"
  depends_on    = [databricks_external_location.faers_ext_loc]
  force_destroy = true

}


# ==============================================================================
# DATABRICKS SCHEMAS (BRONZE, SILVER, GOLD)
# ==============================================================================

resource "databricks_schema" "faers_landing" {
  provider     = databricks.ws
  name         = "landing"
  catalog_name = databricks_catalog.faers_prod.name
  comment      = "Landing schema for initial data ingestion"
}

resource "databricks_volume" "fears_landing_volume" {
  provider         = databricks.ws
  name             = "landing_volume"
  volume_type      = "EXTERNAL"
  schema_name      = databricks_schema.faers_landing.name
  catalog_name     = databricks_catalog.faers_prod.name
  storage_location = "abfss://${azurerm_storage_container.faers_data.name}@${azurerm_storage_account.faerslakehouse.name}.dfs.core.windows.net/landing/"
  comment          = "Volume for landing schema"
  depends_on       = [databricks_schema.faers_landing]
}

resource "databricks_schema" "faers_bronze" {
  provider     = databricks.ws
  name         = "bronze"
  catalog_name = databricks_catalog.faers_prod.name
  comment      = "Bronze schema for raw data"
}

resource "databricks_schema" "faers_silver" {
  provider     = databricks.ws
  name         = "silver"
  catalog_name = databricks_catalog.faers_prod.name
  comment      = "Silver schema for cleaned data"
}

resource "databricks_schema" "faers_gold" {
  provider     = databricks.ws
  name         = "gold"
  catalog_name = databricks_catalog.faers_prod.name
  comment      = "Gold schema for aggregated data"
}


resource "databricks_grants" "prod_catalog" {
  catalog  = databricks_catalog.faers_prod.name
  provider = databricks.ws
  grant {
    principal  = "m.leon.kaba@gmail.com"
    privileges = ["ALL PRIVILEGES"] # required to see the catalog
  }

}



resource "databricks_grants" "external_location" {
  external_location = databricks_external_location.faers_ext_loc.name
  provider          = databricks.ws
  grant {
    principal  = "m.leon.kaba@gmail.com"
    privileges = ["ALL PRIVILEGES"]
  }
}






