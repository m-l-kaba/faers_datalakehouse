# ==============================================================================
# FAERS DATA LAKEHOUSE MODULE
# ==============================================================================

module "faers_lakehouse" {
  source = "./modules/faers-lakehouse"
  
  # Basic Configuration
  resource_group_name = "faers-lakehouse-rg"
  location           = "Germany West Central"
  environment        = "prod"
  project_name       = "faers"
  
  # Storage Configuration
  storage_account_name      = "faerslakehouse"
  storage_account_tier      = "Standard"
  storage_replication_type  = "LRS"
  data_container_name       = "faers-datalake"
  tfstate_container_name    = "tfstate"
  
  # Databricks Configuration
  databricks_workspace_name    = "faers-databricks-ws"
  databricks_sku              = "premium"
  databricks_managed_rg_name  = "faers-databricks-mrg"
  access_connector_name       = "faers-dbx-access-connector"
  storage_credential_name     = "faers-sc"
  external_location_name      = "faers-external-location"
  
  # Catalog and Schema Configuration
  catalogs = {
    production = {
      comment = "Production catalog for FAERS data"
    }
    development = {
      comment = "Development catalog for FAERS data"
    }
  }
  
  schemas                   = ["bronze", "silver", "gold"]
  create_landing_schema     = true
  landing_schema_catalog    = "production"
  
  # Permissions Configuration
  default_principal                    = "account users"
  default_catalog_privileges           = ["ALL PRIVILEGES"]
  default_external_location_privileges = ["ALL PRIVILEGES"]
  
  # Provider Configuration
  databricks_workspace_provider_alias = "ws"
  
  providers = {
    databricks.workspace = databricks.ws
  }
}






