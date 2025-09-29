# ==============================================================================
# RESOURCE GROUP OUTPUTS
# ==============================================================================

output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.main.name
}

output "resource_group_id" {
  description = "ID of the created resource group"
  value       = azurerm_resource_group.main.id
}

output "location" {
  description = "Azure region where resources are deployed"
  value       = azurerm_resource_group.main.location
}

# ==============================================================================
# STORAGE OUTPUTS
# ==============================================================================

output "storage_account_name" {
  description = "Name of the created storage account"
  value       = azurerm_storage_account.main.name
}

output "storage_account_id" {
  description = "ID of the created storage account"
  value       = azurerm_storage_account.main.id
}

output "storage_account_primary_dfs_endpoint" {
  description = "Primary DFS endpoint of the storage account"
  value       = azurerm_storage_account.main.primary_dfs_endpoint
}

output "data_container_name" {
  description = "Name of the data container"
  value       = azurerm_storage_container.data.name
}

output "tfstate_container_name" {
  description = "Name of the Terraform state container"
  value       = azurerm_storage_container.tfstate.name
}

# ==============================================================================
# DATABRICKS OUTPUTS
# ==============================================================================

output "databricks_workspace_id" {
  description = "ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.id
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.workspace_url
}

output "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.name
}

output "access_connector_id" {
  description = "ID of the Databricks access connector"
  value       = azurerm_databricks_access_connector.main.id
}

output "access_connector_principal_id" {
  description = "Principal ID of the access connector's managed identity"
  value       = azurerm_databricks_access_connector.main.identity[0].principal_id
}

# ==============================================================================
# DATABRICKS UNITY CATALOG OUTPUTS
# ==============================================================================

output "storage_credential_name" {
  description = "Name of the Databricks storage credential"
  value       = databricks_storage_credential.main.name
}

output "external_location_name" {
  description = "Name of the Databricks external location"
  value       = databricks_external_location.main.name
}

output "external_location_url" {
  description = "URL of the external location"
  value       = databricks_external_location.main.url
}

output "catalog_names" {
  description = "Names of created catalogs"
  value       = keys(databricks_catalog.catalogs)
}

output "catalog_ids" {
  description = "Map of catalog names to their IDs"
  value       = { for k, v in databricks_catalog.catalogs : k => v.id }
}

output "schema_names" {
  description = "List of all created schemas with their catalog information"
  value = [
    for k, v in databricks_schema.schemas : {
      name         = v.name
      catalog_name = v.catalog_name
      full_name    = "${v.catalog_name}.${v.name}"
    }
  ]
}

# ==============================================================================
# COMPUTED VALUES
# ==============================================================================

output "container_base_url" {
  description = "Base URL for the data container"
  value       = local.container_base_url
}

output "landing_volume_url" {
  description = "URL for the landing volume"
  value       = local.landing_volume_url
}

output "landing_volume_name" {
  description = "Name of the landing volume (if created)"
  value       = var.create_landing_schema ? databricks_volume.landing[0].name : null
}