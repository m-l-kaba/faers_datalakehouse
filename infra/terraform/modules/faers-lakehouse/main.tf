# ==============================================================================
# RESOURCE GROUP
# ==============================================================================

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.common_tags
}

# ==============================================================================
# AZURE STORAGE ACCOUNT AND CONTAINERS
# ==============================================================================

resource "azurerm_storage_account" "main" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_replication_type
  is_hns_enabled           = true
  tags                     = local.common_tags
}

resource "azurerm_storage_container" "data" {
  name                  = var.data_container_name
  storage_account_id    = azurerm_storage_account.main.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "tfstate" {
  name                  = var.tfstate_container_name
  storage_account_id    = azurerm_storage_account.main.id
  container_access_type = "private"
}

# ==============================================================================
# DATABRICKS WORKSPACE AND ACCESS CONNECTOR
# ==============================================================================

resource "azurerm_databricks_workspace" "main" {
  name                        = var.databricks_workspace_name
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = var.databricks_sku
  managed_resource_group_name = var.databricks_managed_rg_name
  tags                        = local.common_tags
}

resource "azurerm_databricks_access_connector" "main" {
  name                = var.access_connector_name
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.common_tags
  
  identity {
    type = "SystemAssigned"
  }
}

# ==============================================================================
# DATABRICKS STORAGE CREDENTIAL AND RBAC
# ==============================================================================

resource "databricks_storage_credential" "main" {
  name          = var.storage_credential_name
  provider      = databricks.workspace
  force_destroy = true
  force_update  = true
  
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.main.id
  }
}

resource "azurerm_role_assignment" "storage_rbac" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}

# ==============================================================================
# DATABRICKS EXTERNAL LOCATION
# ==============================================================================

resource "databricks_external_location" "main" {
  name            = var.external_location_name
  provider        = databricks.workspace
  force_destroy   = true
  url             = local.container_base_url
  credential_name = databricks_storage_credential.main.name
  comment         = "External location for ${var.project_name} data"
}

# ==============================================================================
# DATABRICKS CATALOGS
# ==============================================================================

resource "databricks_catalog" "catalogs" {
  for_each      = var.catalogs
  provider      = databricks.workspace
  name          = each.key
  comment       = each.value.comment
  storage_root  = local.container_base_url
  depends_on    = [databricks_external_location.main]
  force_destroy = true
}

# ==============================================================================
# DATABRICKS SCHEMAS
# ==============================================================================

resource "databricks_schema" "schemas" {
  for_each     = local.all_schemas_map
  provider     = databricks.workspace
  name         = each.value.schema_name
  catalog_name = each.value.catalog_name
  comment      = each.value.comment
  depends_on   = [databricks_catalog.catalogs]
}

# ==============================================================================
# DATABRICKS LANDING VOLUME
# ==============================================================================

resource "databricks_volume" "landing" {
  count            = var.create_landing_schema ? 1 : 0
  provider         = databricks.workspace
  name             = "landing_volume"
  volume_type      = "EXTERNAL"
  schema_name      = "landing"
  catalog_name     = var.landing_schema_catalog
  storage_location = local.landing_volume_url
  comment          = "Volume for landing schema"
  depends_on       = [databricks_schema.schemas]
}

# ==============================================================================
# DATABRICKS GRANTS
# ==============================================================================

resource "databricks_grants" "catalog_grants" {
  for_each = var.catalogs
  catalog  = databricks_catalog.catalogs[each.key].name
  provider = databricks.workspace
  
  grant {
    principal  = var.default_principal
    privileges = var.default_catalog_privileges
  }
}

resource "databricks_grants" "external_location_grants" {
  external_location = databricks_external_location.main.name
  provider          = databricks.workspace
  
  grant {
    principal  = var.default_principal
    privileges = var.default_external_location_privileges
  }
}