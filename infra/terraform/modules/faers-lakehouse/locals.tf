# ==============================================================================
# LOCAL VALUES AND COMPUTED DATA
# ==============================================================================

locals {
  # Flatten catalogs and schemas into a single map for easier iteration
  schemas = flatten([
    for catalog_key, catalog_value in var.catalogs : [
      for schema in var.schemas : {
        key          = "${catalog_key}-${schema}"
        catalog_name = catalog_key
        schema_name  = schema
        comment      = "${title(schema)} schema for ${catalog_value.comment}"
      }
    ]
  ])
  
  # Convert flattened schemas to a map for easier reference
  schemas_map = { for schema in local.schemas : schema.key => schema }
  
  # Landing schema configuration
  landing_schemas = var.create_landing_schema ? [{
    key          = "${var.landing_schema_catalog}-landing"
    catalog_name = var.landing_schema_catalog
    schema_name  = "landing"
    comment      = "Landing schema for initial data ingestion"
  }] : []
  
  # All schemas including landing
  all_schemas = concat(local.schemas, local.landing_schemas)
  all_schemas_map = { for schema in local.all_schemas : schema.key => schema }
  
  # Storage URLs
  storage_account_dfs_endpoint = "${var.storage_account_name}.dfs.core.windows.net"
  container_base_url = "abfss://${var.data_container_name}@${local.storage_account_dfs_endpoint}"
  landing_volume_url = "${local.container_base_url}/landing/"
  
  # Common tags
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}