# ==============================================================================
# BASIC CONFIGURATION
# ==============================================================================

variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
  default     = "faers-lakehouse-rg"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "Germany West Central"
}

variable "environment" {
  description = "Environment name (dev, prod, staging)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "faers"
}

# ==============================================================================
# STORAGE CONFIGURATION
# ==============================================================================

variable "storage_account_name" {
  description = "Name of the Azure storage account"
  type        = string
  default     = "faerslakehouse"
}

variable "storage_account_tier" {
  description = "Storage account tier"
  type        = string
  default     = "Standard"
}

variable "storage_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
}

variable "data_container_name" {
  description = "Name of the data container"
  type        = string
  default     = "faers-datalake"
}

variable "tfstate_container_name" {
  description = "Name of the Terraform state container"
  type        = string
  default     = "tfstate"
}

# ==============================================================================
# DATABRICKS CONFIGURATION
# ==============================================================================

variable "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  type        = string
  default     = "faers-databricks-ws"
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"
}

variable "databricks_managed_rg_name" {
  description = "Name of the Databricks managed resource group"
  type        = string
  default     = "faers-databricks-mrg"
}

variable "access_connector_name" {
  description = "Name of the Databricks access connector"
  type        = string
  default     = "faers-dbx-access-connector"
}

variable "storage_credential_name" {
  description = "Name of the Databricks storage credential"
  type        = string
  default     = "faers-sc"
}

variable "external_location_name" {
  description = "Name of the Databricks external location"
  type        = string
  default     = "faers-external-location"
}

# ==============================================================================
# CATALOG AND SCHEMA CONFIGURATION
# ==============================================================================

variable "catalogs" {
  description = "Map of catalogs to create"
  type = map(object({
    comment = string
  }))
  default = {
    production = {
      comment = "Production catalog for FAERS data"
    }
    development = {
      comment = "Development catalog for FAERS data"
    }
  }
}

variable "schemas" {
  description = "List of schemas to create for each catalog"
  type        = list(string)
  default     = ["bronze", "silver", "gold"]
}

variable "create_landing_schema" {
  description = "Whether to create landing schema and volume"
  type        = bool
  default     = true
}

variable "landing_schema_catalog" {
  description = "Catalog name for landing schema"
  type        = string
  default     = "production"
}

# ==============================================================================
# PERMISSIONS CONFIGURATION
# ==============================================================================

variable "default_principal" {
  description = "Default principal for grants"
  type        = string
  default     = "account users"
}

variable "default_catalog_privileges" {
  description = "Default privileges for catalog grants"
  type        = list(string)
  default     = ["ALL PRIVILEGES"]
}

variable "default_external_location_privileges" {
  description = "Default privileges for external location grants"
  type        = list(string)
  default     = ["ALL PRIVILEGES"]
}

# ==============================================================================
# DATABRICKS PROVIDER CONFIGURATION
# ==============================================================================

variable "databricks_workspace_provider_alias" {
  description = "Alias for the Databricks workspace provider"
  type        = string
  default     = "ws"
}

variable "deploy_databricks_resources" {
  description = "Whether to deploy Databricks Unity Catalog resources"
  type        = bool
  default     = true
}