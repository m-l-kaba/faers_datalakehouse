variable "azure_client_secret" {
  description = "The Client Secret for the Azure Service Principal"
  type        = string
  sensitive   = true
}

variable "azure_client_id" {
  description = "The Client ID for the Azure Service Principal"
  type        = string
}

variable "azure_tenant_id" {
  description = "The Tenant ID for the Azure Service Principal"
  type        = string
}

variable "metastore_id" {
  description = "The Unity Catalog Metastore ID"
  type        = string
}

variable "account_id" {
  description = "The Databricks Account ID"
  type        = string
}