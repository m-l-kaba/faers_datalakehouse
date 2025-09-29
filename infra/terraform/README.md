<!-- BEGIN_TF_DOCS -->
## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | 4.45.1 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_faers_lakehouse"></a> [faers\_lakehouse](#module\_faers\_lakehouse) | ./modules/faers-lakehouse | n/a |

## Resources

| Name | Type |
|------|------|
| [azurerm_databricks_workspace.faers_databricks_ws](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/data-sources/databricks_workspace) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_account_id"></a> [account\_id](#input\_account\_id) | The Databricks Account ID | `string` | n/a | yes |
| <a name="input_azure_client_id"></a> [azure\_client\_id](#input\_azure\_client\_id) | The Client ID for the Azure Service Principal | `string` | n/a | yes |
| <a name="input_azure_client_secret"></a> [azure\_client\_secret](#input\_azure\_client\_secret) | The Client Secret for the Azure Service Principal | `string` | n/a | yes |
| <a name="input_azure_tenant_id"></a> [azure\_tenant\_id](#input\_azure\_tenant\_id) | The Tenant ID for the Azure Service Principal | `string` | n/a | yes |
| <a name="input_metastore_id"></a> [metastore\_id](#input\_metastore\_id) | The Unity Catalog Metastore ID | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_catalog_names"></a> [catalog\_names](#output\_catalog\_names) | Names of created catalogs |
| <a name="output_databricks_workspace_url"></a> [databricks\_workspace\_url](#output\_databricks\_workspace\_url) | URL of the Databricks workspace |
| <a name="output_external_location_url"></a> [external\_location\_url](#output\_external\_location\_url) | URL of the external location |
| <a name="output_resource_group_name"></a> [resource\_group\_name](#output\_resource\_group\_name) | Name of the created resource group |
| <a name="output_schema_names"></a> [schema\_names](#output\_schema\_names) | List of all created schemas with their catalog information |
| <a name="output_storage_account_name"></a> [storage\_account\_name](#output\_storage\_account\_name) | Name of the created storage account |
<!-- END_TF_DOCS -->