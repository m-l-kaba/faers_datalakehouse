<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_azurerm"></a> [azurerm](#requirement\_azurerm) | ~> 4.0 |
| <a name="requirement_databricks"></a> [databricks](#requirement\_databricks) | ~> 1.90 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_azurerm"></a> [azurerm](#provider\_azurerm) | ~> 4.0 |
| <a name="provider_databricks.workspace"></a> [databricks.workspace](#provider\_databricks.workspace) | ~> 1.90 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [azurerm_databricks_access_connector.main](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_access_connector) | resource |
| [azurerm_databricks_workspace.main](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace) | resource |
| [azurerm_resource_group.main](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/resource_group) | resource |
| [azurerm_role_assignment.storage_rbac](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/role_assignment) | resource |
| [azurerm_storage_account.main](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_account) | resource |
| [azurerm_storage_container.data](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [azurerm_storage_container.tfstate](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/storage_container) | resource |
| [databricks_catalog.catalogs](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/catalog) | resource |
| [databricks_external_location.main](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/external_location) | resource |
| [databricks_grants.catalog_grants](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/grants) | resource |
| [databricks_grants.external_location_grants](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/grants) | resource |
| [databricks_schema.schemas](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/schema) | resource |
| [databricks_storage_credential.main](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/storage_credential) | resource |
| [databricks_volume.landing](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/volume) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_access_connector_name"></a> [access\_connector\_name](#input\_access\_connector\_name) | Name of the Databricks access connector | `string` | `"faers-dbx-access-connector"` | no |
| <a name="input_catalogs"></a> [catalogs](#input\_catalogs) | Map of catalogs to create | <pre>map(object({<br/>    comment = string<br/>  }))</pre> | <pre>{<br/>  "development": {<br/>    "comment": "Development catalog for FAERS data"<br/>  },<br/>  "production": {<br/>    "comment": "Production catalog for FAERS data"<br/>  }<br/>}</pre> | no |
| <a name="input_create_landing_schema"></a> [create\_landing\_schema](#input\_create\_landing\_schema) | Whether to create landing schema and volume | `bool` | `true` | no |
| <a name="input_data_container_name"></a> [data\_container\_name](#input\_data\_container\_name) | Name of the data container | `string` | `"faers-datalake"` | no |
| <a name="input_databricks_managed_rg_name"></a> [databricks\_managed\_rg\_name](#input\_databricks\_managed\_rg\_name) | Name of the Databricks managed resource group | `string` | `"faers-databricks-mrg"` | no |
| <a name="input_databricks_sku"></a> [databricks\_sku](#input\_databricks\_sku) | Databricks workspace SKU | `string` | `"premium"` | no |
| <a name="input_databricks_workspace_name"></a> [databricks\_workspace\_name](#input\_databricks\_workspace\_name) | Name of the Databricks workspace | `string` | `"faers-databricks-ws"` | no |
| <a name="input_databricks_workspace_provider_alias"></a> [databricks\_workspace\_provider\_alias](#input\_databricks\_workspace\_provider\_alias) | Alias for the Databricks workspace provider | `string` | `"ws"` | no |
| <a name="input_default_catalog_privileges"></a> [default\_catalog\_privileges](#input\_default\_catalog\_privileges) | Default privileges for catalog grants | `list(string)` | <pre>[<br/>  "ALL PRIVILEGES"<br/>]</pre> | no |
| <a name="input_default_external_location_privileges"></a> [default\_external\_location\_privileges](#input\_default\_external\_location\_privileges) | Default privileges for external location grants | `list(string)` | <pre>[<br/>  "ALL PRIVILEGES"<br/>]</pre> | no |
| <a name="input_default_principal"></a> [default\_principal](#input\_default\_principal) | Default principal for grants | `string` | `"account users"` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | Environment name (dev, prod, staging) | `string` | `"prod"` | no |
| <a name="input_external_location_name"></a> [external\_location\_name](#input\_external\_location\_name) | Name of the Databricks external location | `string` | `"faers-external-location"` | no |
| <a name="input_landing_schema_catalog"></a> [landing\_schema\_catalog](#input\_landing\_schema\_catalog) | Catalog name for landing schema | `string` | `"production"` | no |
| <a name="input_location"></a> [location](#input\_location) | Azure region for resources | `string` | `"Germany West Central"` | no |
| <a name="input_project_name"></a> [project\_name](#input\_project\_name) | Project name for resource naming | `string` | `"faers"` | no |
| <a name="input_resource_group_name"></a> [resource\_group\_name](#input\_resource\_group\_name) | Name of the Azure resource group | `string` | `"faers-lakehouse-rg"` | no |
| <a name="input_schemas"></a> [schemas](#input\_schemas) | List of schemas to create for each catalog | `list(string)` | <pre>[<br/>  "bronze",<br/>  "silver",<br/>  "gold"<br/>]</pre> | no |
| <a name="input_storage_account_name"></a> [storage\_account\_name](#input\_storage\_account\_name) | Name of the Azure storage account | `string` | `"faerslakehouse"` | no |
| <a name="input_storage_account_tier"></a> [storage\_account\_tier](#input\_storage\_account\_tier) | Storage account tier | `string` | `"Standard"` | no |
| <a name="input_storage_credential_name"></a> [storage\_credential\_name](#input\_storage\_credential\_name) | Name of the Databricks storage credential | `string` | `"faers-sc"` | no |
| <a name="input_storage_replication_type"></a> [storage\_replication\_type](#input\_storage\_replication\_type) | Storage account replication type | `string` | `"LRS"` | no |
| <a name="input_tfstate_container_name"></a> [tfstate\_container\_name](#input\_tfstate\_container\_name) | Name of the Terraform state container | `string` | `"tfstate"` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_access_connector_id"></a> [access\_connector\_id](#output\_access\_connector\_id) | ID of the Databricks access connector |
| <a name="output_access_connector_principal_id"></a> [access\_connector\_principal\_id](#output\_access\_connector\_principal\_id) | Principal ID of the access connector's managed identity |
| <a name="output_catalog_ids"></a> [catalog\_ids](#output\_catalog\_ids) | Map of catalog names to their IDs |
| <a name="output_catalog_names"></a> [catalog\_names](#output\_catalog\_names) | Names of created catalogs |
| <a name="output_container_base_url"></a> [container\_base\_url](#output\_container\_base\_url) | Base URL for the data container |
| <a name="output_data_container_name"></a> [data\_container\_name](#output\_data\_container\_name) | Name of the data container |
| <a name="output_databricks_workspace_id"></a> [databricks\_workspace\_id](#output\_databricks\_workspace\_id) | ID of the Databricks workspace |
| <a name="output_databricks_workspace_name"></a> [databricks\_workspace\_name](#output\_databricks\_workspace\_name) | Name of the Databricks workspace |
| <a name="output_databricks_workspace_url"></a> [databricks\_workspace\_url](#output\_databricks\_workspace\_url) | URL of the Databricks workspace |
| <a name="output_external_location_name"></a> [external\_location\_name](#output\_external\_location\_name) | Name of the Databricks external location |
| <a name="output_external_location_url"></a> [external\_location\_url](#output\_external\_location\_url) | URL of the external location |
| <a name="output_landing_volume_name"></a> [landing\_volume\_name](#output\_landing\_volume\_name) | Name of the landing volume (if created) |
| <a name="output_landing_volume_url"></a> [landing\_volume\_url](#output\_landing\_volume\_url) | URL for the landing volume |
| <a name="output_location"></a> [location](#output\_location) | Azure region where resources are deployed |
| <a name="output_resource_group_id"></a> [resource\_group\_id](#output\_resource\_group\_id) | ID of the created resource group |
| <a name="output_resource_group_name"></a> [resource\_group\_name](#output\_resource\_group\_name) | Name of the created resource group |
| <a name="output_schema_names"></a> [schema\_names](#output\_schema\_names) | List of all created schemas with their catalog information |
| <a name="output_storage_account_id"></a> [storage\_account\_id](#output\_storage\_account\_id) | ID of the created storage account |
| <a name="output_storage_account_name"></a> [storage\_account\_name](#output\_storage\_account\_name) | Name of the created storage account |
| <a name="output_storage_account_primary_dfs_endpoint"></a> [storage\_account\_primary\_dfs\_endpoint](#output\_storage\_account\_primary\_dfs\_endpoint) | Primary DFS endpoint of the storage account |
| <a name="output_storage_credential_name"></a> [storage\_credential\_name](#output\_storage\_credential\_name) | Name of the Databricks storage credential |
| <a name="output_tfstate_container_name"></a> [tfstate\_container\_name](#output\_tfstate\_container\_name) | Name of the Terraform state container |
<!-- END_TF_DOCS -->