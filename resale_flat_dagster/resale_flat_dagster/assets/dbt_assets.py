import os
from pathlib import Path
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

# Resolve the absolute path to your root hdb_dbt folder location
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "..", "hdb_dbt").resolve()
dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

# Lineage & Metadata Translator class
class HdbDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        resource_type = dbt_resource_props.get("resource_type")
        name = dbt_resource_props.get("name")
        
        # Intercept source requests to stitch Python assets to your SQL models
        if resource_type == "source" and name == "raw_enriched_transactions":
            return AssetKey("raw_enriched_transactions")
            
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props) -> str:
        # This handles the grouping mapping dynamically without breaking decorators
        return "data_warehouse"

@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=HdbDbtTranslator()
)
def hdb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Executes dbt transformations to rebuild fact_sales and mart tables."""
    yield from dbt.cli(["run"], context=context).stream()
