import os

from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

import dagster

# Define paths as constants
DBT_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dbt'))
DBT_PROFILES = os.path.join(DBT_PROJECT_PATH, "config")
MANIFEST_PATH = os.path.join(DBT_PROJECT_PATH, "target/manifest.json")

# Base DBT resource configuration
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
)


@dbt_assets(manifest=MANIFEST_PATH)
def open_quant_kit_dbt_assets(context: dagster.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
