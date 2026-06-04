import os
from dagster import Definitions, load_assets_from_modules
from .assets import raw_data, dbt_assets
from .schedules import analytical_pipeline_schedule

# Automatically discover and bundle all assets out of your modular files
all_assets = [
    *load_assets_from_modules([raw_data]),
    *load_assets_from_modules([dbt_assets])
]

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": dbt_assets.dbt_resource,
    },
    schedules=[analytical_pipeline_schedule]
)