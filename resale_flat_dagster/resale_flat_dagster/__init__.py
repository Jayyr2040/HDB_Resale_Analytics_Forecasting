# resale_flat_dagster/resale_flat_dagster/__init__.py

import os
import importlib
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from .schedules import analytical_pipeline_schedule
from .project import hdb_dbt_project

# 1. FORCE THE ENGINE TO FETCH PURE FILE MODULES DIRECTLY FROM DISK
# (This completely bypasses Python's internal namespace attribute shadowing)
raw_data_module = importlib.import_module("resale_flat_dagster.assets.raw_data")
dbt_module_file = importlib.import_module("resale_flat_dagster.assets.dbt_assets")
ml_models_module = importlib.import_module("resale_flat_dagster.assets.ml_models")

# 2. UNPACK ALL ASSET NODE EXPERIMENTS CLEANLY
all_assets = [
    *load_assets_from_modules([raw_data_module]),
    *load_assets_from_modules([dbt_module_file]), # <-- Restores your dbt analytics marts perfectly!
    *load_assets_from_modules([ml_models_module])  # <-- Injects your new ML time-series engine node
]

# 3. UNIFIED PRODUCTION DEFINITIONS ENVIRONMENT
defs = Definitions(
    assets=all_assets,
    resources={
        # Natively configures the CLI runner tool using your project's absolute directory path context
        "dbt": DbtCliResource(project_dir=hdb_dbt_project.project_dir),
    },
    schedules=[analytical_pipeline_schedule]
)
