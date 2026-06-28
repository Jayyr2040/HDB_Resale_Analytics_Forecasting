from pathlib import Path

from dagster_dbt import DbtProject

# 1. Import the asset package module 
from . import assets
from dagster import load_assets_from_modules

hdb_dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "hdb_dbt").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
hdb_dbt_project.prepare_if_dev()

# 2. Ensure load_assets_from_modules includes the package so it tracks your new script:
all_assets = load_assets_from_modules([assets])