"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition, AssetSelection

# Automatically targets every single registered asset within your workflow
analytical_pipeline_schedule = ScheduleDefinition(
    name="hdb_analytics_refresh_schedule",
    target=AssetSelection.all(),
    cron_schedule="0 0 1 * *",  # Triggers automatically at midnight on the 1st day of every month
    execution_timezone="Asia/Singapore",
)