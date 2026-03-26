
from orchestration.etl_assets import hive_data_transformed
from orchestration.resources import source_db_resource, target_db_resource
from dagster import Definitions, define_asset_job, ScheduleDefinition


etl_job = define_asset_job(
    name="etl_hive_job", 
    selection=[hive_data_transformed]
)

etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 0 * * *", 
)

defs = Definitions(
    assets=[hive_data_transformed],
    schedules=[etl_schedule],
    resources={
        "source_db": source_db_resource,
        "target_db": target_db_resource,
    },
)