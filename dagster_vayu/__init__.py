from typing import List

from dagster import Definitions

from .jobs.job_creator import JobCreator
from .resources import SensorContextConfig, get_dagster_resources
from .schedules.schedule_creator import ScheduleCreator
from .sensors.definitions.gcs_sensor import *  # noqa
from .sensors.sensor_creator import SensorCreator
from .tasks.asset_creators.dbt_asset_creator import DBTAssetCreator
from .tasks.asset_creators.dlt_asset_creator import DLTAssetCreator
from .tasks.asset_creators.generic_asset_creator import GenericAssetCreator
from .tasks.definitions.gcp import *  # noqa


def build_definitions() -> Definitions:
    """
    Build and return Dagster Definitions for the project.

    This function creates resources, assets, jobs, sensors, and schedules
    based on the config files. It conditionally includes DBT and
    DLT assets if the corresponding resources are available.

    Returns:
        Definitions: A Dagster Definitions object containing all the dagster components.
    """
    resources = get_dagster_resources()

    asset_manager = GenericAssetCreator()
    job_creator = JobCreator()
    sensor_creator = SensorCreator()
    schedule_creator = ScheduleCreator()

    asset_list: List = []

    if "dbt" in resources:
        dbt_asset_manager = DBTAssetCreator()
        asset_list.extend(dbt_asset_manager.get_assets())

    if "dlt" in resources:
        dlt_asset_manager = DLTAssetCreator()
        asset_list.extend(dlt_asset_manager.get_assets())

    resources["sensor_context"] = SensorContextConfig.configure_at_launch()

    job_defs = job_creator.get_jobs()

    return Definitions(
        assets=asset_list + asset_manager.get_assets(),
        resources=resources,
        jobs=job_defs,
        sensors=sensor_creator.get_sensors(),
        schedules=schedule_creator.get_schedules(job_defs),
    )
