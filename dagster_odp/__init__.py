from typing import Optional

from dagster import Definitions

from .config_manager.builders import ConfigBuilder, WorkflowBuilder
from .creators import get_asset_checks, get_assets, get_jobs, get_schedules, get_sensors
from .resources import SensorContextConfig
from .resources.definitions import *  # noqa
from .sensors.definitions.gcs_sensor import *  # noqa
from .tasks.definitions import *  # noqa


def build_definitions(config_path: Optional[str] = None) -> Definitions:
    """
    Build and return Dagster Definitions for the project.

    This function creates resources, assets, jobs, sensors, and schedules
    based on the config files. It conditionally includes DBT and
    DLT assets if the corresponding resources are available.

    Args:
        config_path (Optional[str]): The path to the config files.
            If not provided, the ODP_CONFIG_PATH environment variable will be used.

    Returns:
        Definitions: A Dagster Definitions object containing all the dagster components.
    """

    wb = WorkflowBuilder(config_path=config_path)
    cb = ConfigBuilder(config_path=config_path)

    resources = cb.resource_class_map
    job_defs = get_jobs(wb)
    sensor_defs = get_sensors(wb)
    schedule_defs = get_schedules(wb, job_defs)
    asset_check_defs = get_asset_checks(wb)

    resources["sensor_context"] = SensorContextConfig.configure_at_launch()

    return Definitions(
        assets=get_assets(resources),
        resources=resources,
        jobs=job_defs,
        sensors=sensor_defs,
        schedules=schedule_defs,
        asset_checks=asset_check_defs,
    )
