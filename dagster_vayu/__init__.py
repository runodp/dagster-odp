from dagster import Definitions

from .config_manager.builders import ConfigBuilder, WorkflowBuilder
from .creators import (
    get_assets,
    get_dagster_resources,
    get_jobs,
    get_schedules,
    get_sensors,
)
from .resources import SensorContextConfig
from .sensors.definitions.gcs_sensor import *  # noqa
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

    wb = WorkflowBuilder()
    dagster_config = ConfigBuilder().get_config()
    resources = get_dagster_resources(dagster_config.resources.model_dump())
    job_defs = get_jobs(wb)
    sensor_defs = get_sensors(wb, dagster_config.sensors)
    schedule_defs = get_schedules(wb, job_defs)

    resources["sensor_context"] = SensorContextConfig.configure_at_launch()

    return Definitions(
        assets=get_assets(resources),
        resources=resources,
        jobs=job_defs,
        sensors=sensor_defs,
        schedules=schedule_defs,
    )
