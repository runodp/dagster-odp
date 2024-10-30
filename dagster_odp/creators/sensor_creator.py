from typing import List

from dagster import SensorDefinition
from dagster._core.definitions.sensor_definition import DefaultSensorStatus

from dagster_odp.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_odp.config_manager.models.workflow_model import SensorTrigger

from ..sensors.manager.sensor_registry import sensor_registry


def _get_sensor_def(job_id: str, spec: SensorTrigger) -> SensorDefinition:
    sensor_kind = spec.params.sensor_kind
    required_resources = sensor_registry[sensor_kind]["required_resources"]
    sensor_def = SensorDefinition(
        job_name=job_id,
        name=spec.trigger_id,
        description=spec.description,
        evaluation_fn=spec.params.sensor_params.evaluate,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys=required_resources,
    )
    return sensor_def


def get_sensors(wb: WorkflowBuilder) -> List[SensorDefinition]:
    """
    Builds all the sensors based on the trigger specs in the workflow config files.

    Args:
        wb (WorkflowBuilder): An instance of the WorkflowBuilder class.

    Returns:
        List[SensorDefinition]: List of sensor definitions.
    """
    sensors: List[SensorDefinition] = []
    job_id_sensors = wb.job_id_trigger_map("sensor")

    for job_id, sensor_specs in job_id_sensors.items():
        for spec in sensor_specs:
            sensor_def = _get_sensor_def(job_id, spec)
            sensors.append(sensor_def)

    return sensors
