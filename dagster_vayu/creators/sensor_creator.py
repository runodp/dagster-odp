from typing import Dict, List

from dagster import SensorDefinition
from dagster._core.definitions.sensor_definition import DefaultSensorStatus

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.config_model import SensorConfig
from dagster_vayu.config_manager.models.workflow_model import SensorTrigger


def _get_sensor_def(
    job_id: str, sensor_resource_map: Dict[str, List], spec: SensorTrigger
) -> SensorDefinition:
    sensor_kind = spec.params.sensor_kind
    required_resources = set(sensor_resource_map[sensor_kind])
    sensor_def = SensorDefinition(
        job_name=job_id,
        name=spec.trigger_id,
        description=spec.description,
        evaluation_fn=spec.params.sensor_params.run,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys=required_resources,
    )
    return sensor_def


def get_sensors(
    wb: WorkflowBuilder, sensor_config: List[SensorConfig]
) -> List[SensorDefinition]:
    """
    Builds all the sensors based on the trigger specs in the workflow config files.

    Args:
        wb (WorkflowBuilder): An instance of the WorkflowBuilder class.
        sensor_config (Dict): The sensor configuration.

    Returns:
        List[SensorDefinition]: List of sensor definitions.
    """
    sensors: List[SensorDefinition] = []
    job_id_sensors = wb.job_id_trigger_map("sensor")
    sensor_resource_map = {
        sensor.name: sensor.required_resources for sensor in sensor_config
    }

    for job_id, sensor_specs in job_id_sensors.items():
        for spec in sensor_specs:
            sensor_def = _get_sensor_def(job_id, sensor_resource_map, spec)
            sensors.append(sensor_def)

    return sensors
