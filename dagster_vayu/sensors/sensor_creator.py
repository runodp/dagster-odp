from typing import Dict, List, Optional

from dagster import SensorDefinition
from dagster._core.definitions.sensor_definition import DefaultSensorStatus

from ..config_manager.builders.config_builder import ConfigBuilder
from ..config_manager.builders.workflow_builder import WorkflowBuilder
from ..config_manager.models.config_model import Sensor
from ..config_manager.models.workflow_model import SensorTrigger
from .manager.sensor_registry import sensor_registry


class SensorCreator:
    """
    Reads the sensor configurations from the workflow config files and provides
    methods to build the sensors based on the loaded configurations.

    Attributes:
        _sensors (List[SensorDefinition]): List of sensor definitions.
    """

    _instance: Optional["SensorCreator"] = None

    def __new__(cls) -> "SensorCreator":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._sensors: List[SensorDefinition] = []
        self._wb = WorkflowBuilder()
        self._dagster_config = ConfigBuilder().get_config()

    def _get_sensor_def(
        self, job_id: str, sensor_resource_map: Dict[str, Sensor], spec: SensorTrigger
    ) -> SensorDefinition:
        sensor_kind = spec.params.sensor_kind
        if sensor_kind not in sensor_registry:
            raise ValueError(
                f"Sensor '{sensor_kind}' is not defined with the decorator."
            )
        sensor_cls = sensor_registry[sensor_kind]
        sensor = sensor_cls(**spec.params.sensor_params.model_dump())
        required_resources = set(sensor_resource_map[sensor_kind].required_resources)
        sensor_def = SensorDefinition(
            job_name=job_id,
            name=spec.trigger_id,
            description=spec.description,
            evaluation_fn=sensor.run,
            default_status=DefaultSensorStatus.RUNNING,
            required_resource_keys=required_resources,
        )

        return sensor_def

    def get_sensors(self) -> List[SensorDefinition]:
        """
        Builds all the sensors based on the trigger specs in the workflow config files.

        Returns:
            List[SensorDefinition]
        """
        if not self._sensors:
            job_id_sensors = self._wb.job_id_trigger_map("sensor")
            for job_id, sensors in job_id_sensors.items():
                sensor_resource_map = {
                    sensor.name: sensor for sensor in self._dagster_config.sensors
                }
                for spec in sensors:
                    sensor_def = self._get_sensor_def(job_id, sensor_resource_map, spec)
                    self._sensors.append(sensor_def)
        return self._sensors
