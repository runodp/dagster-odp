from typing import Dict, Optional

from dagster import ConfigurableResource

from .resource_registry import odp_resource


class SensorContextConfig(ConfigurableResource):
    """
    Configuration Resource for the sensor context.

    The purpose of this resource is so the sensor context can be sent to all assets in
    the job without having to list out the asset keys in the sensor context.
    This avoids a dependency between the asset keys and the sensors.
    The sensor is instead linked directly to the job.

    Attributes:
        sensor_context_config (Optional[Dict[str, str]]): Sensor context dictionary.
    """

    sensor_context_config: Optional[Dict[str, str]] = {}


__all__ = ["odp_resource"]
