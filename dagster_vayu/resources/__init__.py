from typing import Any, Dict, Optional

from dagster import AssetExecutionContext, ConfigurableResource
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource

from . import utils
from .dlt_resource import VayuDltResource


class SensorContextConfig(ConfigurableResource):  # type: ignore[misc]
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


class VayuBigQueryResource(BigQueryResource):
    """
    Custom BigQuery resource that returns the client in the ExecutionContext.
    """

    def get_object_to_set_on_execution_context(self) -> Any:
        """
        Returns the BigQuery client.

        Returns:
            bigquery.Client: The BigQuery client.
        """
        return self.get_client()


class VayuDbtResource(DbtCliResource):  # type: ignore[misc]
    """
    Custom DBT resource that extends DbtCliResource.

    This resource provides additional functionality to update configuration parameters
    based on the execution context.
    """

    def update_config_params(
        self, context: AssetExecutionContext, config: Dict
    ) -> Dict:
        """
        Updates the configuration parameters based on the execution context.

        Args:
            context (AssetExecutionContext): The current asset execution context.
            config (Dict): The original configuration dictionary.

        Returns:
            Dict
        """
        return utils.update_config_params(context, config)


__all__ = [
    "VayuDltResource",
]
