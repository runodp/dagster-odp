from typing import Any, Dict, Optional

from dagster import AssetExecutionContext, ConfigurableResource
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource, GCSResource

from ..config_manager.builders.config_builder import ConfigBuilder
from .dlt_resource import VayuDltResource
from .utils import update_config_params


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
        return update_config_params(context, config)


RESOURCE_CLASS_MAP = {
    "dbt": VayuDbtResource,
    "bigquery": VayuBigQueryResource,
    "gcs": GCSResource,
    "dlt": VayuDltResource,
}


def _filter_resource_params(params: Dict[str, Any]) -> Any:
    return {k: v for k, v in params.items() if not k.endswith("_")}


def get_dagster_resources() -> Dict[str, Any]:
    """
    Create and return a dictionary of dagster resources based on the resource
    configuration. Only the parameters that do not end with an underscore ("_")
    are passed to the resource classes.
    (Parameters that end with an _ are to be used in the dagster project itself.)

    Returns:
        Dict: A dictionary containing the created resources.
    """
    resources: Dict[str, Any] = {}
    dagster_resource_config = ConfigBuilder().get_config()
    for resource_name, params in dagster_resource_config.resources.model_dump().items():
        if (not params) or (resource_name not in RESOURCE_CLASS_MAP):
            continue
        resource_class = RESOURCE_CLASS_MAP[resource_name]
        resources[resource_name] = resource_class(**_filter_resource_params(params))
    return resources
