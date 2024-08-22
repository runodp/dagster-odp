from typing import Any, Dict, Optional

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource, GCSResource

from .. import utils
from ..resource_registry import resource_registry, vayu_resource
from .dlt_resource import VayuDltResource
from .soda_resource import SodaResource


@vayu_resource("bigquery")
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


@vayu_resource("dbt")
class VayuDbtResource(DbtCliResource):
    """
    Custom DBT resource that extends DbtCliResource.

    This resource provides additional functionality to update configuration parameters
    based on the execution context.
    """

    sources_file_path: Optional[str] = None

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


resource_registry["gcs"] = GCSResource

__all__ = ["VayuDltResource", "SodaResource", "VayuBigQueryResource", "VayuDbtResource"]
