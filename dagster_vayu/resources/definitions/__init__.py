from typing import Any, Dict, Optional

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource
from dagster_gcp import BigQueryResource, GCSResource

from ..resource_registry import resource_registry, vayu_resource
from . import utils
from .dlt_resource import VayuDltResource
from .duckdb_resource import DuckDbResource
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
    load_all_models: bool = True

    def update_asset_params(
        self,
        context: AssetExecutionContext,
        resource_config: Dict[str, Dict],
        asset_params: Dict,
    ) -> Dict:
        """
        Updates the configuration parameters based on the execution context.

        Args:
            context (AssetExecutionContext): The current asset execution context.
            resource_config (Dict): The resource configuration.
            config (Dict): The original configuration dictionary.

        Returns:
            Dict
        """
        return utils.update_asset_params(context, resource_config, asset_params)


resource_registry["gcs"] = GCSResource

__all__ = [
    "VayuDltResource",
    "SodaResource",
    "VayuBigQueryResource",
    "VayuDbtResource",
    "DuckDbResource",
]
