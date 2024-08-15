from typing import Any, Dict

from dagster_gcp import GCSResource

from ..resources import VayuBigQueryResource, VayuDbtResource, VayuDltResource

RESOURCE_CLASS_MAP = {
    "dbt": VayuDbtResource,
    "bigquery": VayuBigQueryResource,
    "gcs": GCSResource,
    "dlt": VayuDltResource,
}


def _filter_resource_params(params: Dict[str, Any]) -> Any:
    return {k: v for k, v in params.items() if not k.endswith("_")}


def get_dagster_resources(dagster_resource_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create and return a dictionary of dagster resources based on the resource
    configuration.

    Args:
        dagster_resource_config (Dict[str, Any]): The resource config dict.

    Returns:
        Dict[str, Any]: A dictionary containing the created resources.

    Note:
        Only parameters that do not end with an underscore ("_") are passed to the
        resource classes. Parameters ending with "_" are reserved for use in the
        vayu project itself.
    """
    resources: Dict[str, Any] = {}
    for resource_name, params in dagster_resource_config.items():
        if (not params) or (resource_name not in RESOURCE_CLASS_MAP):
            continue
        resource_class = RESOURCE_CLASS_MAP[resource_name]
        resources[resource_name] = resource_class(**_filter_resource_params(params))
    return resources
