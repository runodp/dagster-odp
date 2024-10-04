from typing import Dict

from dagster import AssetExecutionContext

from ...utils import ConfigParamReplacer


def update_asset_params(
    context: AssetExecutionContext, resource_config: Dict[str, Dict], asset_params: Dict
) -> Dict:
    """
    Updates the configuration parameters based on the execution context.

    This function is used to dynamically modify the configuration based on
    the current asset execution context. It can be used to inject runtime
    information into the configuration.

    Args:
        context (AssetExecutionContext): The current asset execution context.
        asset_params (Dict): The asset params to be replaced.

    Returns:
        Dict
    """
    config_replacer = ConfigParamReplacer(context, None, resource_config)
    return config_replacer.replace(asset_params)
