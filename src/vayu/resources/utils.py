from typing import Dict

from dagster import AssetExecutionContext

from ..config_manager.builders.config_builder import ConfigBuilder
from ..utils import ConfigParamReplacer


def update_config_params(context: AssetExecutionContext, config: Dict) -> Dict:
    """
    Updates the configuration parameters based on the execution context.

    This function is used to dynamically modify the configuration based on
    the current asset execution context. It can be used to inject runtime
    information into the configuration.

    Args:
        context (AssetExecutionContext): The current asset execution context.
        config (Dict): The original configuration dictionary.

    Returns:
        Dict
    """
    dagster_config = ConfigBuilder().get_config()
    config_replacer = ConfigParamReplacer(
        context, None, dagster_config.resources.model_dump()
    )
    return config_replacer.replace(config)
