from typing import Any, Dict, List

from dagster import AssetsDefinition

from .dbt_asset_creator import DBTAssetCreator
from .dlt_asset_creator import DLTAssetCreator
from .generic_asset_creator import GenericAssetCreator


def get_assets(dagster_resources: Dict[str, Any]) -> List[AssetsDefinition]:
    """
    Retrieves all asset definitions based on the provided dagster resources.

    This function creates DBT, DLT, and generic assets as applicable. DBT and DLT
    assets are only created if their respective resources are present.

    Args:
        dagster_resources (Dict[str, Any]): A dictionary of dagster resources.

    Returns:
        List[AssetsDefinition]: A list of all asset definitions.
    """
    asset_list: List = []

    if "dbt" in dagster_resources:
        dbt_asset_manager = DBTAssetCreator()
        asset_list.extend(dbt_asset_manager.get_assets())

    if "dlt" in dagster_resources:
        dlt_asset_manager = DLTAssetCreator()
        asset_list.extend(dlt_asset_manager.get_assets())

    asset_manager = GenericAssetCreator()

    return asset_list + asset_manager.get_assets()
