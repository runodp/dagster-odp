from abc import ABC, abstractmethod
from typing import Sequence, Union

from dagster import AssetsDefinition, AssetSpec

from ...config_manager.builders import ConfigBuilder, WorkflowBuilder


class BaseAssetCreator(ABC):
    """
    An abstract base class for asset managers.

    This class provides a common interface and some basic functionality
    for asset managers.

    Attributes:
        _wb (WorkflowBuilder): An instance of WorkflowBuilder.
        _dagster_config (DagsterConfig): The Dagster configuration.

    Methods:
        _build_asset(): Abstract method to build an asset.
        get_assets(): Abstract method to retrieve assets.
    """

    def __init__(self) -> None:
        self._wb = WorkflowBuilder()
        cb = ConfigBuilder()
        self._dagster_config = cb.get_config()
        self._resource_config_map = cb.resource_config_map
        self._resource_class_map = cb.resource_class_map

    @abstractmethod
    def get_assets(self) -> Sequence[Union[AssetsDefinition, AssetSpec]]:
        """
        Retrieves or builds a list of asset definitions.

        This abstract method should be implemented by subclasses to provide
        the logic for retrieving or building asset definitions specific to
        their asset type.

        Returns:
            List[AssetsDefinition | AssetSpec]: A list of Dagster AssetsDefinition
                objects representing the assets managed by this asset manager.
        """
        raise NotImplementedError("Subclasses must implement get_assets method")
