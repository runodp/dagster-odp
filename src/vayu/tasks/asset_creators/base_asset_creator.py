from abc import ABC, abstractmethod
from typing import List, Self

from dagster import AssetsDefinition

from ...config_manager.builders.config_builder import ConfigBuilder
from ...config_manager.builders.workflow_builder import WorkflowBuilder


class BaseAssetCreator(ABC):
    """
    An abstract base class for asset managers.

    This class provides a common interface and some basic functionality
    for asset managers. It uses a singleton pattern to ensure only one
    instance exists.

    Attributes:
        _wb (WorkflowBuilder): An instance of WorkflowBuilder.
        _dagster_config (DagsterConfig): The Dagster configuration.

    Methods:
        _build_asset(): Abstract method to build an asset.
        get_assets(): Abstract method to retrieve assets.
    """

    _instance = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._wb = WorkflowBuilder()
        self._dagster_config = ConfigBuilder().get_config()

    @abstractmethod
    def get_assets(self) -> List[AssetsDefinition]:
        """
        Retrieves or builds a list of asset definitions.

        This abstract method should be implemented by subclasses to provide
        the logic for retrieving or building asset definitions specific to
        their asset type.

        Returns:
            List[AssetsDefinition]: A list of Dagster AssetsDefinition objects
            representing the assets managed by this asset manager.
        """
