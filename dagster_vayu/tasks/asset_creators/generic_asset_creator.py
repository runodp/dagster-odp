from typing import Dict, List, Optional, Tuple

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    MaterializeResult,
    TimeWindowPartitionsDefinition,
    asset,
)

from ...config_manager.models.workflow_model import PartitionParams, TaskTypeUnion
from ...utils import ConfigParamReplacer
from ..manager.task_registry import task_registry
from ..utils import generate_partition_params
from .base_asset_creator import BaseAssetCreator


class GenericAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetManager for managing general assets.

    This class is responsible for building and retrieving asset definitions
    based on the workflow configuration.

    Attributes:
        _assets (List[AssetsDefinition]): A list to store asset definitions.

    Methods:
        _execute_task_function(): Executes a task function based on its type.
        _execute_asset_fn(): Executes an asset function with the given context and spec.
        _get_asset_key_split(): Splits an asset key into name and prefix.
        _build_asset(): Builds an asset definition based on the given task spec.
        get_assets(): Retrieves or builds all asset definitions for the workflow.
    """

    def __init__(self) -> None:
        super().__init__()
        self._assets: List[AssetsDefinition] = []

    def _execute_task_function(
        self, task_type: str, params: Dict, resource_map: Dict
    ) -> Dict:
        cls = task_registry[task_type]
        task = cls(**params)
        task.set_resources(resource_map)
        return task.run()

    def _materialize_asset(
        self,
        context: AssetExecutionContext,
        spec: TaskTypeUnion,
        required_resources: List,
    ) -> MaterializeResult:
        config_replacer = ConfigParamReplacer(
            context,
            spec.depends_on,
            self._dagster_config.resources.model_dump(),
        )
        params = config_replacer.replace(spec.params.model_dump())
        resource_map = {
            resource: getattr(context.resources, resource)
            for resource in required_resources
        }
        metadata = self._execute_task_function(spec.task_type, params, resource_map)

        return MaterializeResult(metadata=metadata)

    def _get_asset_key_split(self, asset_key: str) -> Tuple[str, Optional[List[str]]]:
        if "/" not in asset_key:
            return asset_key, None
        name_parts = asset_key.split("/")
        key_prefix, name = name_parts[:-1], name_parts[-1]
        return name, key_prefix

    def _build_asset(
        self, spec: TaskTypeUnion, partitions: Dict[str, PartitionParams]
    ) -> AssetsDefinition:
        """
        Builds an asset definition based on the given task specification.

        Args:
            spec (WorkflowTaskUnion): The task specification from the config files.
            partitions (Dict[str, PartitionParams]): The partition parameters.

        Returns:
            AssetsDefinition
        """
        task_type = spec.task_type
        task_config = next(
            (task for task in self._dagster_config.tasks if task.name == task_type),
            None,
        )

        required_resources = task_config.required_resources if task_config else []
        required_resource_keys = {*required_resources, "sensor_context"}

        name, key_prefix = self._get_asset_key_split(spec.asset_key)

        partition_params = (
            generate_partition_params(partitions[spec.asset_key])
            if spec.asset_key in partitions
            else None
        )

        deps = (
            [AssetKey(dep.split("/")) for dep in spec.depends_on]
            if spec.depends_on
            else None
        )

        @asset(
            name=name,
            key_prefix=key_prefix,
            description=spec.description,
            required_resource_keys=required_resource_keys,
            deps=deps,
            metadata=spec.params.model_dump(),
            group_name=spec.group_name,
            compute_kind=task_config.compute_kind if task_config else None,
            partitions_def=(
                TimeWindowPartitionsDefinition(**partition_params)
                if partition_params
                else None
            ),
        )
        def _asset_def(context: AssetExecutionContext) -> MaterializeResult:
            return self._materialize_asset(context, spec, required_resources)

        return _asset_def

    def get_assets(self) -> List[AssetsDefinition]:
        """
        Builds asset definitions for all the assets in the workflow config files.

        Returns:
            List[AssetsDefinition]
        """
        if not self._assets:
            partitions = self._wb.asset_key_partition_map
            self._assets = [
                self._build_asset(t, partitions) for t in self._wb.generic_assets
            ]
        return self._assets
