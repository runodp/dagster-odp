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
from ...tasks.manager.task_registry import task_registry
from ...utils import ConfigParamReplacer
from .base_asset_creator import BaseAssetCreator
from .utils import generate_partition_params


class GenericAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetCreator for managing general assets.

    This class is responsible for building and retrieving asset definitions
    based on the workflow configuration.

    Attributes:
        _assets (List[AssetsDefinition]): A list to store asset definitions.

    Methods:
        __init__: Initializes the GenericAssetCreator.
        _execute_task_function: Executes a task function based on its type.
        _materialize_asset: Materializes an asset with the given context, spec,
                            and required resources.
        _get_asset_key_split: Splits an asset key into name and prefix.
        _build_asset: Builds an asset definition based on the given task specification.
        get_assets: Retrieves or builds all asset definitions for the workflow.
    """

    def __init__(self) -> None:
        super().__init__()
        self._assets: List[AssetsDefinition] = []

    def _execute_task_function(
        self,
        task_type: str,
        params: Dict,
        context: AssetExecutionContext,
    ) -> Dict:
        cls = task_registry[task_type]["class"]
        task = cls(**params)
        task.initialize(context)
        return task.run()

    def _materialize_asset(
        self,
        context: AssetExecutionContext,
        spec: TaskTypeUnion,
    ) -> MaterializeResult:
        config_replacer = ConfigParamReplacer(
            context, spec.depends_on, self._resource_config_map
        )
        params = config_replacer.replace(spec.params.model_dump())

        metadata = self._execute_task_function(spec.task_type, params, context)
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
        task_config = task_registry[spec.task_type]

        required_resources = task_config["required_resources"]
        required_resource_keys = {*required_resources, "sensor_context"}

        name, key_prefix = self._get_asset_key_split(spec.asset_key)

        partition_params = (
            generate_partition_params(
                partitions[spec.asset_key].model_dump(
                    exclude_defaults=True, exclude_unset=True
                )
            )
            if spec.asset_key in partitions
            else None
        )

        deps = (
            [AssetKey(dep.split("/")) for dep in spec.depends_on]
            if spec.depends_on
            else None
        )

        tags = (
            {"dagster/storage_kind": task_config["storage_kind"]}
            if task_config["storage_kind"]
            else {}
        )

        @asset(
            name=name,
            key_prefix=key_prefix,
            description=spec.description,
            required_resource_keys=required_resource_keys,
            deps=deps,
            metadata=spec.params.model_dump(),
            group_name=spec.group_name,
            compute_kind=task_config["compute_kind"],
            partitions_def=(
                TimeWindowPartitionsDefinition(**partition_params)
                if partition_params
                else None
            ),
            tags=tags,
        )
        def _asset_def(context: AssetExecutionContext) -> MaterializeResult:
            return self._materialize_asset(context, spec)

        return _asset_def

    def get_assets(self) -> List[AssetsDefinition]:
        """
        Retrieves or builds asset definitions for all the assets in the workflow config.

        If the assets haven't been built yet, this method will build them based on
        the workflow configuration.

        Returns:
            List[AssetsDefinition]: A list of asset definitions.
        """
        if not self._assets:
            partitions = self._wb.asset_key_partition_map
            self._assets = [
                self._build_asset(t, partitions) for t in self._wb.generic_assets
            ]
        return self._assets
