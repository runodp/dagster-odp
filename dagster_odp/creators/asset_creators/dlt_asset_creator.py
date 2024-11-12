import os
from typing import Dict, Iterator, List, Optional, Set, Union

import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
    TimeWindowPartitionsDefinition,
    multi_asset,
)

from ...config_manager.models.workflow_model import DLTTask
from ...resources.definitions.dlt_resource import OdpDltResource
from .base_asset_creator import BaseAssetCreator
from .utils import generate_partition_params


class DLTAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetCreator for managing DLT assets.

    This class is responsible for building and retrieving asset definitions
    specifically for DLT assets.

    Attributes:
        _dlt_assets (List[AssetsDefinition | AssetSpec]): A list of DLT asset
            definitions or specs.

    Methods:
        __init__: Initializes the DLTAssetCreator.
        _get_dlt_destination_objects: Retrieves objects from DLT schema files.
        _build_asset: Builds a DLT asset definition based on the given DLT asset spec.
        get_assets: Retrieves or builds all DLT asset definitions for the workflow.
    """

    def __init__(self) -> None:
        super().__init__()
        self._dlt_assets: List[Union[AssetsDefinition, AssetSpec]] = []

    def _get_dlt_destination_objects(
        self,
        dlt_path: str,
        schema_file_path: str,
        resource_name: str,
        schema: Optional[Dict] = None,
    ) -> List[str]:
        if schema is None:
            # The schema file name is the DLT source name.
            # We require the user to provide the schema file path explicitly because:
            # 1. We can't reliably infer the schema file name from the source method
            #    name, as some source methods don't have the source name.
            # 2. We can't initialize the source to get the source name, as the source
            #    arguments might contain variables that need to be replaced.
            schema_file_path = os.path.join(dlt_path, schema_file_path)
            if not os.path.exists(schema_file_path):
                raise FileNotFoundError(
                    f"Schema file is missing in path '{schema_file_path}'. "
                    f"Please generate it."
                )
            with open(schema_file_path, encoding="utf-8") as f:
                schema = yaml.safe_load(f)

        if not schema or not schema.get("tables"):
            raise ValueError("Schema must contain the 'tables' key.")

        return [
            table_name
            for table_name, table_data in schema["tables"].items()
            if table_name.startswith(resource_name) and table_data.get("columns")
        ]

    def _build_asset(self, dlt_asset: DLTTask, dlt_path: str) -> AssetsDefinition:
        prefix, resource_name = dlt_asset.asset_key.rsplit("/", 1)

        destination_objects = self._get_dlt_destination_objects(
            dlt_path,
            dlt_asset.params.schema_file_path,
            resource_name,
        )

        dlt_asset_names = [prefix.split("/") + [obj] for obj in destination_objects]

        partitions = self._wb.asset_key_partition_map

        partition_params = (
            generate_partition_params(
                partitions[dlt_asset.asset_key].model_dump(
                    exclude_defaults=True, exclude_unset=True
                )
            )
            if dlt_asset.asset_key in partitions
            else None
        )

        @multi_asset(
            name=dlt_asset.asset_key.replace("/", "__"),
            group_name=dlt_asset.group_name,
            required_resource_keys={"sensor_context", "dlt"},
            compute_kind="dlt",
            partitions_def=(
                TimeWindowPartitionsDefinition(**partition_params)
                if partition_params
                else None
            ),
            specs=[
                AssetSpec(
                    key=AssetKey(asset_name),
                    deps=[AssetKey(prefix.split("/"))],
                    description=("/").join(asset_name),
                    tags={"dagster/storage_kind": dlt_asset.params.destination},
                    metadata=dlt_asset.params.model_dump(),
                )
                for asset_name in dlt_asset_names
            ],
        )
        def _dlt_asset_defs(
            context: AssetExecutionContext,
        ) -> Iterator[MaterializeResult]:
            dlt = context.resources.dlt
            replaced_params = dlt.update_asset_params(
                context, self._resource_config_map, dlt_asset.params.model_dump()
            )
            dlt_client: OdpDltResource = context.resources.dlt
            yield from dlt_client.run(
                replaced_params, dlt_asset.asset_key, dlt_asset_names
            )

        return _dlt_asset_defs

    def get_assets(self) -> List[Union[AssetsDefinition, AssetSpec]]:
        """
        Retrieves the DLT asset definitions.

        If the DLT assets have not been built yet, it builds them based on the
        DLT assets in the workflow configuration files.

        The primary purpose of DLT is to ingest data on a schedule.
        Hence they do not support having a parent.

        They also do not support being partitioned, but this could be implemented.

        Returns:
            List[AssetsDefinition | AssetSpec]: A list of DLT asset definitions,
                including external asset specs and the built DLT assets.
        """
        if not self._dlt_assets:
            dlt_assets = self._wb.get_assets_with_task_type(DLTTask)
            if not self._resource_class_map.get("dlt"):
                raise ValueError("Resource config must contain dlt resource config.")
            dlt_path = self._resource_class_map["dlt"].project_dir

            external_assets: List[AssetSpec] = []
            created_external_keys: Set[tuple] = set()

            for asset in dlt_assets:
                external_asset_key = tuple(asset.asset_key.split("/")[:-1])
                if (
                    external_asset_key
                    and external_asset_key not in created_external_keys
                ):
                    external_assets.append(
                        AssetSpec(
                            key=external_asset_key,
                            group_name=asset.group_name,
                            description=asset.description,
                        )
                    )
                    created_external_keys.add(external_asset_key)
            self._dlt_assets = external_assets + [
                self._build_asset(t, dlt_path) for t in dlt_assets
            ]
        return self._dlt_assets
