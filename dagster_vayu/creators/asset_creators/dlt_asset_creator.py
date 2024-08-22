import os
from typing import Dict, Iterator, List, Optional

import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
    external_asset_from_spec,
    multi_asset,
)

from ...config_manager.models.workflow_model import DLTTask
from .base_asset_creator import BaseAssetCreator


class DLTAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetCreator for managing DLT assets.

    This class is responsible for building and retrieving asset definitions
    specifically for DLT assets.

    Attributes:
        _dlt_assets (List[AssetsDefinition]): A list to store DLT asset definitions.

    Methods:
        __init__: Initializes the DLTAssetCreator.
        _get_dlt_destination_objects: Retrieves objects from DLT schema files.
        _build_asset: Builds a DLT asset definition based on the given DLT asset spec.
        get_assets: Retrieves or builds all DLT asset definitions for the workflow.
    """

    def __init__(self) -> None:
        super().__init__()
        self._dlt_assets: List[AssetsDefinition] = []

    def _get_dlt_destination_objects(
        self,
        dlt_path: str,
        source_module: str,
        source_func_name: str,
        schema: Optional[Dict] = None,
    ) -> List[str]:
        if schema is None:
            source_directory = source_module.split(".")[0]
            schema_file_name = f"{source_func_name}.schema.yaml"
            schema_file_path = os.path.join(
                dlt_path, source_directory, "schemas", "export", schema_file_name
            )
            if not os.path.exists(schema_file_path):
                raise FileNotFoundError(
                    f"Schema file '{schema_file_name}' is missing. Please generate it."
                )
            with open(schema_file_path, encoding="utf-8") as f:
                schema = yaml.safe_load(f)

        if not schema or not schema.get("tables"):
            raise ValueError("Schema must contain the 'tables' key.")

        return [
            table_name
            for table_name, table_data in schema["tables"].items()
            if not table_name.startswith("_") and table_data.get("columns")
        ]

    def _build_asset(self, dlt_asset: DLTTask, dlt_path: str) -> AssetsDefinition:
        source_module, source_func_name = dlt_asset.params.source_module.rsplit(".", 1)
        destination_objects = self._get_dlt_destination_objects(
            dlt_path, source_module, source_func_name
        )
        op_name = dlt_asset.asset_key.rsplit("/", 1)[0]

        dlt_asset_names: Dict[str, List[List[str]]] = {}

        dlt_asset_names[op_name] = [
            op_name.split("/") + [obj] for obj in destination_objects
        ]

        @multi_asset(
            name=op_name.replace("/", "__"),
            group_name=dlt_asset.group_name,
            required_resource_keys={"sensor_context", "dlt"},
            compute_kind="dlt",
            specs=[
                AssetSpec(
                    key=AssetKey(asset_name),
                    deps=[AssetKey(op_name.split("/"))],
                    description=("/").join(asset_name),
                    tags={"dagster/storage_kind": dlt_asset.params.destination},
                    metadata=dlt_asset.params.model_dump(),
                )
                for asset_name in dlt_asset_names[op_name]
            ],
        )
        def _dlt_asset_defs(
            context: AssetExecutionContext,
        ) -> Iterator[MaterializeResult]:
            yield from context.resources.dlt.run(context, dlt_asset, dlt_asset_names)

        return _dlt_asset_defs

    def get_assets(self) -> List[AssetsDefinition]:
        """
        Retrieves the DLT asset definitions.

        If the DLT assets have not been built yet, it builds them based on the
        DLT assets in the workflow configuration files.

        The primary purpose of DLT is to ingest data on a schedule.
        Hence they do not support having a parent.

        They also do not support being partitioned, but this could be implemented.

        Returns:
            List[AssetsDefinition]: A list of DLT asset definitions, including external
                                    asset specs and the built DLT assets.
        """
        if not self._dlt_assets:
            dlt_assets = self._wb.get_assets_with_task_type(DLTTask)
            if not self._resource_class_map.get("dlt"):
                raise ValueError("Resource config must contain dlt resource config.")
            dlt_path = self._resource_class_map["dlt"].project_dir

            external_assets = []
            for asset in dlt_assets:
                external_asset_key = asset.asset_key.split("/")[:-1]
                if external_asset_key:
                    external_assets.append(
                        external_asset_from_spec(
                            AssetSpec(
                                key=external_asset_key,
                                group_name=asset.group_name,
                                description=asset.description,
                            )
                        )
                    )
            self._dlt_assets = external_assets + [
                self._build_asset(t, dlt_path) for t in dlt_assets
            ]
        return self._dlt_assets
