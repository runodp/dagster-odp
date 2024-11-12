import json
from typing import Any, Dict, Iterator, List, Mapping, Optional, Union

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    AssetSpec,
    Output,
    TimeWindowPartitionsDefinition,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliEventMessage, DbtProject, dbt_assets

from ...config_manager.models.workflow_model import DBTTask
from ...resources.definitions import OdpDbtResource
from .base_asset_creator import BaseAssetCreator
from .utils import generate_partition_params


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """
    Extends the DagsterDbtTranslator to include custom metadata in the asset metadata.

    This class overrides the `get_metadata` method to merge custom metadata
    with the base metadata provided by the parent class.

    Attributes:
        custom_metadata (Dict[str, Any]): Additional metadata to be included
                                          in the asset metadata.
    """

    def __init__(self, custom_metadata: Dict[str, Any]):
        super().__init__()
        self.custom_metadata = custom_metadata

    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> dict[str, Any]:
        base_metadata = super().get_metadata(dbt_resource_props)
        return {**base_metadata, **self.custom_metadata}


class DBTAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetCreator for managing DBT assets.

    This class is responsible for creating Dagster asset definitions
    from DBT models. It uses DbtProject for core DBT functionality and adds
    ODP-specific asset creation capabilities.

    Attributes:
        _dbt_cli_resource (OdpDbtResource): The DBT CLI resource.
        _dbt_project (DbtProject): Core DBT project functionality.
        _dbt_manifest_path (Path): The path to the DBT manifest file.

    Methods:
        build_dbt_external_sources: Builds external asset definitions for DBT sources.
        _get_dbt_output_metadata: Extracts metadata from a DBT CLI event.
        _materialize_dbt_asset: Materializes a DBT asset.
        _build_asset: Builds a DBT asset definition.
        get_assets: Retrieves or builds all DBT asset definitions for the workflow.
    """

    def __init__(self) -> None:
        super().__init__()
        dbt_cli_resource = self._resource_class_map.get("dbt")
        if not dbt_cli_resource:
            raise ValueError("dbt resource must be configured in dagster config")

        self._dbt_cli_resource: OdpDbtResource = dbt_cli_resource
        self._dbt_project = DbtProject(
            project_dir=self._dbt_cli_resource.project_dir,
            target=self._dbt_cli_resource.target,
        )
        # Ensure project is prepared during development
        self._dbt_project.prepare_if_dev()

    def build_dbt_external_sources(self) -> List[AssetSpec]:
        """
        Creates Dagster asset specs for any DBT sources that are marked as external
        in their DBT metadata. This allows non-dagster sources to be represented
        in Dagster in the appropriate group.

        Returns:
            List[AssetSpec]: A list of asset specs for external sources.
        """
        with open(self._dbt_project.manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        dagster_dbt_translator = DagsterDbtTranslator()
        return [
            AssetSpec(
                key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
                group_name=dagster_dbt_translator.get_group_name(dbt_resource_props),
                description=dagster_dbt_translator.get_description(dbt_resource_props),
            )
            for dbt_resource_props in manifest.get("sources", {}).values()
            if dbt_resource_props.get("meta", {}).get("dagster", {}).get("external")
            in (True, "true", "True", "TRUE")
        ]

    def _get_dbt_output_metadata(self, event: DbtCliEventMessage) -> Dict:
        dest_info = event.raw_event["data"]["node_info"]["node_relation"]
        return {"destination_table_id": f"{dest_info['schema']}.{dest_info['alias']}"}

    def _materialize_dbt_asset(
        self,
        context: AssetExecutionContext,
        dbt_vars: Dict[str, str],
    ) -> Iterator[Any]:
        dbt = context.resources.dbt
        replaced_dbt_vars = dbt.update_asset_params(
            context=context,
            resource_config=self._resource_config_map,
            asset_params=dbt_vars,
        )

        dbt_cli_invocation = dbt.cli(
            ["build", "--vars", json.dumps(replaced_dbt_vars)], context=context
        )

        for event in dbt_cli_invocation.stream_raw_events():
            for dagster_event in event.to_default_asset_events(
                manifest=dbt_cli_invocation.manifest,
                dagster_dbt_translator=dbt_cli_invocation.dagster_dbt_translator,
                context=dbt_cli_invocation.context,
                target_path=dbt_cli_invocation.target_path,
            ):
                if isinstance(dagster_event, Output):
                    context.add_output_metadata(
                        metadata=self._get_dbt_output_metadata(event),
                        output_name=dagster_event.output_name,
                    )
                yield dagster_event

    def _build_asset(
        self,
        name: str,
        # Name needs to be passed in; Dagster cannot automatically
        # differentiate between different DBT AssetDefinitions because of a bug
        dbt_vars: Dict[str, str],
        select: str = "fqn:*",
        exclude: Optional[str] = None,
        partition_params: Optional[Dict] = None,
    ) -> AssetsDefinition:
        """Creates a DBT asset definition with specified parameters."""
        partitions_def = (
            TimeWindowPartitionsDefinition(**partition_params)
            if partition_params
            else None
        )

        custom_dbt_translator = CustomDagsterDbtTranslator(
            {
                "dbt_vars": dbt_vars,
                "select": select,
                "exclude": exclude,
            }
        )

        @dbt_assets(
            manifest=self._dbt_project.manifest_path,
            select=select,
            exclude=exclude,
            partitions_def=partitions_def,
            name=name,
            required_resource_keys={"dbt", "sensor_context"},
            dagster_dbt_translator=custom_dbt_translator,
        )
        def _dbt_assets(context: AssetExecutionContext) -> Iterator[Any]:
            yield from self._materialize_dbt_asset(context, dbt_vars)

        return _dbt_assets

    def get_assets(self) -> List[Union[AssetsDefinition, AssetSpec]]:
        """
        Creates all DBT asset definitions based on configuration.

        This method processes the ODP configuration to create three types of assets:
        1. Configured DBT models with their partitioning and variables
        2. Remaining unselected models (if load_all_models is True)
        3. External source definitions

        The partitioning is optional and applied on a per-asset basis.
        DBT vars are also applied per asset, allowing different configurations
        for different parts of your DBT project.

        Returns:
            List[AssetsDefinition | AssetSpec]: A list containing:
                - Individual asset definitions for each configured DBT asset
                - An 'unselected_dbt_assets' asset for remaining models (if enabled)
                - External source asset definitions
        """
        partitions = self._wb.asset_key_partition_map
        dbt_config_assets = self._wb.get_assets_with_task_type(DBTTask)

        all_dbt_assets = []
        for dbt_asset in dbt_config_assets:
            partition = partitions.get(dbt_asset.asset_key)
            partition_params = (
                generate_partition_params(
                    partition.model_dump(exclude_defaults=True, exclude_unset=True)
                )
                if partition
                else None
            )

            all_dbt_assets.append(
                self._build_asset(
                    select=dbt_asset.params.selection,
                    partition_params=partition_params,
                    name=dbt_asset.asset_key,
                    dbt_vars=dbt_asset.params.dbt_vars,
                )
            )

        # Handle unselected models if configured
        if self._dbt_cli_resource.load_all_models:
            partition_selection = " ".join(
                dbt_asset.params.selection for dbt_asset in dbt_config_assets
            )
            all_dbt_assets.append(
                self._build_asset(
                    exclude=partition_selection,
                    name="unselected_dbt_models",
                    dbt_vars={},
                )
            )

        return all_dbt_assets + self.build_dbt_external_sources()
