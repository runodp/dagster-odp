import json
from typing import Any, Dict, Iterator, List, Mapping, Optional

from dagster_dbt import DagsterDbtTranslator, DbtCliEventMessage, dbt_assets

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    AssetSpec,
    Output,
    TimeWindowPartitionsDefinition,
    external_assets_from_specs,
)

from ...config_manager.models.workflow_model import DBTTask
from .base_asset_creator import BaseAssetCreator
from .dbt_project_manager import DBTProjectManager
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
        merged_metadata = {**base_metadata, **self.custom_metadata}
        return merged_metadata


class DBTAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetCreator for managing DBT assets.

    This class is responsible for creating Dagster asset definitions
    from DBT models. It uses a DBTProjectManager to handle DBT-specific operations.

    Attributes:
        _dbt_cli_resource (VayuDbtResource): The DBT CLI resource.
        _dbt_project_manager (DBTProjectManager): Manager for DBT project operations.
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
        if dbt_cli_resource:
            self._dbt_cli_resource = dbt_cli_resource
        else:
            raise ValueError("dbt resource must be configured in dagster config")

        self._project_manager = DBTProjectManager(
            self._dbt_cli_resource, self._wb.asset_key_dbt_params_map
        )
        self._dbt_manifest_path = self._project_manager.manifest_path

    def build_dbt_external_sources(self) -> List[AssetsDefinition]:
        """
        Builds the dbt sources assets.

        Returns:
            List[AssetsDefinition]: The dbt sources assets.
        """
        dagster_dbt_translator = DagsterDbtTranslator()
        asset_defs = [
            AssetSpec(
                key=dagster_dbt_translator.get_asset_key(dbt_resource_props),
                group_name=dagster_dbt_translator.get_group_name(dbt_resource_props),
                description=dagster_dbt_translator.get_description(dbt_resource_props),
            )
            for dbt_resource_props in self._project_manager.manifest_sources.values()
            if dbt_resource_props.get("meta", {}).get("dagster", {}).get("external")
            in (True, "true", "True", "TRUE")
        ]
        return external_assets_from_specs(asset_defs)

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

        dbt_args = ["build", "--vars", json.dumps(replaced_dbt_vars)]

        dbt_cli_invocation = dbt.cli(dbt_args, context=context)

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
        # Name needs to be passed in, otherwise there's a dagster bug that is unable to
        # differentiate between the different DBT AssetDefinitions
        dbt_vars: Dict[str, str],
        select: str = "fqn:*",
        exclude: Optional[str] = None,
        partition_params: Optional[Dict] = None,
    ) -> AssetsDefinition:
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
            manifest=self._dbt_manifest_path,
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

    def get_assets(self) -> List[AssetsDefinition]:
        """
        Retrieves and constructs all DBT asset definitions based on the configuration.

        This method performs the following tasks:
        1. Retrieves DBT assets defined in the configuration.
        2. Creates separate asset definitions for each DBT asset, which may contain
           multiple DBT models.
        3. Applies DBT vars and partitions (if specified) to each asset,
           affecting all models within that asset.
        4. Creates an additional 'unselected_dbt_assets' asset for models not explicitly
           selected in other assets.
        5. Includes external source assets.

        The partitioning is optional and applied on a per-asset basis.
        DBT vars are also applied per asset.

        Returns:
            List[AssetsDefinition]: A list containing:
                - Individual asset definitions for each configured DBT asset.
                - An 'unselected_dbt_assets' asset for the remaining models.
                - External source asset definitions.
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

        partition_selection = " ".join(
            [dbt_asset.params.selection for dbt_asset in dbt_config_assets]
        )
        all_dbt_assets.append(
            self._build_asset(
                exclude=partition_selection, name="unselected_dbt_assets", dbt_vars={}
            )
        )
        return all_dbt_assets + self.build_dbt_external_sources()
