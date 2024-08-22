import json
import os
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

import yaml
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    AssetSpec,
    Output,
    TimeWindowPartitionsDefinition,
    external_assets_from_specs,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliEventMessage, dbt_assets

from ...config_manager.models.workflow_model import DBTParams, DBTTask
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
        merged_metadata = {**base_metadata, **self.custom_metadata}
        return merged_metadata


class DBTAssetCreator(BaseAssetCreator):
    """
    A concrete implementation of BaseAssetCreator for managing DBT assets.

    This class is responsible for building and retrieving asset definitions
    specifically for DBT assets.

    Attributes:
        _resources (Any): The resources configuration.
        _dbt_cli_resource (VayuDbtResource): The DBT CLI resource.
        _dbt_manifest_path (Path): The path to the DBT manifest file.

    Methods:
        __init__: Initializes the DBTAssetCreator.
        _parse_project: Parses the DBT project and returns the manifest path.
        _load_dbt_manifest_path: Loads or generates the DBT manifest path.
        _manifest_sources: Property that returns the sources from the DBT manifest.
        build_dbt_external_sources: Builds external asset definitions for DBT sources.
        _read_or_create_sources_yml: Reads or creates a sources YAML file.
        _update_sources_yml_data: Updates the sources YAML data.
        update_dbt_sources: Updates the sources YAML file based on DBT assets
                            and the manifest.
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

        self._load_dbt_manifest_path()

    def _parse_project(self, target_path: Path) -> Path:
        return (
            self._dbt_cli_resource.cli(["--quiet", "parse"], target_path=target_path)
            .wait()
            .target_path.joinpath("manifest.json")
        )

    def _load_dbt_manifest_path(self) -> None:
        """
        Loads or generates the DBT manifest path.

        If the DAGSTER_DBT_PARSE_PROJECT_ON_LOAD environment variable is set,
        this method will parse the DBT project and generate a new manifest.
        Otherwise, it will use the existing manifest file in the project directory.
        """
        if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
            if self._dbt_cli_resource.sources_file_path:
                self.update_dbt_sources()
            self._dbt_manifest_path = self._parse_project(Path("target"))
        else:
            self._dbt_manifest_path = Path(self._dbt_cli_resource.project_dir).joinpath(
                "target", "manifest.json"
            )

    @cached_property
    def _manifest_sources(self) -> Dict[str, Any]:
        with open(self._dbt_manifest_path, "r", encoding="utf-8") as file:
            manifest = json.load(file)
        return manifest.get("sources", {})

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
            for dbt_resource_props in self._manifest_sources.values()
            if not dbt_resource_props.get("meta", {})
            .get("dagster", {})
            .get("asset_key")
        ]
        return external_assets_from_specs(asset_defs)

    def _read_or_create_sources_yml(self, sources_yml_path: str) -> Dict[str, Any]:
        if os.path.exists(sources_yml_path):
            with open(sources_yml_path, "r", encoding="utf-8") as file:
                return yaml.safe_load(file) or {"version": 2, "sources": []}
        return {"version": 2, "sources": []}

    def _update_sources_yml_data(
        self, sources_yml_data: Dict[str, Any], dbt_asset_defs: Dict[str, DBTParams]
    ) -> Dict[str, Any]:
        for asset_key, dbt_params in dbt_asset_defs.items():
            source_name = dbt_params.source_name
            table_name = dbt_params.table_name

            table = {
                "name": table_name,
                "meta": {"dagster": {"asset_key": [asset_key]}},
            }
            existing_source = next(
                (s for s in sources_yml_data["sources"] if s["name"] == source_name),
                None,
            )
            if existing_source:
                existing_table = next(
                    (t for t in existing_source["tables"] if t["name"] == table_name),
                    None,
                )
                if existing_table:
                    continue

                existing_source["tables"].append(table)
            else:
                sources_yml_data["sources"].append(
                    {"name": source_name, "tables": [table]}
                )
        return sources_yml_data

    def update_dbt_sources(self) -> None:
        """
        Update the sources.yml file based on the DBT assets and manifest.

        This method compares the sources in the manifest with the DBT assets
        and updates the sources.yml file accordingly. It raises an error if
        there's a discrepancy between the sources.yml and the manifest.

        Raises:
            ValueError: If a source and table exist in sources.yml but
                        is not in the manifest.
        """
        if not self._dbt_cli_resource.sources_file_path:
            raise ValueError("sources_file_path must be configured in dagster config")

        sources_yml_path = os.path.join(
            self._dbt_cli_resource.project_dir,
            self._dbt_cli_resource.sources_file_path,
        )

        sources_yml_data = self._read_or_create_sources_yml(sources_yml_path)
        dbt_asset_defs = self._wb.asset_key_dbt_params_map

        sources_yml_data = self._update_sources_yml_data(
            sources_yml_data, dbt_asset_defs
        )

        with open(sources_yml_path, "w", encoding="utf-8") as file:
            yaml.dump(sources_yml_data, file, sort_keys=False)
        print(f"YAML file updated successfully at {sources_yml_path}")

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
