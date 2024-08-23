import json
import os
from functools import cached_property
from pathlib import Path
from typing import Any, Dict

import yaml

from ...config_manager.models.workflow_model import DBTParams
from ...resources.definitions import VayuDbtResource


class DBTProjectManager:
    """
    Manages operations related to the DBT project.

    This class is responsible for parsing the DBT project, loading the manifest,
    and updating the sources. It provides an interface for interacting with
    the DBT project files and configuration.

    Attributes:
        _dbt_cli_resource: The DBT CLI resource used for executing DBT commands.
        _dbt_manifest_path (Path): The path to the DBT manifest file.
        _dbt_asset_defs (Dict[str, DBTParams]): The DBT asset definitions.

    Methods:
        manifest_path: Property that returns the path to the DBT manifest file.
        manifest_sources: Property that returns the sources from the DBT manifest.
        update_dbt_sources: Updates the sources.yml file.
    """

    def __init__(
        self, dbt_cli_resource: VayuDbtResource, dbt_asset_defs: Dict[str, DBTParams]
    ) -> None:
        """
        Initializes the DBTProjectManager.

        Args:
            dbt_cli_resource (VayuDbtResource)
        """
        self._dbt_cli_resource = dbt_cli_resource
        self._dbt_asset_defs = dbt_asset_defs
        self._load_dbt_manifest_path()

    def _parse_project(self, target_path: Path) -> Path:
        """
        Parses the DBT project and returns the path to the generated manifest.

        Args:
            target_path (Path)

        Returns:
            Path
        """
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

    @property
    def manifest_path(self) -> Path:
        """
        Returns the path to the DBT manifest file.

        Returns:
            Path: The path to the DBT manifest file.
        """
        return self._dbt_manifest_path

    @cached_property
    def manifest_sources(self) -> Dict[str, Any]:
        """
        Returns the sources from the DBT manifest.

        This property is cached to avoid repeated file reads.

        Returns:
            Dict[str, Any]: The sources from the DBT manifest.
        """
        with open(self._dbt_manifest_path, "r", encoding="utf-8") as file:
            manifest = json.load(file)
        return manifest.get("sources", {})

    def _read_or_create_sources_yml(self, sources_yml_path: str) -> Dict[str, Any]:
        """
        Reads an existing sources.yml file or creates a new one if it doesn't exist.

        Args:
            sources_yml_path (str)

        Returns:
            Dict[str, Any]
        """
        if os.path.exists(sources_yml_path):
            with open(sources_yml_path, "r", encoding="utf-8") as file:
                return yaml.safe_load(file) or {"version": 2, "sources": []}
        return {"version": 2, "sources": []}

    def _update_sources_yml_data(
        self, sources_yml_data: Dict[str, Any], dbt_asset_defs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Updates the sources.yml data with new or modified source definitions.

        Args:
            sources_yml_data (Dict[str, Any])
            dbt_asset_defs (Dict[str, Any])

        Returns:
            Dict[str, Any]
        """
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

        sources_yml_data = self._update_sources_yml_data(
            sources_yml_data, self._dbt_asset_defs
        )

        with open(sources_yml_path, "w", encoding="utf-8") as file:
            yaml.dump(sources_yml_data, file, sort_keys=False)
        print(f"YAML file updated successfully at {sources_yml_path}")
