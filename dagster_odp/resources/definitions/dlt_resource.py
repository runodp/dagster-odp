import importlib
import os
import sys
from typing import Any, Dict, Iterator, List, Mapping, Union

import tomli
from dagster import (
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    MaterializeResult,
)
from dlt import destinations, pipeline
from dlt.common.pipeline import LoadInfo
from dlt.extract.source import DltSource

from ..resource_registry import odp_resource
from .utils import update_asset_params


@odp_resource("dlt")
class OdpDltResource(ConfigurableResource):
    """
    A configurable resource for managing DLT (Data Load Tool) operations.

    This resource provides methods for executing DLT pipelines, handling metadata,
    and managing DLT-specific configurations.

    Attributes:
        project_dir (str): The directory of the DLT project.
    """

    project_dir: str

    def _get_source(self, source_module: str, source_params: Dict) -> DltSource:
        sys.path.append(self.project_dir)
        source_module, source_func_name = source_module.rsplit(".", 1)
        module = importlib.import_module(source_module)
        source_func = getattr(module, source_func_name)
        return source_func(**source_params)

    def _cast_load_info_metadata(self, mapping: Mapping[Any, Any]) -> Mapping[Any, Any]:
        """Converts pendulum DateTime and Timezone values in a mapping to strings.

        Workaround for dagster._core.errors.DagsterInvalidMetadata:
        Could not resolve the metadata value for "jobs" to a known type.
        Value is not JSON serializable.

        Copied from the dagster DLT library.

        Args:
            mapping (Mapping): Dictionary possibly containing pendulum values.

        Returns:
            Mapping[Any, Any]: Metadata with pendulum DateTime and Timezone values
                               casted to strings.

        """

        from pendulum import DateTime  # pylint: disable=import-outside-toplevel

        try:
            from pendulum import Timezone  # pylint: disable=import-outside-toplevel

            casted_instance_types: Union[type, tuple[type, ...]] = (DateTime, Timezone)
        except ImportError:
            casted_instance_types = DateTime

        def _recursive_cast(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: _recursive_cast(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_recursive_cast(item) for item in value]
            if isinstance(value, casted_instance_types):
                return str(value)
            return value

        return {k: _recursive_cast(v) for k, v in mapping.items()}

    def extract_dlt_metadata(
        self, load_info: LoadInfo, asset_name: str
    ) -> Mapping[str, Any]:
        """
        Extracts and updates metadata from the LoadInfo object based on the destination.

        Args:
            load_info (LoadInfo): The LoadInfo object from the pipeline run.
            asset_name (str): The name of the asset created.

        Returns:
            Mapping[str, Any]: A dictionary containing the updated metadata.

        Raises:
            ValueError: If the destination is not supported.
        """
        dlt_base_metadata_types = {
            "started_at",
            "finished_at",
            "dataset_name",
            "destination_name",
            "destination_type",
        }

        load_info_dict = self._cast_load_info_metadata(load_info.asdict())

        base_metadata = {
            k: v for k, v in load_info_dict.items() if k in dlt_base_metadata_types
        }

        if base_metadata["destination_name"] in ["bigquery", "duckdb"]:
            base_metadata["destination_table_id"] = (
                f"{base_metadata['dataset_name']}.{asset_name}"
            )
        if base_metadata["destination_name"] == "filesystem":
            bucket_url = load_info_dict["destination_displayable_credentials"]

            # When DLT does not ingest new data in a run, the dataset_name is None.
            if base_metadata["dataset_name"]:
                base_metadata["destination_file_uri"] = os.path.join(
                    bucket_url, base_metadata["dataset_name"], asset_name
                )
        return base_metadata

    def materialize_dlt_results(
        self,
        load_info: LoadInfo,
        dlt_asset_names: List[List[str]],
    ) -> Iterator[MaterializeResult]:
        """
        Materializes DLT assets with the metadata in the LoadInfo object.

        Args:
            load_info (LoadInfo): The LoadInfo object from the completed DLT run.
            dlt_asset_names (List[List[str]]): A list of asset key components for each
                                               object produced by DLT.

        Yields:
            Iterator[MaterializeResult]
        """
        for asset_name in dlt_asset_names:
            yield MaterializeResult(
                asset_key=AssetKey(asset_name),
                metadata=self.extract_dlt_metadata(load_info, asset_name[-1]),
            )

    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = "__") -> Dict:
        items: List = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _write_dlt_secrets_to_env(self, source_module: str) -> None:
        source_module_path = "/".join(source_module.split(".")[:-2])

        # Read the TOML file
        secrets_path = os.path.join(
            self.project_dir, source_module_path, ".dlt", "secrets.toml"
        )
        if not os.path.exists(secrets_path):
            raise FileNotFoundError(f"File not found: {secrets_path}")
        with open(secrets_path, "rb") as f:
            secrets = tomli.load(f)

        # Flatten the nested dictionary
        flat_secrets = self._flatten_dict(secrets)

        # Convert to environment variables
        for key, value in flat_secrets.items():
            env_key = key.upper()
            os.environ[env_key] = str(value)

    def update_asset_params(
        self,
        context: AssetExecutionContext,
        resource_config: Dict[str, Dict],
        asset_params: Dict,
    ) -> Dict:
        """
        Updates the configuration parameters based on the execution context.

        Args:
            context (AssetExecutionContext): The current asset execution context.
            resource_config (Dict): The resource configuration.
            asset_params (Dict): The asset params.

        Returns:
            Dict
        """
        return update_asset_params(context, resource_config, asset_params)

    def run(
        self,
        params: Dict[str, Any],
        asset_key: str,
        dlt_asset_names: List[List[str]],
    ) -> Iterator[MaterializeResult]:
        """
        Executes a DLT pipeline for the given asset configuration.

        Sets up and runs a DLT pipeline based on the provided parameters,
        handling configurations and secret management. Yields MaterializeResult
        objects for the assets produced by the pipeline.

        Args:
            params: Configuration for the DLT pipeline (source, destination, etc.)
            asset_key: DLT asset_key from the config
            dlt_asset_names: The names of the DLT objects expected to be created
                             by the pipeline

        Yields:
            Iterator[MaterializeResult]
        """

        # Write DLT secrets to dagster environment variables.
        # Creating the source might depend on the secrets.
        self._write_dlt_secrets_to_env(params["source_module"])

        source = self._get_source(params["source_module"], params["source_params"])

        resource_name = asset_key.split("/")[-1]

        destination_func = getattr(destinations, params["destination"])

        dlt_pipeline = pipeline(
            pipeline_name=asset_key.replace("/", "__"),
            destination=destination_func(**params["destination_params"]),
            **params["pipeline_params"],
        )
        load_info = dlt_pipeline.run(
            source.with_resources(resource_name),
            **params["run_params"],
        )
        yield from self.materialize_dlt_results(load_info, dlt_asset_names)
