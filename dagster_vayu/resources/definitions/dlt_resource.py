import importlib
import os
import sys
import tomllib
from typing import Any, Callable, Dict, Iterator, List, Mapping, Union

from dagster import (
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    MaterializeResult,
)
from dlt import destinations, pipeline
from dlt.common.pipeline import LoadInfo

from dagster_vayu.config_manager.models.workflow_model import DLTTask

from ..resource_registry import vayu_resource
from .utils import update_asset_params


@vayu_resource("dlt")
class VayuDltResource(ConfigurableResource):
    """
    A configurable resource for managing DLT (Data Loading Tool) operations.

    This resource provides methods for executing DLT pipelines, handling metadata,
    and managing DLT-specific configurations.

    Attributes:
        project_dir (str): The directory of the DLT project.
    """

    project_dir: str

    def _get_source_func(self, source_module: str, source_func_name: str) -> Callable:
        sys.path.append(self.project_dir)
        module = importlib.import_module(source_module)
        source_func = getattr(module, source_func_name)
        return source_func

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
            "first_run",
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

        if base_metadata["destination_name"] == "bigquery":
            base_metadata["destination_table_id"] = (
                f"{base_metadata['dataset_name']}.{asset_name}"
            )
        elif base_metadata["destination_name"] == "filesystem":
            bucket_url = load_info_dict["destination_displayable_credentials"]
            base_metadata["destination_file_uri"] = os.path.join(
                bucket_url, base_metadata["dataset_name"], asset_name
            )
        else:
            raise ValueError(
                f"Unsupported destination: {base_metadata['destination_name']}"
            )

        return base_metadata

    def materialize_dlt_results(
        self,
        op_name: str,
        load_info: LoadInfo,
        dlt_asset_names: Dict[str, List[List[str]]],
    ) -> Iterator[MaterializeResult]:
        """
        Generates MaterializeResult objects for DLT assets.

        Args:
            op_name (str): The name of the op.
            load_info (LoadInfo): The LoadInfo object from the pipeline run.
            dlt_asset_names (Dict[str, List[List[str]]]): A dictionary mapping
                op names to lists of asset names.

        Yields:
            Iterator[MaterializeResult]
        """
        for asset_name in dlt_asset_names[op_name]:
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

    def _write_dlt_secrets_to_env(self, module_name: str) -> None:
        # Read the TOML file
        secrets_path = os.path.join(
            self.project_dir, module_name, ".dlt", "secrets.toml"
        )
        if not os.path.exists(secrets_path):
            raise FileNotFoundError(f"File not found: {secrets_path}")
        with open(secrets_path, "rb") as f:
            secrets = tomllib.load(f)

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
        dlt_asset: DLTTask,
        dlt_asset_names: Dict[str, List[List[str]]],
    ) -> Iterator[MaterializeResult]:
        """
        Runs a DLT pipeline for the given asset.

        This method sets up and executes a DLT pipeline, handling the necessary
        configurations and yielding MaterializeResult objects for the assets.

        Args:
            context (AssetExecutionContext): The asset execution context.
            dlt_asset (DLTTask): The DLT asset to process.
            dlt_asset_names (Dict[str, List[List[str]]]): A dictionary mapping
                op names to lists of asset names.

        Yields:
            Iterator[MaterializeResult]
        """
        source_module, source_func_name = dlt_asset.params.source_module.rsplit(".", 1)
        source_func = self._get_source_func(source_module, source_func_name)

        op_name, resource_name = dlt_asset.asset_key.rsplit("/", 1)

        self._write_dlt_secrets_to_env(source_module.split(".")[0])

        destination_func = getattr(destinations, dlt_asset.params.destination)

        dlt_pipeline = pipeline(
            pipeline_name=dlt_asset.asset_key.replace("/", "__"),
            destination=destination_func(**params["destination_params"]),
            **params["pipeline_params"],
        )
        load_info = dlt_pipeline.run(
            source_func(**params["source_params"]).with_resources(resource_name),
            **params["run_params"],
        )
        yield from self.materialize_dlt_results(op_name, load_info, dlt_asset_names)
