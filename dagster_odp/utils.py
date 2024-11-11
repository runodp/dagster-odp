from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, TypeVar, Union

import chevron
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterEventType,
    DagsterInvariantViolationError,
    EventRecordsFilter,
)
from dagster._check.functions import CheckError
from dateutil import parser

T = TypeVar("T", Dict, List, str)


def has_partition_def(context: AssetExecutionContext, input_name: str) -> bool:
    """
    Checks if the asset has partitions
    """
    try:
        partitions_def = context.asset_partitions_def_for_input(input_name)
        return partitions_def is not None
    except DagsterInvariantViolationError:
        return False


class ConfigParamReplacer:
    """
    A class responsible for replacing configuration placeholders with actual values.

    This class handles the replacement of various types of configuration placeholders,
    including sensor context, resource configuration, run information,
    partition information, and parent materializations.

    Attributes:
        context (AssetExecutionContext): The context of the asset execution.
        depends_on (List[str]): A list of dependencies for the current asset.
        resource_config (Dict): The resource configuration.
    """

    def __init__(
        self,
        context: AssetExecutionContext,
        depends_on: Union[List[str], None],
        resource_config: Dict,
    ):
        self.context = context
        self.depends_on = depends_on
        self.resource_config = resource_config

    def _format_date(self, text: str, render: Callable = chevron.render) -> str:
        """
        Formats a date string using the specified format.
        """
        date_str, fmt = "", ""
        try:
            result = render(text)
            date_str, fmt = result.split("|")
            date_obj = parser.parse(date_str)
            return date_obj.strftime(fmt)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Error formatting date '{date_str}' with format '{fmt}': {str(e)}"
            ) from e

    def _replace_config(self, params: T, config: Dict) -> T:
        """
        Recursively replaces placeholders in params with values from the config dict.

        Args:
            params (T)
            config (Dict)

        Returns:
            T: The updated parameters with placeholders replaced.

        """
        if isinstance(params, dict):
            return {
                key: self._replace_config(value, config)
                for key, value in params.items()
            }

        if isinstance(params, list):
            return [self._replace_config(item, config) for item in params]

        if isinstance(params, str):
            try:
                return chevron.render(params, {**config, "date": self._format_date})
            except ValueError as e:
                raise ValueError(f"Error rendering template: {str(e)}") from e
        return params

    def replace(self, params: Dict) -> Dict:
        """
        Replace configuration placeholders in the given parameters with actual values.

        This method handles various types of replacements, including:
        - Sensor context configuration
        - Resource configuration
        - Run ID
        - Partition information (if available)
        - Parent asset materialization metadata (if the asset depends on other assets)

        Args:
            params (Dict): The original parameters containing placeholders.

        Returns:
            Dict: A new dictionary with placeholders replaced by actual values.
        """

        replacement_config: Dict[str, Dict[str, str]] = defaultdict(dict)
        if self.context.resources.sensor_context.sensor_context_config:
            replacement_config["sensor"] = (
                self.context.resources.sensor_context.sensor_context_config
            )

        replacement_config["resource"] = self.resource_config
        replacement_config["context"]["run_id"] = self.context.run.run_id.split("-")[0]
        if self.context.has_partition_key:
            replacement_config["context"]["partition_key"] = self.context.partition_key
        try:
            replacement_config["context"][
                "partition_window_start"
            ] = self.context.partition_time_window.start.isoformat()
            replacement_config["context"][
                "partition_window_end"
            ] = self.context.partition_time_window.end.isoformat()
        # Adding CheckError because that's thrown instead of
        # DagsterInvariantViolationError. Not sure why tho
        except (DagsterInvariantViolationError, CheckError):
            pass

        replacement_config["utils"]["now"] = datetime.now().isoformat()

        if self.depends_on:
            self._add_parent_materializations(replacement_config)

        return self._replace_config(params, replacement_config)

    def _add_parent_materializations(
        self, replacement_config: Dict[str, Dict[str, str]]
    ) -> None:
        if not self.depends_on:
            raise ValueError(
                "Parent materializations can only be accessed if a depends_on is set"
            )
        instance = self.context.instance
        for dep in self.depends_on:
            parent_asset_key = dep.replace("/", "_")
            materialization = self._get_materialization(instance, dep, parent_asset_key)
            if materialization:
                replacement_config[parent_asset_key] = (
                    self._materialization_metadata_to_dict(materialization.metadata)
                )

    def _get_materialization(
        self, instance: Any, dep: str, parent_asset_key: str
    ) -> Any:
        if has_partition_def(self.context, parent_asset_key):
            events = instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_key=AssetKey(dep.split("/")),
                    asset_partitions=self.context.asset_partition_keys_for_input(
                        parent_asset_key
                    ),
                ),
                limit=1,
            )
            return events[0].asset_materialization if events else None

        materialization_event = instance.get_latest_materialization_event(
            AssetKey(dep.split("/"))
        )
        return (
            materialization_event.asset_materialization
            if materialization_event
            else None
        )

    @staticmethod
    def _materialization_metadata_to_dict(metadata: Any) -> Dict:
        try:
            return {key: value.text for key, value in metadata.items()}
        except (KeyError, AttributeError) as exc:
            raise ValueError(f"Invalid metadata format: {metadata}") from exc
