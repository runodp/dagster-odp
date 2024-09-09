import json
from pathlib import Path
from typing import Any, Dict, Optional

from ..models.config_model import DagsterConfig
from .base_builder import BaseBuilder


class ConfigBuilder(BaseBuilder):
    """
    A configuration builder for Dagster resources.

    Loads and manages Dagster resource configurations from multiple sources:
    1. Default empty configuration
    2. Provided config_data (optional)
    3. dagster_config.json file (if exists at config_path)
    """

    def load_config(
        self, config_data: Optional[Dict], config_path: Optional[Path]
    ) -> None:

        # Start with the provided config_data as the base configuration
        merged_config = DagsterConfig().model_dump()

        if config_data:
            # Merge config_data with the empty configuration
            self._merge_configs(merged_config, config_data)

        if config_path:
            resources_file = config_path / "dagster_config.json"
            if resources_file.exists():
                with resources_file.open("r", encoding="utf-8") as file:
                    file_config = json.load(file)
                    # Merge file_config with the already merged configuration
                    self._merge_configs(merged_config, file_config)

        # Create the DagsterConfig object with the final merged configuration
        self._config = DagsterConfig(**merged_config)

    def get_config(self) -> DagsterConfig:
        return self._config

    @property
    def resource_config_map(self) -> Dict[str, Dict]:
        """
        Returns a dictionary of resource configurations from the dagster config.
        """

        return {r.resource_kind: r.params.model_dump() for r in self._config.resources}

    @property
    def resource_class_map(self) -> Dict[str, Any]:
        """
        Returns a dictionary of resource classes from the dagster config.
        """

        return {r.resource_kind: r.params for r in self._config.resources}
