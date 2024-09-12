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
        if config_data:
            self._config = DagsterConfig(**config_data)
            return

        if config_path is None:
            self._config = DagsterConfig()
            return

        resources_file = config_path / "dagster_config.json"
        if not resources_file.exists():
            self._config = DagsterConfig()
            return

        with resources_file.open("r", encoding="utf-8") as file:
            data = json.load(file)
            self._config = DagsterConfig(**data)

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
