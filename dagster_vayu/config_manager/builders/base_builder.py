import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Self


class BaseBuilder(ABC):
    """
    An abstract base class for building configuration objects.

    This class implements the Singleton pattern and provides a common interface
    for loading and retrieving configuration data. Subclasses should implement
    the abstract methods to define specific configuration loading behavior.

    Attributes:
        config_path (Path): The path to the configuration files.

    Methods:
        load_config: Abstract method to load configuration from files.
        get_config: Abstract method to retrieve the loaded configuration.
    """

    _instance: Self | None = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self, config_data: Optional[Dict] = None, config_path: Optional[str] = None
    ) -> None:
        if not hasattr(self, "_config_loaded"):
            if config_path:
                path_obj = Path(config_path).resolve()
            else:
                path_obj = Path(os.environ.get("VAYU_CONFIG_PATH", "")).resolve()
            self.load_config(config_data, path_obj)
            self._config_loaded = True

    @abstractmethod
    def load_config(
        self, config_data: Optional[Dict], config_path: Optional[Path]
    ) -> None:
        """Load configuration from files."""

    @abstractmethod
    def get_config(self) -> Any:
        """Return the loaded configuration."""

    def _merge_configs(
        self, merged_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        """
        Merge new_config into merged_config, merging all fields recursively.
        We can assume all keys in new_config are in merged_config.
        """
        for key, value in new_config.items():
            if isinstance(value, list):
                merged_config[key].extend(value)
            elif isinstance(value, dict):
                self._merge_configs(merged_config[key], value)
            else:
                merged_config[key] = value
