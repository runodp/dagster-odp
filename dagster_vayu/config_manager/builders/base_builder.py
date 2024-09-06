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
        if config_path:
            path_obj = Path(config_path).resolve()
        else:
            path_obj = Path(os.environ.get("VAYU_CONFIG_PATH", "")).resolve()
        self.load_config(config_data, path_obj)

    @abstractmethod
    def load_config(
        self, config_data: Optional[Dict], config_path: Optional[Path]
    ) -> None:
        """Load configuration from files."""

    @abstractmethod
    def get_config(self) -> Any:
        """Return the loaded configuration."""
