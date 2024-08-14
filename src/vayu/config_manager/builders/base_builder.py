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

    _instance = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, config_data: Optional[Dict] = None) -> None:
        config_path = Path(os.environ.get("CONFIG_PATH", "")).resolve()
        self.load_config(config_data, config_path)

    @abstractmethod
    def load_config(self, config_data: Optional[Dict], config_path: Path) -> None:
        """Load configuration from files."""

    @abstractmethod
    def get_config(self) -> Any:
        """Return the loaded configuration."""
