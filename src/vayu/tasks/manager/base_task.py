from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pydantic import BaseModel, PrivateAttr


class BaseTask(ABC, BaseModel):
    """
    Abstract base class for implementing tasks in the system.

    This class provides a common interface for all tasks, combining
    Pydantic's data validation with the abstract base class pattern.

    Attributes:
        _resources (Optional[Dict[str, Any]]): Private attribute to store
            resources needed by the task.
    """

    _resources: Optional[Dict[str, Any]] = PrivateAttr(default=None)

    def set_resources(self, resources: Dict[str, Any]) -> None:
        """
        Set the resources for the task.

        Args:
            resources (Dict[str, Any]): Resources to be used by the task.
        """
        self._resources = resources

    @abstractmethod
    def run(self) -> Dict:
        """
        Execute the main logic of the task.

        This method should be implemented by subclasses to perform
        the task's specific operations, such as data transfer or
        transformation.

        Returns:
            Dict: Metadata about the task execution.

        Raises:
            NotImplementedError: If not implemented by a subclass.
        """
        raise NotImplementedError("Subclasses must implement run method")
