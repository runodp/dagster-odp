from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext
from pydantic import BaseModel, PrivateAttr


class BaseTask(ABC, BaseModel):
    """
    Abstract base class for implementing tasks in the system.

    This class provides a common interface for all tasks, combining
    Pydantic's data validation with the abstract base class pattern.

    Attributes:
        _resources (Optional[Dict[str, Any]]): Private attribute to store
            resources needed by the task.
        _context (AssetExecutionContext): Private attribute to store the context.
    """

    _resources: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _context: AssetExecutionContext = PrivateAttr()

    def initialize(
        self, context: AssetExecutionContext, required_resources: List
    ) -> None:
        """
        Initialize the task with the execution context and required resources.

        Args:
            context (AssetExecutionContext): The execution context for the task.
            required_resources (List[str]): Resources required by this task.

        Returns:
            None
        """
        self._context = context
        resource_map = {
            resource: getattr(context.resources, resource)
            for resource in required_resources
        }
        self._resources = resource_map

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
