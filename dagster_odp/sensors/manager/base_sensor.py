from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, Union

from dagster import RunRequest, SensorEvaluationContext, SkipReason
from pydantic import BaseModel, PrivateAttr


class BaseSensor(ABC, BaseModel):
    """
    Abstract base class for implementing sensors in the system.

    This class provides a common interface for all sensors, combining
    Pydantic's data validation with the abstract base class pattern.

    Attributes:
        _context: The sensor evaluation context from Dagster
        _cursor: The current cursor value for tracking progress
        _resources: Available resources from the context
    """

    _context: SensorEvaluationContext = PrivateAttr()
    _cursor: Optional[str] = PrivateAttr()
    _resources: Dict[str, Any] = PrivateAttr()

    def evaluate(
        self, context: SensorEvaluationContext
    ) -> Iterator[Union[SkipReason, RunRequest]]:
        """
        Initialize context and execute the sensor's evaluation logic.

        Args:
            context (SensorEvaluationContext)

        Returns:
            Iterator[SkipReason | RunRequest]: One or more evaluation results.
        """
        self._context = context
        self._cursor = context.cursor
        self._resources = context.resources._asdict()
        return self.run()

    @abstractmethod
    def run(self) -> Iterator[Union[SkipReason, RunRequest]]:
        """
        Execute the sensor's evaluation logic. This method should be implemented
        by subclasses to define the specific sensor behavior.

        Yields either:
        - SkipReason: When no action is needed (e.g., no new data)
        - RunRequest: When a run should be triggered, including:
            - A unique run_key to deduplicate runs
            - run_config with context for downstream assets

        The following attributes are available within this method:
        - self._context: Full access to the Dagster context if needed
        - self._cursor: Current cursor value for tracking progress
        - self._resources: Available resources from the context

        Returns:
            Iterator[SkipReason | RunRequest]: One or more evaluation results.

        Example:
            >>> def run(self):
            >>>     client = self._resources.gcs  # Access resources directly
            >>>     if not new_data:
            >>>         yield SkipReason("No new data")
            >>>     for item in new_data:
            >>>         yield RunRequest(
            >>>             run_key=f"process_{item.id}",
            >>>             run_config={"data": {"id": item.id}}
            >>>         )
            >>>     self._context.update_cursor(new_data[-1])  # Update cursor if needed
        """
        raise NotImplementedError("Subclasses must implement run method")
