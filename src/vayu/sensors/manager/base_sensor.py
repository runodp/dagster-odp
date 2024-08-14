from abc import ABC, abstractmethod
from typing import Iterator

from pydantic import BaseModel

from dagster import RunRequest, SensorEvaluationContext, SkipReason


class BaseSensor(ABC, BaseModel):
    """
    Abstract base class for implementing sensors in the system.

    This class provides a common interface for all sensors, combining
    Pydantic's data validation with the abstract base class pattern.
    """

    @abstractmethod
    def run(
        self, context: SensorEvaluationContext
    ) -> Iterator[SkipReason | RunRequest]:
        """
        Execute the main logic of the sensor.

        This method should be implemented by subclasses to define the sensor's
        behavior. It should yield either SkipReason or RunRequest objects
        based on the sensor's evaluation of the current state.

        Args:
            context (SensorEvaluationContext)

        Returns:
            Iterator[SkipReason | RunRequest]

        Raises:
            NotImplementedError: If not implemented by a subclass.
        """
        raise NotImplementedError("Subclasses must implement run method")
