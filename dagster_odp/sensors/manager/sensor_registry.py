from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

from .base_sensor import BaseSensor

# Sensor registry to store all registered sensors
sensor_registry: Dict[str, Dict[str, Any]] = {}

T = TypeVar("T", bound=BaseSensor)


def odp_sensor(
    sensor_type: str,
    required_resources: Optional[List[str]] = None,
) -> Callable[[Type[T]], Type[T]]:
    """
    Decorator to register a sensor with additional metadata

    Args:
        task_type (str): The type of the task.
        required_resources (Optional[List[str]]): List of required resources.

    Returns:
        Callable[[Type[T]], Type[T]]: A decorator function.
    """

    def decorator(cls: Type[T]) -> Type[T]:
        if not issubclass(cls, BaseSensor):
            raise TypeError("Sensor class must inherit from odp.BaseSensor")

        sensor_registry[sensor_type] = {
            "class": cls,
            "required_resources": set(required_resources or []),
        }
        return cls

    return decorator
