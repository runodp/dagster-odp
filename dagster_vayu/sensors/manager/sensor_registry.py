from typing import Callable, Dict, Type, TypeVar

from .base_sensor import BaseSensor

# Sensor registry to store all registered sensors
sensor_registry: Dict[str, Type[BaseSensor]] = {}

T = TypeVar("T", bound=BaseSensor)


def sensor_trigger(sensor_type: str) -> Callable[[Type[T]], Type[T]]:
    """Decorator to register a task"""

    def decorator(cls: Type[T]) -> Type[T]:
        if not issubclass(cls, BaseSensor):
            raise TypeError("Sensor class must inherit from vayu.BaseSensor")
        sensor_registry[sensor_type] = cls
        return cls

    return decorator
