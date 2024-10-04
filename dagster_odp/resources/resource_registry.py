from typing import Callable, Dict, Type, TypeVar

from dagster import ConfigurableResource

# Resource registry to store all registered resources
resource_registry: Dict[str, Type[ConfigurableResource]] = {}

T = TypeVar("T", bound=ConfigurableResource)


def odp_resource(resource_type: str) -> Callable[[Type[T]], Type[T]]:
    """Decorator to register a task"""

    def decorator(cls: Type[T]) -> Type[T]:
        if not issubclass(cls, ConfigurableResource):
            raise TypeError(
                "Resource class must inherit from dagster.ConfigurableResource"
            )
        resource_registry[resource_type] = cls
        return cls

    return decorator
