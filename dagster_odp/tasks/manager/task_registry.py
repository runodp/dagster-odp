from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

from .base_task import BaseTask

# Task registry to store all registered tasks
task_registry: Dict[str, Dict[str, Any]] = {}

T = TypeVar("T", bound=BaseTask)


def odp_task(
    task_type: str,
    compute_kind: Optional[str] = None,
    storage_kind: Optional[str] = None,
    required_resources: Optional[List[str]] = None,
) -> Callable[[Type[T]], Type[T]]:
    """
    Decorator to register a task with additional metadata.

    Args:
        task_type (str): The type of the task.
        compute_kind (Optional[str]): The compute kind for the task.
        storage_kind (Optional[str]): The storage kind for the task.
        required_resources (Optional[List[str]]): List of required resources.

    Returns:
        Callable[[Type[T]], Type[T]]: A decorator function.
    """

    def decorator(cls: Type[T]) -> Type[T]:
        if not issubclass(cls, BaseTask):
            raise TypeError("Task class must inherit from odp.BaseTask")

        task_registry[task_type] = {
            "class": cls,
            "compute_kind": compute_kind,
            "storage_kind": storage_kind,
            "required_resources": required_resources or [],
        }
        return cls

    return decorator
