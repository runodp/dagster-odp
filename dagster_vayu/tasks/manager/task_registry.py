from typing import Callable, Dict, Type, TypeVar

from .base_task import BaseTask

# Task registry to store all registered tasks
task_registry: Dict[str, Type[BaseTask]] = {}

T = TypeVar("T", bound=BaseTask)


def vayu_task(task_type: str) -> Callable[[Type[T]], Type[T]]:
    """Decorator to register a task"""

    def decorator(cls: Type[T]) -> Type[T]:
        if not issubclass(cls, BaseTask):
            raise TypeError("Task class must inherit from vayu.BaseTask")
        task_registry[task_type] = cls
        return cls

    return decorator
