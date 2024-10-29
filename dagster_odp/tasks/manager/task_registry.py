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
    The decorated class must inherit from BaseTask.

    Args:
        task_type (str): Name used to reference this task in workflow configurations
            under the 'task_type' field.
        compute_kind (Optional[str]): Label for the computation type
            (e.g., "python", "sql"). Passed to the Dagster asset definition.
        storage_kind (Optional[str]): Label for the output storage type
            (e.g., "filesystem", "database"). Passed to the Dagster asset definition.
        required_resources (Optional[List[str]]): Resources this task needs. Must be
            registered with @odp_resource and configured in dagster_config.yaml.
            Available in task implementation via self._resources dictionary.

    Example:
        ```python
        @odp_task("bq_query", compute_kind="bigquery", required_resources=["bigquery"])
        class BigQueryTask(BaseTask):
            query: str
            def run(self):
                client = self._resources["bigquery"]
                # Implementation...
        ```
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
