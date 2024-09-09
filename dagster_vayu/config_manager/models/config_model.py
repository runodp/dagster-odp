# pylint: disable=C0115,C0116
from typing import Any, List, Optional, Self

from pydantic import BaseModel, BeforeValidator, field_validator, model_validator
from typing_extensions import Annotated

from ...resources.resource_registry import resource_registry


def validate_resource_names(value: str) -> str:
    """resources should be defined in ResourceConfig"""
    if value not in resource_registry:
        raise ValueError(f"Resource {value} not defined in ResourceConfig")
    return value


class TaskConfig(BaseModel):
    name: str
    required_resources: List[
        Annotated[str, BeforeValidator(validate_resource_names)]
    ] = []
    compute_kind: Optional[str] = None
    storage_kind: Optional[str] = None


class SensorConfig(BaseModel):
    name: str
    required_resources: List[
        Annotated[str, BeforeValidator(validate_resource_names)]
    ] = []


class GenericResource(BaseModel):
    resource_kind: str
    params: Any

    @field_validator("resource_kind")
    @classmethod
    def validate_resource_kind(cls, v: str) -> str:
        if v not in resource_registry:
            raise ValueError(f"Resource kind {v} must be defined.")
        return v

    @model_validator(mode="after")
    def validate_params(self) -> Self:
        self.params = resource_registry[self.resource_kind].model_validate(self.params)
        return self


class DagsterConfig(BaseModel):
    resources: List[GenericResource] = []
    tasks: List[TaskConfig] = []
    sensors: List[SensorConfig] = []

    @model_validator(mode="after")
    def validate_unique_names(self) -> Self:
        task_names = set()
        sensor_names = set()
        resource_kinds = set()

        for task in self.tasks:
            if task.name in task_names:
                raise ValueError(f"Duplicate task name: {task.name}")
            task_names.add(task.name)

        for sensor in self.sensors:
            if sensor.name in sensor_names:
                raise ValueError(f"Duplicate sensor name: {sensor.name}")
            sensor_names.add(sensor.name)

        for resource in self.resources:
            if resource.resource_kind in resource_kinds:
                raise ValueError(f"Duplicate resource kind: {resource.resource_kind}")
            resource_kinds.add(resource.resource_kind)

        return self
