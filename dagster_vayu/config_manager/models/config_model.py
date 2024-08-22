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
