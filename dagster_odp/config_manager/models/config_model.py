# pylint: disable=C0115,C0116
from typing import Any, List

from pydantic import BaseModel, field_validator, model_validator
from typing_extensions import Self

from ...resources.resource_registry import resource_registry


def validate_resource_names(value: str) -> str:
    """resources should be defined in ResourceConfig"""
    if value not in resource_registry:
        raise ValueError(f"Resource {value} not defined in ResourceConfig")
    return value


class GenericResource(BaseModel):
    resource_kind: str
    params: Any = {}

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

    @model_validator(mode="after")
    def validate_unique_resources(self) -> Self:
        resource_kinds = set()

        for resource in self.resources:
            if resource.resource_kind in resource_kinds:
                raise ValueError(f"Duplicate resource kind: {resource.resource_kind}")
            resource_kinds.add(resource.resource_kind)

        return self
