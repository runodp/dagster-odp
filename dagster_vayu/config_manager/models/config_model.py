# pylint: disable=C0115
from typing import List, Optional

from pydantic import BaseModel, BeforeValidator
from typing_extensions import Annotated


class DbtResourceParams(BaseModel):
    project_dir: str
    profile: str
    sources_file_path_: Optional[str] = None


class DltResourceParams(BaseModel):
    project_dir: str


class BigQueryResourceParams(BaseModel):
    project: str
    location: str


class GcsResourceParams(BaseModel):
    project: str


class SodaResourceParams(BaseModel):
    project_dir: str
    checks_dir: Optional[str] = None


def validate_resource_names(value: str) -> str:
    """resources should be defined in ResourceConfig"""
    if value not in ResourceConfig.model_fields:
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


class ResourceConfig(BaseModel):
    bigquery: Optional[BigQueryResourceParams] = None
    dbt: Optional[DbtResourceParams] = None
    gcs: Optional[GcsResourceParams] = None
    dlt: Optional[DltResourceParams] = None
    soda: Optional[SodaResourceParams] = None


class DagsterConfig(BaseModel):
    resources: ResourceConfig = ResourceConfig()
    tasks: List[TaskConfig] = []
    sensors: List[SensorConfig] = []
