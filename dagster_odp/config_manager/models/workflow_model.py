# pylint: disable=C0115,C0116
from typing import Any, Dict, List, Literal, Optional, Set, Union

from croniter import croniter
from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    ValidationInfo,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated, Self, TypeAlias

from ...sensors.manager.sensor_registry import sensor_registry
from ...tasks.manager.task_registry import task_registry


class WorkflowTrigger(BaseModel):
    trigger_id: str
    description: Optional[str] = ""


class ScheduleCronParams(BaseModel):
    cron_schedule: str

    @field_validator("cron_schedule")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError("not a valid cron schedule")
        return v


class ScheduleCronTrigger(BaseModel):
    schedule_params: ScheduleCronParams
    schedule_kind: Literal["cron"]


class PartitionParams(BaseModel):
    schedule_type: Optional[
        Union[
            Literal["HOURLY"], Literal["DAILY"], Literal["WEEKLY"], Literal["MONTHLY"]
        ]
    ] = None
    start: str
    end: Optional[str] = None
    timezone: Optional[str] = None
    fmt: Optional[str] = None
    minute_offset: Optional[int] = None
    hour_offset: Optional[int] = None
    day_offset: Optional[int] = None
    end_offset: Optional[int] = None
    cron_schedule: Optional[str] = None

    @model_validator(mode="after")
    def validate_schedule(self) -> Self:
        if self.cron_schedule is not None:
            if (
                self.schedule_type is not None
                or self.minute_offset is not None
                or self.hour_offset is not None
                or self.day_offset is not None
            ):
                raise ValueError(
                    "If cron_schedule argument is provided, then schedule_type, "
                    "minute_offset, hour_offset, and day_offset can't also be provided"
                )
        elif self.schedule_type is None:
            raise ValueError("One of schedule_type and cron_schedule must be provided")
        return self


class WorkflowPartition(BaseModel):
    assets: List[str]
    params: PartitionParams


class SchedulePartitionTrigger(BaseModel):
    schedule_params: Dict[str, Any] = {}
    schedule_kind: Literal["partition"]


class ScheduleTrigger(WorkflowTrigger):
    trigger_type: Literal["schedule"]
    params: Union[ScheduleCronTrigger, SchedulePartitionTrigger] = Field(
        ..., discriminator="schedule_kind"
    )


class GenericSensor(BaseModel):
    sensor_kind: str
    sensor_params: Any

    @field_validator("sensor_kind")
    @classmethod
    def validate_sensor_kind(cls, v: str) -> str:
        if v not in sensor_registry:
            raise ValueError(f"Sensor type {v} must be a defined sensor type.")
        return v

    @model_validator(mode="after")
    def validate_params(self) -> Self:
        self.sensor_params = sensor_registry[self.sensor_kind]["class"].model_validate(
            self.sensor_params
        )
        return self


class SensorTrigger(WorkflowTrigger):
    params: GenericSensor
    trigger_type: Literal["sensor"]


class WorkflowParams(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )


class DLTParams(WorkflowParams):
    schema_file_path: str
    source_module: str
    source_params: Dict[str, Any]
    destination: str
    destination_params: Dict[str, Any] = {}
    pipeline_params: Dict[str, Any] = {}
    run_params: Dict[str, Any] = {}

    @field_validator("pipeline_params")
    @classmethod
    def validate_pipeline(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if "dataset_name" not in v:
            raise ValueError("dataset_name is required")
        return v


class DBTTaskParams(WorkflowParams):
    selection: str
    dbt_vars: Optional[Dict[str, str]] = {}


class SodaCheck(BaseModel):
    asset_key: str
    check_file_path: str
    blocking: Optional[bool] = True
    description: Optional[str] = ""
    data_source: str


class WorkflowTask(BaseModel):
    asset_key: str
    params: Any


class GenericTask(WorkflowTask):
    task_type: str
    description: Optional[str] = ""
    group_name: Optional[str] = None
    depends_on: Optional[List[str]] = None

    @field_validator("task_type")
    @classmethod
    def validate_task_type(cls, v: str) -> str:
        if v not in task_registry:
            raise ValueError(f"Task type {v} must be a defined task type.")
        return v

    @model_validator(mode="after")
    def validate_params(self) -> Self:
        self.params = task_registry[self.task_type]["class"].model_validate(self.params)
        return self


class DBTTask(WorkflowTask):
    params: DBTTaskParams
    task_type: Literal["dbt"]
    description: Literal[None] = None
    depends_on: Literal[None] = None
    group_name: Literal[None] = None


class DLTTask(WorkflowTask):
    params: DLTParams
    task_type: Literal["dlt"]
    description: Optional[str] = ""
    group_name: Optional[str] = None
    depends_on: Literal[None] = None

    @field_validator("asset_key")
    @classmethod
    def validate_asset_key(cls, v: str) -> str:
        """
        Validates that the asset key contains at least one forward slash.
        This ensures proper specification of both external asset name and resource name.
        """
        if "/" not in v:
            raise ValueError(
                f"DLT asset key '{v}' must contain at least one '/' to specify both "
                "external asset name and resource name (e.g., 'github/pull_requests')"
            )
        return v


Trigger = Annotated[
    Union[ScheduleTrigger, SensorTrigger], Field(discriminator="trigger_type")
]


class WorkflowJob(BaseModel):
    job_id: str
    triggers: List[Trigger] = []
    asset_selection: Set[str]
    description: Optional[str] = ""

    @model_validator(mode="after")
    def validate_partition_schedule(self) -> Self:
        """
        Validates that only one partition schedule is allowed per job.
        """
        partition_schedules = [
            trigger
            for trigger in self.triggers
            if trigger.trigger_type == "schedule"
            and trigger.params.schedule_kind == "partition"
        ]

        if len(partition_schedules) > 1:
            raise ValueError("Only one partition schedule is allowed")

        return self


def task_discriminator(v: Any) -> str:
    if isinstance(v, dict):
        task_type = v["task_type"]
    else:
        task_type = v.task_type

    if task_type in ["dbt", "dlt"]:
        return task_type
    return "generic"


Asset = Annotated[
    Union[
        Annotated[DLTTask, Tag("dlt")],
        Annotated[DBTTask, Tag("dbt")],
        Annotated[GenericTask, Tag("generic")],
    ],
    Discriminator(task_discriminator),
]

TaskTypeUnion: TypeAlias = Union[DLTTask, DBTTask, GenericTask]


class WorkflowConfig(BaseModel):
    jobs: List[WorkflowJob] = []
    assets: List[Asset] = []
    partitions: List[WorkflowPartition] = []
    soda_checks: List[SodaCheck] = []

    @model_validator(mode="before")
    @classmethod
    def validate_unique_ids(cls, values: Any, info: ValidationInfo) -> Any:
        """
        Validates that the job_ids, trigger_ids, and asset_keys
        of all workflows are unique.
        """

        context = info.context
        if not context or context.get("consolidated", False) is False:
            return values

        unique_ids = set()

        for job in values["jobs"]:
            if job["job_id"] in unique_ids:
                raise ValueError(
                    f"Duplicate job_id: {job['job_id']}"
                    " job ids, trigger ids, and asset keys must be unique."
                )
            unique_ids.add(job["job_id"])

            for trigger in job["triggers"]:
                if trigger["trigger_id"] in unique_ids:
                    raise ValueError(
                        f"Duplicate trigger_id found: {trigger['trigger_id']}"
                        " job ids, trigger ids, and asset keys must be unique."
                    )
                unique_ids.add(trigger["trigger_id"])

        for asset in values["assets"]:
            if asset["asset_key"] in unique_ids:
                raise ValueError(
                    f"Duplicate asset key found: {asset['asset_key']}"
                    " job ids, trigger ids, and asset keys must be unique."
                )
            unique_ids.add(asset["asset_key"])

        return values

    @model_validator(mode="after")
    def validate_partitioned_assets(self, info: ValidationInfo) -> Self:
        """
        Validates that the assets with partitions are defined,
        and that each asset has a single partition.
        """
        context = info.context
        if not context or context.get("consolidated", False) is False:
            return self

        partitioned_assets = set()
        asset_keys = {asset.asset_key for asset in self.assets}

        for partition in self.partitions:
            for asset in partition.assets:
                # Check if the asset is defined under assets
                if asset not in asset_keys:
                    raise ValueError(
                        f"Asset '{asset}' in partition is not defined as a asset."
                    )

                # Check for duplicate assets across partitions
                if asset in partitioned_assets:
                    raise ValueError(
                        f"Asset '{asset}' is defined in multiple partitions."
                        f" Each asset can only have one partition."
                    )

                partitioned_assets.add(asset)

        return self
