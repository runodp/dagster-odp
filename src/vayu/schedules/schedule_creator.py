from typing import Dict, List, Optional, Union

from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from dagster._core.definitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from ..config_manager.builders.workflow_builder import WorkflowBuilder
from ..config_manager.models.workflow_model import ScheduleCronTrigger


class ScheduleCreator:
    """
    Reads the schedule configurations from the workflow config files and provides
    methods to build the schedules based on the loaded configurations.

    Attributes:
        _schedules: List of schedule definitions.
    """

    _instance: Optional["ScheduleCreator"] = None

    def __new__(cls) -> "ScheduleCreator":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._schedules: List[
            Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]
        ] = []
        self._wb = WorkflowBuilder()

    def _create_cron_schedule(
        self, trigger_id: str, params: ScheduleCronTrigger, job_id: str
    ) -> ScheduleDefinition:
        return ScheduleDefinition(
            name=trigger_id,
            cron_schedule=params.schedule_params.cron_schedule,
            job_name=job_id,
        )

    def _create_partition_schedule(
        self, trigger_id: str, params: Dict, job_def: UnresolvedAssetJobDefinition
    ) -> Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]:
        return build_schedule_from_partitioned_job(
            name=trigger_id,
            job=job_def,
            **params,
        )

    def get_schedules(
        self, job_defs: List[UnresolvedAssetJobDefinition]
    ) -> List[Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]]:
        """
        Builds schedules based on the trigger specs in the workflow config files.

        Args:
            job_defs (List[UnresolvedAssetJobDefinition]): List of job definitions.

        Returns:
            List of created schedule definitions.

        Raises:
            ValueError: If a job for a trigger is not found.
        """
        if not self._schedules:
            job_id_schedules = self._wb.job_id_trigger_map("schedule")

            for job_id, schedules in job_id_schedules.items():

                self._schedules.extend(
                    [
                        self._create_cron_schedule(spec.trigger_id, spec.params, job_id)
                        for spec in schedules
                        if spec.params.schedule_kind == "cron"
                    ]
                )
                partition_schedules = [
                    (spec.trigger_id, spec.params.schedule_params)
                    for spec in schedules
                    if spec.params.schedule_kind == "partition"
                ]

                if partition_schedules:
                    job_def = next(
                        (job for job in job_defs if job.name == job_id), None
                    )
                    if not job_def:
                        raise ValueError(
                            f"Job {job_id} for partition schedules not found."
                        )
                    self._schedules.extend(
                        [
                            self._create_partition_schedule(spec[0], spec[1], job_def)
                            for spec in partition_schedules
                        ]
                    )

        return self._schedules
