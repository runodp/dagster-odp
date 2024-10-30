from typing import Dict, List, Union

from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from dagster._core.definitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from dagster_odp.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_odp.config_manager.models.workflow_model import ScheduleCronTrigger


def _create_cron_schedule(
    trigger_id: str, params: ScheduleCronTrigger, job_id: str
) -> ScheduleDefinition:
    return ScheduleDefinition(
        name=trigger_id,
        cron_schedule=params.schedule_params.cron_schedule,
        job_name=job_id,
    )


def _create_partition_schedule(
    trigger_id: str, params: Dict, job_def: UnresolvedAssetJobDefinition
) -> Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]:
    return build_schedule_from_partitioned_job(
        name=trigger_id,
        job=job_def,
        **params,
    )


def get_schedules(
    wb: WorkflowBuilder, job_defs: List[UnresolvedAssetJobDefinition]
) -> List[Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]]:
    """
    Builds schedule definitions from workflow configuration. Supports two types of
    schedules:

    1. Cron-based schedules: Run jobs at fixed intervals using cron expressions
       (e.g., "@daily", "0 * * * *").

    2. Partition-based schedules: Run jobs based on asset partition definitions
       (e.g., monthly partitions).

    Args:
        wb (WorkflowBuilder): An instance of the WorkflowBuilder class containing
            workflow configuration.
        job_defs (List[UnresolvedAssetJobDefinition]): List of job definitions that
            the schedules will trigger.

    Returns:
        List[Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]]:
            List of created schedule definitions.

    Raises:
        ValueError: If a job referenced by a partition schedule is missing in job_defs.
    """
    schedules: List[
        Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]
    ] = []
    job_id_schedules = wb.job_id_trigger_map("schedule")

    for job_id, schedules_specs in job_id_schedules.items():
        cron_schedules = [
            _create_cron_schedule(spec.trigger_id, spec.params, job_id)
            for spec in schedules_specs
            if spec.params.schedule_kind == "cron"
        ]
        schedules.extend(cron_schedules)

        partition_schedules = [
            (spec.trigger_id, spec.params.schedule_params)
            for spec in schedules_specs
            if spec.params.schedule_kind == "partition"
        ]

        if partition_schedules:
            job_def = next((job for job in job_defs if job.name == job_id), None)
            if not job_def:
                raise ValueError(f"Job {job_id} for partition schedules not found.")

            partition_schedule_defs = [
                _create_partition_schedule(trigger_id, params, job_def)
                for trigger_id, params in partition_schedules
            ]
            schedules.extend(partition_schedule_defs)

    return schedules
