from typing import List

from dagster import AssetSelection, define_asset_job
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from dagster_odp.config_manager.builders.workflow_builder import WorkflowBuilder


def get_jobs(wb: WorkflowBuilder) -> List[UnresolvedAssetJobDefinition]:
    """
    Creates asset jobs based on the job definitions in the WorkflowBuilder.

    Args:
        wb (WorkflowBuilder): An instance of the WorkflowBuilder class.

    Returns:
        List[UnresolvedAssetJobDefinition]: A list of defined asset jobs.
    """
    jobs = []
    for job in wb.jobs:
        selection = AssetSelection.from_coercible(
            list(job.asset_selection)
        ).required_multi_asset_neighbors()
        jobs.append(
            define_asset_job(
                name=job.job_id, selection=selection, description=job.description
            )
        )
    return jobs
