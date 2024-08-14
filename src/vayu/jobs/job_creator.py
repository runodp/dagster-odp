from typing import List, Optional

from dagster import AssetSelection, define_asset_job
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from ..config_manager.builders.workflow_builder import WorkflowBuilder


class JobCreator:
    """
    A builder class for creating and managing asset jobs.

    Attributes:
        _instance (Optional[JobBuilder]): The singleton instance of JobBuilder.
        wb (WorkflowBuilder): An instance of the WorkflowBuilder class.
        _jobs (List[UnresolvedAssetJobDefinition]): A list of asset job definitions.
    """

    _instance: Optional["JobCreator"] = None

    def __new__(cls) -> "JobCreator":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._wb = WorkflowBuilder()
        self._jobs: List[UnresolvedAssetJobDefinition] = []

    def get_jobs(self) -> List[UnresolvedAssetJobDefinition]:
        """
        Retrieves the asset jobs for all the assets and links them to a job ID.

        Returns:
            List[UnresolvedAssetJobDefinition]: A list of asset jobs.
        """
        if not self._jobs:

            for job in self._wb.jobs:
                selection = AssetSelection.from_coercible(
                    list(job.asset_selection)
                ).required_multi_asset_neighbors()
                self._jobs.append(
                    define_asset_job(name=job.job_id, selection=selection)
                )

        return self._jobs
