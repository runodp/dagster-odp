from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Literal, Optional, Type, Union, overload

from ..models.workflow_model import (
    DBTTask,
    DLTTask,
    PartitionParams,
    ScheduleTrigger,
    SensorTrigger,
    SodaCheck,
    TaskTypeUnion,
    Trigger,
    WorkflowConfig,
    WorkflowJob,
    WorkflowPartition,
)
from .base_builder import BaseBuilder


class WorkflowBuilder(BaseBuilder):
    """
    A configuration builder for workflow definitions.

    This class is responsible for loading and managing workflow configurations
    from JSON files. It provides methods to retrieve various aspects of the
    workflow configuration, such as triggers, assets, sensors, jobs, and partitions.

    """

    def load_config(
        self, config_data: Optional[Dict], config_path: Optional[Path]
    ) -> None:
        if config_data:
            self._config = WorkflowConfig.model_validate(config_data)
            return

        if config_path is None:
            self._config = WorkflowConfig()
            return

        workflow_dir = config_path / "workflows"
        if not workflow_dir.exists():
            self._config = WorkflowConfig()
            return

        consolidated_data = self._consolidate_workflow_data(workflow_dir)
        self._config = WorkflowConfig.model_validate(
            consolidated_data, context={"consolidated": True}
        )

    def get_config(self) -> WorkflowConfig:
        return self._config

    def _consolidate_workflow_data(self, workflow_dir: Path) -> Dict:
        merged_data = WorkflowConfig().model_dump()

        config_files: List[Path] = []

        for ext in ["*.json", "*.yml", "*.yaml"]:
            config_files.extend(workflow_dir.glob(ext))

        for file_path in config_files:
            data = self._read_config_file(file_path)
            workflow_config = WorkflowConfig.model_validate(data)
            self._merge_configs(merged_data, workflow_config.model_dump())
        return merged_data

    @property
    def triggers(self) -> List[Dict[str, List[Trigger]]]:
        """
        Retrieves the list of triggers for each job.

        Returns:
            A list of dictionaries mapping job IDs to their corresponding triggers.
        """
        return [{job.job_id: job.triggers} for job in self._config.jobs]

    @property
    def generic_assets(self) -> List[TaskTypeUnion]:
        """
        Retrieves the list of assets from all workflows excluding
        external assets (DBT and DLT).

        Returns:
            A list of WorkflowTask objects.
        """
        return [
            asset
            for asset in self._config.assets
            if not isinstance(asset, (DLTTask, DBTTask))
        ]

    @property
    def sensors(self) -> List[SensorTrigger]:
        """
        Retrieves the list of triggers that are not ScheduleTriggers.

        Returns:
            A list of trigger objects that are not ScheduleTriggers.
        """
        return [
            trigger
            for job in self._config.jobs
            for trigger in job.triggers
            if not isinstance(trigger, ScheduleTrigger)
        ]

    @property
    def jobs(self) -> List[WorkflowJob]:
        """
        Retrieves the list of jobs from all workflows.
        """
        return self._config.jobs

    @property
    def partitions(self) -> List[WorkflowPartition]:
        """
        Retrieves the list of partitions from all workflows.
        """
        return self._config.partitions

    @property
    def soda_checks(self) -> List[SodaCheck]:
        """
        Retrieves the list of soda checks from all workflows.
        """
        return self._config.soda_checks

    def get_assets_with_task_type(self, task_type: Type[TaskTypeUnion]) -> List:
        """
        Retrieves the list of assets that match the given task type.

        Args:
            task_type: The task type to filter the tasks.

        Returns:
            A list of objects that match the given type.
        """
        return [asset for asset in self._config.assets if isinstance(asset, task_type)]

    @property
    def job_ids(self) -> List[str]:
        """
        Retrieves the list of job IDs from all workflows.

        Returns:
            A list of job IDs.
        """
        return [job.job_id for job in self._config.jobs]

    @property
    def asset_key_type_map(self) -> Dict[str, Type[TaskTypeUnion]]:
        """
        Retrieves a dictionary mapping asset keys to their corresponding task types.

        Returns:
            A dictionary with asset keys as keys and task types as values.
        """
        return {asset.asset_key: type(asset) for asset in self._config.assets}

    @property
    def asset_key_partition_map(self) -> Dict[str, PartitionParams]:
        """
        Retrieves a dictionary mapping asset keys to their corresponding
        partition params.

        Returns:
            A dictionary with asset keys as keys and PartitionParams as values.
        """
        return {
            asset_key: partition.params
            for partition in self._config.partitions
            for asset_key in partition.assets
        }

    @overload
    def job_id_trigger_map(
        self, trigger_type: Literal["schedule"]
    ) -> Dict[str, List[ScheduleTrigger]]: ...

    @overload
    def job_id_trigger_map(
        self, trigger_type: Literal["sensor"]
    ) -> Dict[str, List[SensorTrigger]]: ...

    def job_id_trigger_map(
        self, trigger_type: str
    ) -> Union[Dict[str, List[ScheduleTrigger]], Dict[str, List[SensorTrigger]]]:
        """
        Retrieves a dictionary mapping job IDs to their corresponding
        triggers of a specific type.

        Args:
            trigger_type: The type of trigger to filter by.
            Must be either "sensor" or "schedule".

        Returns:
            A dictionary mapping job IDs to their corresponding triggers.

        Raises:
            ValueError: If an invalid trigger type is provided.
        """
        if trigger_type not in ["sensor", "schedule"]:
            raise ValueError(
                f"Invalid trigger type: {trigger_type}. Must be 'sensor' or 'schedule'."
            )

        result: Dict[str, List] = defaultdict(list)
        for job in self._config.jobs:
            for trigger in job.triggers:
                if trigger.trigger_type == trigger_type:
                    result[job.job_id].append(trigger)
        return result
