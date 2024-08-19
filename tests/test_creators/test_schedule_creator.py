from unittest.mock import Mock, patch

import pytest
from dagster import ScheduleDefinition
from dagster._core.definitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.workflow_model import (
    ScheduleCronParams,
    ScheduleCronTrigger,
    SchedulePartitionTrigger,
    ScheduleTrigger,
)
from dagster_vayu.creators.schedule_creator import (
    _create_cron_schedule,
    _create_partition_schedule,
    get_schedules,
)


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock(spec=WorkflowBuilder)
    mock_wb.job_id_trigger_map.return_value = {
        "job1": [
            ScheduleTrigger(
                trigger_id="cron_schedule",
                trigger_type="schedule",
                params=ScheduleCronTrigger(
                    schedule_kind="cron",
                    schedule_params=ScheduleCronParams(cron_schedule="0 * * * *"),
                ),
            ),
            ScheduleTrigger(
                trigger_id="partition_schedule",
                trigger_type="schedule",
                params=SchedulePartitionTrigger(
                    schedule_kind="partition", schedule_params={}
                ),
            ),
        ],
        "job2": [
            ScheduleTrigger(
                trigger_id="another_cron_schedule",
                trigger_type="schedule",
                params=ScheduleCronTrigger(
                    schedule_kind="cron",
                    schedule_params=ScheduleCronParams(cron_schedule="0 0 * * *"),
                ),
            ),
        ],
    }
    return mock_wb


@pytest.fixture
def job_defs():
    job1 = Mock(spec=UnresolvedAssetJobDefinition)
    job1.configure_mock(name="job1")
    job2 = Mock(spec=UnresolvedAssetJobDefinition)
    job2.configure_mock(name="job2")
    return [job1, job2]


@pytest.fixture
def mock_schedule_builders():
    def create_schedule_mock(name, spec):
        mock = Mock(spec=spec)
        mock.configure_mock(name=name)
        return mock

    with (
        patch(
            "dagster_vayu.creators.schedule_creator.ScheduleDefinition"
        ) as mock_schedule_def,
        patch(
            "dagster_vayu.creators.schedule_creator.build_schedule_from_partitioned_job"
        ) as mock_build,
    ):
        mock_schedule_def.side_effect = lambda **kwargs: create_schedule_mock(
            kwargs["name"], ScheduleDefinition
        )
        mock_build.side_effect = lambda **kwargs: create_schedule_mock(
            kwargs["name"], UnresolvedPartitionedAssetScheduleDefinition
        )
        yield mock_schedule_def, mock_build


def test_create_cron_schedule():
    trigger_id = "test_cron"
    params = ScheduleCronTrigger(
        schedule_kind="cron",
        schedule_params=ScheduleCronParams(cron_schedule="0 * * * *"),
    )
    job_id = "test_job"

    schedule = _create_cron_schedule(trigger_id, params, job_id)

    assert isinstance(schedule, ScheduleDefinition)
    assert schedule.name == "test_cron"
    assert schedule.cron_schedule == "0 * * * *"
    assert schedule.job_name == "test_job"


def test_create_partition_schedule():
    trigger_id = "test_partition"
    params = {}
    job_def = Mock(spec=UnresolvedAssetJobDefinition)

    with patch(
        "dagster_vayu.creators.schedule_creator.build_schedule_from_partitioned_job"
    ) as mock_build:
        _create_partition_schedule(trigger_id, params, job_def)
        mock_build.assert_called_once_with(name="test_partition", job=job_def)


def test_get_schedules(mock_workflow_builder, job_defs, mock_schedule_builders):
    mock_schedule_def, mock_build = mock_schedule_builders

    schedules = get_schedules(mock_workflow_builder, job_defs)

    assert len(schedules) == 3
    assert all(isinstance(schedule, Mock) for schedule in schedules)
    assert [schedule.name for schedule in schedules] == [
        "cron_schedule",
        "partition_schedule",
        "another_cron_schedule",
    ]
    assert mock_schedule_def.call_count == 2
    assert mock_build.call_count == 1


def test_get_schedules_job_not_found(mock_workflow_builder):
    job2 = Mock(spec=UnresolvedAssetJobDefinition)
    job2.configure_mock(name="job2")
    job_defs = [job2]

    with pytest.raises(ValueError, match="Job job1 for partition schedules not found."):
        get_schedules(mock_workflow_builder, job_defs)
