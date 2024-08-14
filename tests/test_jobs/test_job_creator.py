from unittest.mock import Mock, patch

import pytest
from task_nicely_core.configs.builders.workflow_builder import WorkflowBuilder
from task_nicely_core.configs.models.workflow_model import WorkflowJob
from task_nicely_core.jobs.job_creator import JobCreator

from dagster import AssetSelection
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock(spec=WorkflowBuilder)
    mock_wb.jobs = [
        WorkflowJob(
            job_id="test_job_1", triggers=[], asset_selection={"asset1", "asset2"}
        ),
        WorkflowJob(
            job_id="test_job_2", triggers=[], asset_selection={"asset3", "asset4"}
        ),
    ]
    return mock_wb


@pytest.fixture
def job_creator(mock_workflow_builder):
    with patch(
        "task_nicely_core.jobs.job_creator.WorkflowBuilder",
        return_value=mock_workflow_builder,
    ):
        return JobCreator()


def test_get_jobs(job_creator):
    jobs = job_creator.get_jobs()

    assert len(jobs) == 2
    assert all(isinstance(job, UnresolvedAssetJobDefinition) for job in jobs)
    assert jobs[0].name == "test_job_1"
    assert jobs[1].name == "test_job_2"


@patch("task_nicely_core.jobs.job_creator.AssetSelection.from_coercible")
@patch("task_nicely_core.jobs.job_creator.define_asset_job")
def test_job_creation_details(mock_define_asset_job, mock_from_coercible, job_creator):
    # Create a mock AssetSelection that returns itself
    # for required_multi_asset_neighbors
    mock_asset_selection = Mock(spec=AssetSelection)
    mock_asset_selection.required_multi_asset_neighbors.return_value = (
        mock_asset_selection
    )
    mock_from_coercible.return_value = mock_asset_selection

    job_creator.get_jobs()

    assert mock_from_coercible.call_count == 2

    # Check that the calls were made with the correct sets of assets,
    # regardless of order
    call_args_list = [set(call[0][0]) for call in mock_from_coercible.call_args_list]
    assert {"asset1", "asset2"} in call_args_list
    assert {"asset3", "asset4"} in call_args_list

    assert mock_define_asset_job.call_count == 2
    mock_define_asset_job.assert_any_call(
        name="test_job_1", selection=mock_asset_selection
    )
    mock_define_asset_job.assert_any_call(
        name="test_job_2", selection=mock_asset_selection
    )


def test_empty_workflow(mock_workflow_builder):
    mock_workflow_builder.jobs = []
    with patch(
        "task_nicely_core.jobs.job_creator.WorkflowBuilder",
        return_value=mock_workflow_builder,
    ):
        job_creator = JobCreator()

    jobs = job_creator.get_jobs()
    assert len(jobs) == 0
