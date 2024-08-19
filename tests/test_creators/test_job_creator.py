from unittest.mock import Mock, patch

import pytest
from dagster import AssetSelection
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.workflow_model import WorkflowJob
from dagster_vayu.creators.job_creator import get_jobs


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


def test_get_jobs(mock_workflow_builder):
    jobs = get_jobs(mock_workflow_builder)
    assert len(jobs) == 2
    assert all(isinstance(job, UnresolvedAssetJobDefinition) for job in jobs)
    assert jobs[0].name == "test_job_1"
    assert jobs[1].name == "test_job_2"


@patch("dagster_vayu.creators.job_creator.AssetSelection.from_coercible")
@patch("dagster_vayu.creators.job_creator.define_asset_job")
def test_job_creation_details(
    mock_define_asset_job, mock_from_coercible, mock_workflow_builder
):
    # Create a mock AssetSelection that returns itself
    # for required_multi_asset_neighbors
    mock_asset_selection = Mock(spec=AssetSelection)
    mock_asset_selection.required_multi_asset_neighbors.return_value = (
        mock_asset_selection
    )
    mock_from_coercible.return_value = mock_asset_selection

    get_jobs(mock_workflow_builder)

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
    jobs = get_jobs(mock_workflow_builder)
    assert len(jobs) == 0
