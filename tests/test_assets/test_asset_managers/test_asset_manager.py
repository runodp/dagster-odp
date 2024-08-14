from datetime import datetime
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
from task_nicely_core.configs.builders.config_builder import ConfigBuilder
from task_nicely_core.configs.builders.workflow_builder import WorkflowBuilder
from task_nicely_core.configs.models.workflow_model import (
    CustomWorkflowTask,
    PartitionParams,
    WorkflowParams,
)

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.partition import ScheduleType
from dagster.task_nicely_core.assets.asset_managers.generic_asset_manager import (
    AssetManager,
)


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock(spec=WorkflowBuilder)
    mock_wb.asset_key_partition_map = {
        "asset1": PartitionParams(schedule_type="DAILY", start="2023-01-01"),
        "asset2": PartitionParams(schedule_type="MONTHLY", start="2023-01-01"),
    }
    mock_wb.assets = [
        CustomWorkflowTask(
            asset_key="asset1",
            task_type="custom_task",
            params=WorkflowParams(**{"param1": "value1"}),
            description="Test asset 1",
            group_name="group1",
        ),
        CustomWorkflowTask(
            asset_key="asset2",
            task_type="custom_task",
            params=WorkflowParams(**{"param2": "value2"}),
            description="Test asset 2",
            group_name="group2",
            depends_on=["asset1"],
        ),
    ]
    return mock_wb


@pytest.fixture
def mock_config_builder():
    mock_cb = Mock(spec=ConfigBuilder)
    mock_cb.get_config.return_value = Mock(
        resources=Mock(model_dump=Mock(return_value={"resource1": "value1"})),
        tasks=[
            Mock(
                name="custom_task",
                required_resources=["resource1"],
                compute_kind="custom",
            )
        ],
    )
    return mock_cb


@pytest.fixture
def mock_task_function():
    return Mock(return_value={"result": "success"})


@pytest.fixture
def task_registry(mock_task_function):
    return {"custom_task": mock_task_function}


@pytest.fixture
def asset_manager(mock_workflow_builder, mock_config_builder, task_registry):
    with patch(
        "task_nicely_core.assets.asset_managers.base_asset_manager.WorkflowBuilder",
        return_value=mock_workflow_builder,
    ), patch(
        "task_nicely_core.assets.asset_managers.base_asset_manager.ConfigBuilder",
        return_value=mock_config_builder,
    ), patch(
        "task_nicely_core.assets.asset_managers.asset_manager.task_registry",
        task_registry,
    ):
        yield AssetManager()


def test_init(asset_manager, mock_workflow_builder, mock_config_builder):
    assert asset_manager._wb == mock_workflow_builder
    assert asset_manager._dagster_config == mock_config_builder.get_config()


@pytest.mark.usefixtures("asset_manager")
def test_execute_task_function(asset_manager, mock_task_function):
    result = asset_manager._execute_task_function(
        "custom_task", {"param1": "value1"}, {"resource1": "value1"}
    )

    mock_task_function.assert_called_once_with(param1="value1", resource1="value1")
    assert result == {"result": "success"}


@pytest.mark.usefixtures("asset_manager")
def test_execute_task_function_invalid_task(asset_manager):
    with pytest.raises(
        KeyError, match="Task invalid_task needs to be defined with a @task decorator"
    ):
        asset_manager._execute_task_function("invalid_task", {}, {})


@patch("task_nicely_core.assets.asset_managers.asset_manager.ConfigReplacer")
def test_execute_asset_fn(mock_config_replacer, asset_manager):
    mock_context = Mock(spec=AssetExecutionContext)
    mock_spec = Mock(
        task_type="custom_task",
        params=Mock(model_dump=Mock(return_value={"param1": "value1"})),
        depends_on=["asset1"],
    )
    mock_replacer = Mock()
    mock_replacer.replace.return_value = {"param1": "replaced_value1"}
    mock_config_replacer.return_value = mock_replacer

    with patch.object(
        asset_manager, "_execute_task_function", return_value={"result": "success"}
    ):
        result = asset_manager._execute_asset_fn(mock_context, mock_spec, ["resource1"])

    assert isinstance(result, MaterializeResult)
    assert result.metadata == {"result": "success"}


@pytest.mark.parametrize(
    "asset_key, expected",
    [
        ("asset1", ("asset1", None)),
        ("group/asset1", ("asset1", ["group"])),
        ("group1/group2/asset1", ("asset1", ["group1", "group2"])),
    ],
)
def test_get_asset_key_split(asset_manager, asset_key, expected):
    assert asset_manager._get_asset_key_split(asset_key) == expected


@patch("task_nicely_core.assets.asset_managers.asset_manager.asset")
@patch("task_nicely_core.assets.asset_managers.asset_manager.generate_partition_params")
def test_build_asset(mock_generate_partition_params, mock_asset, asset_manager):
    mock_spec = Mock(
        asset_key="group/asset1",
        task_type="custom_task",
        description="Test asset",
        group_name="test_group",
        depends_on=["asset2", "group2/asset3"],  # Added a dependency with a slash
        params=Mock(model_dump=Mock(return_value={"param1": "value1"})),
    )
    # Add required_resources to the mock task configuration
    mock_task = Mock(
        required_resources=["resource1"],
        compute_kind="custom",
    )
    mock_task.configure_mock(name="custom_task")
    asset_manager._dagster_config.tasks = [
        mock_task,
    ]

    mock_partitions = {"group/asset1": Mock()}
    mock_generate_partition_params.return_value = {
        "start": "2023-01-01",
        "end": "2023-12-31",
        "fmt": "%Y-%m-%d",
        "schedule_type": ScheduleType.DAILY,
    }

    asset_manager._build_asset(mock_spec, mock_partitions)

    mock_asset.assert_called_once()
    _, kwargs = mock_asset.call_args
    assert kwargs["name"] == "asset1"
    assert kwargs["key_prefix"] == ["group"]
    assert kwargs["description"] == "Test asset"
    assert kwargs["required_resource_keys"] == {"resource1", "sensor_context"}
    assert kwargs["deps"] == [
        AssetKey(["asset2"]),
        AssetKey(["group2", "asset3"]),
    ]  # Updated assertion
    assert kwargs["metadata"] == {"param1": "value1"}
    assert kwargs["group_name"] == "test_group"
    assert kwargs["compute_kind"] == "custom"
    assert isinstance(kwargs["partitions_def"], TimeWindowPartitionsDefinition)
    assert kwargs["partitions_def"].fmt == "%Y-%m-%d"
    assert kwargs["partitions_def"].start == datetime(
        2023, 1, 1, tzinfo=ZoneInfo("UTC")
    )
    assert kwargs["partitions_def"].end == datetime(
        2023, 12, 31, tzinfo=ZoneInfo("UTC")
    )


def test_get_assets(asset_manager):
    with patch.object(asset_manager, "_build_asset") as mock_build_asset:
        mock_build_asset.side_effect = lambda spec, _: spec.asset_key

        assets = asset_manager.get_assets()

    assert len(assets) == 2
    assert assets == ["asset1", "asset2"]
    assert asset_manager._assets == assets

    # Verify that _build_asset was called for each asset
    assert mock_build_asset.call_count == 2
    mock_build_asset.assert_any_call(
        asset_manager._wb.assets[0], asset_manager._wb.asset_key_partition_map
    )
    mock_build_asset.assert_any_call(
        asset_manager._wb.assets[1], asset_manager._wb.asset_key_partition_map
    )
