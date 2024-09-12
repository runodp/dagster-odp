from unittest.mock import Mock, patch

import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.partition import ScheduleType

from dagster_vayu.config_manager.models.workflow_model import (
    GenericTask,
    PartitionParams,
)
from dagster_vayu.creators.asset_creators.generic_asset_creator import (
    GenericAssetCreator,
)
from dagster_vayu.tasks.manager.base_task import BaseTask


@pytest.fixture
def mock_workflow_builder(task_registry):
    mock_wb = Mock()
    mock_wb.asset_key_partition_map = {
        "asset1": PartitionParams(schedule_type="DAILY", start="2023-01-01"),
        "asset2": PartitionParams(schedule_type="MONTHLY", start="2023-01-01"),
    }
    with patch(
        "dagster_vayu.config_manager.models.workflow_model.task_registry", task_registry
    ):
        mock_wb.generic_assets = [
            GenericTask(
                asset_key="asset1",
                task_type="custom_task",
                params={"param1": "value1"},
                description="Test asset 1",
                group_name="group1",
            ),
            GenericTask(
                asset_key="asset2",
                task_type="custom_task",
                params={"param1": "value2"},
                description="Test asset 2",
                group_name="group2",
                depends_on=["asset1"],
            ),
        ]
    return mock_wb


@pytest.fixture
def mock_config_builder():
    mock_cb = Mock()
    mock_cb.get_config.return_value = Mock(
        resources=Mock(model_dump=Mock(return_value={"resource1": "value1"}))
    )
    return mock_cb


@pytest.fixture
def task_registry():
    class CustomTask(BaseTask):
        param1: str

        def run(self):
            return {"result": f"Executed with {self.param1}"}

    return {
        "custom_task": {
            "class": CustomTask,
            "compute_kind": "custom",
            "storage_kind": None,
            "required_resources": ["resource1"],
        }
    }


@pytest.fixture
def generic_asset_creator(mock_workflow_builder, mock_config_builder, task_registry):
    with (
        patch(
            "dagster_vayu.creators.asset_creators.base_asset_creator.WorkflowBuilder",
            return_value=mock_workflow_builder,
        ),
        patch(
            "dagster_vayu.creators.asset_creators.base_asset_creator.ConfigBuilder",
            return_value=mock_config_builder,
        ),
        patch(
            "dagster_vayu.creators.asset_creators.generic_asset_creator.task_registry",
            task_registry,
        ),
    ):
        yield GenericAssetCreator()


@patch(
    "dagster_vayu.creators.asset_creators.generic_asset_creator.AssetExecutionContext"
)
def test_init_and_execute(
    mock_asset_execution_context,
    generic_asset_creator,
    mock_workflow_builder,
    mock_config_builder,
):
    assert generic_asset_creator._wb == mock_workflow_builder
    assert generic_asset_creator._dagster_config == mock_config_builder.get_config()

    mock_context = mock_asset_execution_context.return_value

    result = generic_asset_creator._execute_task_function(
        "custom_task", {"param1": "value1"}, mock_context
    )
    assert result == {"result": "Executed with value1"}


@patch("dagster_vayu.creators.asset_creators.generic_asset_creator.ConfigParamReplacer")
def test_materialize_asset(mock_config_param_replacer, generic_asset_creator):
    mock_context = Mock(spec=AssetExecutionContext)
    mock_spec = Mock(
        task_type="custom_task",
        params=Mock(model_dump=Mock(return_value={"param1": "value1"})),
        depends_on=["asset1"],
    )
    mock_replacer = Mock()
    mock_replacer.replace.return_value = {"param1": "replaced_value1"}
    mock_config_param_replacer.return_value = mock_replacer

    with patch.object(
        generic_asset_creator,
        "_execute_task_function",
        return_value={"result": "success"},
    ):
        result = generic_asset_creator._materialize_asset(mock_context, mock_spec)

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
def test_get_asset_key_split(generic_asset_creator, asset_key, expected):
    assert generic_asset_creator._get_asset_key_split(asset_key) == expected


@patch("dagster_vayu.creators.asset_creators.generic_asset_creator.asset")
@patch(
    "dagster_vayu.creators.asset_creators.generic_asset_creator.generate_partition_params"
)
def test_build_asset(mock_generate_partition_params, mock_asset, generic_asset_creator):
    mock_spec = Mock(
        asset_key="group/asset1",
        task_type="custom_task",
        description="Test asset",
        group_name="test_group",
        depends_on=["asset2", "group2/asset3"],
        params=Mock(model_dump=Mock(return_value={"param1": "value1"})),
    )
    mock_task = Mock(required_resources=["resource1"], compute_kind="custom")
    mock_task.configure_mock(name="custom_task")
    generic_asset_creator._dagster_config.tasks = [mock_task]

    mock_partitions = {"group/asset1": Mock()}
    mock_generate_partition_params.return_value = {
        "start": "2023-01-01",
        "end": "2023-12-31",
        "fmt": "%Y-%m-%d",
        "schedule_type": ScheduleType.DAILY,
    }

    generic_asset_creator._build_asset(mock_spec, mock_partitions)

    mock_asset.assert_called_once()
    _, kwargs = mock_asset.call_args
    assert kwargs["name"] == "asset1"
    assert kwargs["key_prefix"] == ["group"]
    assert kwargs["description"] == "Test asset"
    assert kwargs["required_resource_keys"] == {"resource1", "sensor_context"}
    assert kwargs["deps"] == [AssetKey(["asset2"]), AssetKey(["group2", "asset3"])]
    assert kwargs["metadata"] == {"param1": "value1"}
    assert kwargs["group_name"] == "test_group"
    assert kwargs["compute_kind"] == "custom"
    assert isinstance(kwargs["partitions_def"], TimeWindowPartitionsDefinition)


def test_get_assets(generic_asset_creator):
    with patch.object(generic_asset_creator, "_build_asset") as mock_build_asset:
        mock_build_asset.side_effect = lambda spec, _: spec.asset_key
        assets = generic_asset_creator.get_assets()

    assert assets == ["asset1", "asset2"]
    assert generic_asset_creator._assets == assets
    assert mock_build_asset.call_count == 2
    mock_build_asset.assert_any_call(
        generic_asset_creator._wb.generic_assets[0],
        generic_asset_creator._wb.asset_key_partition_map,
    )
    mock_build_asset.assert_any_call(
        generic_asset_creator._wb.generic_assets[1],
        generic_asset_creator._wb.asset_key_partition_map,
    )
