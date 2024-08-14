from unittest.mock import Mock, call, patch

import pytest
import yaml
from dlt.common.pipeline import LoadInfo
from pendulum import DateTime, Timezone
from task_nicely_core.assets.asset_managers.dlt_asset_manager import DLTAssetManager
from task_nicely_core.configs.models.workflow_model import DLTParams, DLTTask

from dagster import AssetExecutionContext, AssetKey, AssetSpec, MaterializeResult


# Shared fixtures
@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock()
    mock_wb.get_assets_with_task_type.return_value = [
        DLTTask(
            asset_key="test_dlt_asset",
            task_type="dlt_to_bq",
            params=DLTParams(
                source_module="test_module",
                source_params={},
                destination={},
                pipeline_params={"dataset_name": "test_dataset"},
            ),
        )
    ]
    return mock_wb


@pytest.fixture
def mock_config_builder():
    mock_cb = Mock()
    mock_cb.get_config.return_value = Mock(
        resources=Mock(
            model_dump=Mock(
                return_value={"dlt": {"project_dir_": "/path/to/dlt_project"}}
            )
        )
    )
    return mock_cb


@pytest.fixture
def dlt_asset_manager(mock_workflow_builder, mock_config_builder):
    with patch(
        "task_nicely_core.assets.asset_managers.base_asset_manager.WorkflowBuilder",
        return_value=mock_workflow_builder,
    ), patch(
        "task_nicely_core.assets.asset_managers.base_asset_manager.ConfigBuilder",
        return_value=mock_config_builder,
    ):
        return DLTAssetManager()


@pytest.fixture
def sample_dlt_task():
    return DLTTask(
        asset_key="test/abc/dlt_asset",
        task_type="dlt_to_bq",
        group_name="test_group",
        params=DLTParams(
            source_module="top_module.test_module.test_function",
            source_params={"source_param": "value"},
            destination={"dest_param": "value"},
            pipeline_params={"dataset_name": "test_dataset"},
        ),
    )


@pytest.fixture
def mock_load_info():
    load_info = Mock()
    load_info.asdict.return_value = {
        "first_run": True,
        "started_at": "2023-05-17T12:00:00",
        "finished_at": "2023-05-17T12:05:00",
        "dataset_name": "test_dataset",
        "destination_name": "bigquery",
        "destination_type": "bigquery",
        "destination_displayable_credentials": "gs://test-bucket",
    }
    return load_info


# Tests
def test_init(dlt_asset_manager, mock_workflow_builder, mock_config_builder):
    assert dlt_asset_manager._wb == mock_workflow_builder
    assert dlt_asset_manager._dagster_config == mock_config_builder.get_config()
    assert dlt_asset_manager._dlt_assets == []
    assert dlt_asset_manager.dlt_asset_names == {}


def test_get_dlt_destination_objects(dlt_asset_manager, tmpdir):
    schema_content = {
        "tables": {
            "table1": {"columns": ["col1", "col2"]},
            "table2": {"columns": ["col3", "col4"]},
            "_hidden_table": {"columns": ["col5"]},
        }
    }
    schema_dir = tmpdir.mkdir("test_module").mkdir("schemas").mkdir("export")
    schema_file = schema_dir.join("test_function.schema.yaml")
    schema_file.write(yaml.dump(schema_content))

    result = dlt_asset_manager._get_dlt_destination_objects(
        str(tmpdir), "test_module", "test_function"
    )

    assert result == ["table1", "table2"]
    assert "_hidden_table" not in result


def test_get_dlt_destination_objects_file_not_found(dlt_asset_manager, tmpdir):
    with pytest.raises(FileNotFoundError):
        dlt_asset_manager._get_dlt_destination_objects(
            str(tmpdir), "non_existent_module", "non_existent_function"
        )


@pytest.mark.parametrize(
    "schema, expected_error",
    [
        ({}, "Schema must contain the 'tables' key."),
        ({"other_key": "some_value"}, "Schema must contain the 'tables' key."),
    ],
)
def test_get_dlt_destination_objects_invalid_schema(
    dlt_asset_manager, schema, expected_error
):
    with pytest.raises(ValueError, match=expected_error):
        dlt_asset_manager._get_dlt_destination_objects(
            "/path/to/dlt", "test_module", "test_function", schema=schema
        )


def test_cast_load_info_metadata(dlt_asset_manager):
    test_data = {
        "datetime": DateTime(2023, 5, 17, 12, 0, 0),
        "timezone": Timezone("UTC"),
        "nested": {
            "datetime": DateTime(2023, 5, 17, 12, 0, 0),
            "list": [DateTime(2023, 5, 17, 12, 0, 0), "string", 123],
        },
    }
    result = dlt_asset_manager._cast_load_info_metadata(test_data)
    assert isinstance(result["datetime"], str)
    assert isinstance(result["timezone"], str)
    assert isinstance(result["nested"]["datetime"], str)
    assert isinstance(result["nested"]["list"][0], str)
    assert result["nested"]["list"][1] == "string"
    assert result["nested"]["list"][2] == 123


@pytest.mark.parametrize(
    "destination_name, expected_key, expected_value",
    [
        ("bigquery", "destination_table_id", "test_dataset.test_asset"),
        (
            "filesystem",
            "destination_file_path",
            "gs://test-bucket/test_dataset/test_asset",
        ),
    ],
)
def test_extract_dlt_metadata(
    dlt_asset_manager, mock_load_info, destination_name, expected_key, expected_value
):
    mock_load_info.asdict.return_value["destination_name"] = destination_name
    result = dlt_asset_manager._extract_dlt_metadata(mock_load_info, "test_asset")
    assert result[expected_key] == expected_value
    expected_base_keys = {
        "first_run",
        "started_at",
        "finished_at",
        "dataset_name",
        "destination_name",
        "destination_type",
    }
    assert all(key in result for key in expected_base_keys)


def test_extract_dlt_metadata_unsupported_destination(
    dlt_asset_manager, mock_load_info
):
    mock_load_info.asdict.return_value["destination_name"] = "unsupported"
    with pytest.raises(ValueError, match="Unsupported destination: unsupported"):
        dlt_asset_manager._extract_dlt_metadata(mock_load_info, "test_asset")


@patch.object(DLTAssetManager, "_extract_dlt_metadata")
def test_materialize_dlt_results(mock_extract_dlt_metadata, dlt_asset_manager):
    op_name = "test_op"
    dlt_asset_manager.dlt_asset_names[op_name] = [
        ["test_op", "table1"],
        ["test_op", "table2"],
    ]
    mock_load_info = Mock()
    mock_extract_dlt_metadata.side_effect = [
        {"key1": "value1", "key2": "value2"},
        {"key3": "value3", "key4": "value4"},
    ]

    results = list(dlt_asset_manager._materialize_dlt_results(op_name, mock_load_info))

    assert len(results) == 2
    assert all(isinstance(result, MaterializeResult) for result in results)
    assert results[0].asset_key == AssetKey(["test_op", "table1"])
    assert results[0].metadata == {"key1": "value1", "key2": "value2"}
    assert results[1].asset_key == AssetKey(["test_op", "table2"])
    assert results[1].metadata == {"key3": "value3", "key4": "value4"}

    mock_extract_dlt_metadata.assert_has_calls(
        [
            call(mock_load_info, "table1"),
            call(mock_load_info, "table2"),
        ]
    )


@pytest.mark.parametrize(
    "task_type, expected_destination",
    [
        ("dlt_to_gcs", "filesystem"),
        ("dlt_to_bq", "bigquery"),
    ],
)
def test_get_destination(dlt_asset_manager, task_type, expected_destination):
    assert dlt_asset_manager._get_destination(task_type) == expected_destination


def test_get_destination_invalid_type(dlt_asset_manager):
    with pytest.raises(ValueError, match="Invalid DLT task type: invalid_type"):
        dlt_asset_manager._get_destination("invalid_type")


@patch("importlib.import_module")
def test_get_source_func(mock_importlib, dlt_asset_manager):
    mock_module = Mock()
    mock_func = Mock()
    mock_module.test_function = mock_func
    mock_importlib.return_value = mock_module

    result = dlt_asset_manager._get_source_func(
        "/path/to/dlt", "test_module", "test_function"
    )

    assert result == mock_func
    mock_importlib.assert_called_once_with("test_module")


@patch.object(DLTAssetManager, "_get_dlt_destination_objects")
@patch.object(DLTAssetManager, "_get_source_func")
@patch.object(DLTAssetManager, "_get_destination")
@patch("task_nicely_core.assets.asset_managers.dlt_asset_manager.multi_asset")
@patch("task_nicely_core.assets.asset_managers.dlt_asset_manager.AssetSpec")
def test_build_asset(
    mock_asset_spec,
    mock_multi_asset,
    mock_get_destination,
    mock_get_source_func,
    mock_get_dlt_destination_objects,
    dlt_asset_manager,
    sample_dlt_task,
):
    mock_get_dlt_destination_objects.return_value = ["table1", "table2"]
    mock_get_source_func.return_value = Mock()
    mock_get_destination.return_value = "bigquery"

    result = dlt_asset_manager._build_asset(sample_dlt_task, "/path/to/dlt")

    assert dlt_asset_manager.dlt_asset_names == {
        "test/abc": [["test", "abc", "table1"], ["test", "abc", "table2"]]
    }

    mock_multi_asset.assert_called_once()
    _, kwargs = mock_multi_asset.call_args
    assert kwargs["name"] == "test__abc"
    assert kwargs["group_name"] == "test_group"
    assert kwargs["required_resource_keys"] == {"sensor_context"}
    assert kwargs["compute_kind"] == "dlt"
    assert len(kwargs["specs"]) == 2

    expected_calls = [
        call(
            key=AssetKey(["test", "abc", "table1"]),
            deps=[AssetKey(["test", "abc"])],
            description="test/abc/table1",
            tags={"dagster/storage_kind": "bigquery"},
            metadata=sample_dlt_task.params.model_dump(),
        ),
        call(
            key=AssetKey(["test", "abc", "table2"]),
            deps=[AssetKey(["test", "abc"])],
            description="test/abc/table2",
            tags={"dagster/storage_kind": "bigquery"},
            metadata=sample_dlt_task.params.model_dump(),
        ),
    ]
    mock_asset_spec.assert_has_calls(expected_calls, any_order=True)

    mock_get_dlt_destination_objects.assert_called_once_with(
        "/path/to/dlt", "top_module.test_module", "test_function"
    )
    mock_get_source_func.assert_called_once_with(
        "/path/to/dlt", "top_module.test_module", "test_function"
    )
    mock_get_destination.assert_called_once_with("dlt_to_bq")

    assert callable(result)


@patch("task_nicely_core.assets.asset_managers.dlt_asset_manager.pipeline")
@patch("task_nicely_core.assets.asset_managers.dlt_asset_manager.ConfigReplacer")
def test_execute_dlt_asset_def(
    mock_config_replacer, mock_pipeline, dlt_asset_manager, sample_dlt_task
):
    mock_context = Mock(spec=AssetExecutionContext)
    mock_replacer = Mock()
    mock_replacer.replace.return_value = {
        "source": {"source_param": "replaced_value"},
        "destination": {"dest_param": "replaced_value"},
        "pipeline": {"dataset_name": "replaced_dataset"},
    }
    mock_config_replacer.return_value = mock_replacer

    mock_pipeline_instance = mock_pipeline.return_value
    mock_load_info = Mock(spec=LoadInfo)
    mock_load_info.asdict.return_value = {
        "dataset_name": "replaced_dataset",
        "destination_name": "bigquery",
        "destination_type": "bigquery",
    }
    mock_pipeline_instance.run.return_value = mock_load_info

    dlt_asset_manager.dlt_asset_names["test/abc"] = [
        ["test", "abc", "table1"],
        ["test", "abc", "table2"],
    ]

    result = list(
        dlt_asset_manager._execute_dlt_asset_def(
            mock_context,
            sample_dlt_task,
            Mock(),
            Mock(),
            "test/abc",
            "dlt_asset",
        )
    )

    assert len(result) == 2
    assert all(isinstance(item, MaterializeResult) for item in result)
    assert result[0].asset_key == AssetKey(["test", "abc", "table1"])
    assert result[1].asset_key == AssetKey(["test", "abc", "table2"])

    mock_config_replacer.assert_called_once()
    mock_replacer.replace.assert_called_once_with(sample_dlt_task.params.model_dump())
    mock_pipeline.assert_called_once()
    mock_pipeline_instance.run.assert_called_once()


@patch(
    "task_nicely_core.assets.asset_managers.dlt_asset_manager.external_asset_from_spec"
)
@patch.object(DLTAssetManager, "_build_asset")
def test_get_assets(mock_build_asset, mock_external_asset_from_spec, dlt_asset_manager):
    # Mock the workflow builder and config
    dlt_asset_manager._wb = Mock()
    dlt_asset_manager._dagster_config = Mock()

    # Create sample DLT assets
    sample_assets = [
        DLTTask(
            asset_key="simple_asset",
            task_type="dlt_to_bq",
            group_name="group1",
            description="Simple asset",
            params=DLTParams(
                source_module="module1",
                source_params={},
                destination={},
                pipeline_params={"dataset_name": "dataset1"},
            ),
        ),
        DLTTask(
            asset_key="parent/nested_asset",
            task_type="dlt_to_bq",
            group_name="group2",
            description="Nested asset",
            params=DLTParams(
                source_module="module2",
                source_params={},
                destination={},
                pipeline_params={"dataset_name": "dataset2"},
            ),
        ),
        DLTTask(
            asset_key="a/b/c/deep_nested",
            task_type="dlt_to_gcs",
            group_name="group3",
            description="Deeply nested asset",
            params=DLTParams(
                source_module="module3",
                source_params={},
                destination={},
                pipeline_params={"dataset_name": "dataset3"},
            ),
        ),
    ]

    dlt_asset_manager._wb.get_assets_with_task_type.return_value = sample_assets
    dlt_asset_manager._dagster_config.resources.model_dump.return_value = {
        "dlt": {"project_dir_": "/path/to/dlt"}
    }

    # Mock the _build_asset method to return a unique mock for each call
    mock_build_asset.side_effect = [Mock(), Mock(), Mock()]

    # Call get_assets
    result = dlt_asset_manager.get_assets()

    # Assertions
    assert len(result) == 5  # 2 external assets + 3 DLT assets

    # Check external assets
    mock_external_asset_from_spec.assert_any_call(
        AssetSpec(
            key=AssetKey(["parent"]), group_name="group2", description="Nested asset"
        )
    )
    mock_external_asset_from_spec.assert_any_call(
        AssetSpec(
            key=AssetKey(["a", "b", "c"]),
            group_name="group3",
            description="Deeply nested asset",
        )
    )

    # Check that external_asset_from_spec is not called for the simple asset
    assert AssetSpec(
        key=AssetKey(["simple_asset"]), group_name="group1", description="Simple asset"
    ) not in [call.args[0] for call in mock_external_asset_from_spec.call_args_list]

    # Check that _build_asset was called for each DLT asset
    assert mock_build_asset.call_count == 3
    for asset, fn_call in zip(sample_assets, mock_build_asset.call_args_list):
        assert fn_call[0][0] == asset
        assert fn_call[0][1] == "/path/to/dlt"
