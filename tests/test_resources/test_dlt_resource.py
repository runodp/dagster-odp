import os
from unittest.mock import Mock, mock_open, patch

import pytest
from dagster import AssetKey, MaterializeResult
from dlt.common.pipeline import LoadInfo
from pendulum import DateTime, Timezone

from dagster_vayu.config_manager.models.workflow_model import DLTParams, DLTTask
from dagster_vayu.resources.dlt_resource import VayuDltResource


@pytest.fixture
def vayu_dlt_resource():
    return VayuDltResource(project_dir="/path/to/dlt_project")


@pytest.fixture
def sample_dlt_task():
    return DLTTask(
        asset_key="test/abc/dlt_asset",
        task_type="dlt",
        params=DLTParams(
            source_module="top_module.test_module.test_function",
            source_params={"source_param": "value"},
            destination="bigquery",
            destination_params={"dest_param": "value"},
            pipeline_params={"dataset_name": "test_dataset"},
        ),
    )


@pytest.fixture
def mock_load_info():
    load_info = Mock(spec=LoadInfo)
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


def test_get_source_func(vayu_dlt_resource):
    with patch("importlib.import_module") as mock_importlib:
        mock_module = Mock()
        mock_func = Mock()
        mock_module.test_function = mock_func
        mock_importlib.return_value = mock_module

        result = vayu_dlt_resource._get_source_func("test_module", "test_function")

        assert result == mock_func
        mock_importlib.assert_called_once_with("test_module")


def test_cast_load_info_metadata(vayu_dlt_resource):
    test_data = {
        "datetime": DateTime(2023, 5, 17, 12, 0, 0),
        "timezone": Timezone("UTC"),
        "nested": {
            "datetime": DateTime(2023, 5, 17, 12, 0, 0),
            "list": [DateTime(2023, 5, 17, 12, 0, 0), "string", 123],
        },
    }
    result = vayu_dlt_resource._cast_load_info_metadata(test_data)
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
            "destination_file_uri",
            "gs://test-bucket/test_dataset/test_asset",
        ),
    ],
)
def test_extract_dlt_metadata(
    vayu_dlt_resource, mock_load_info, destination_name, expected_key, expected_value
):
    mock_load_info.asdict.return_value["destination_name"] = destination_name
    result = vayu_dlt_resource.extract_dlt_metadata(mock_load_info, "test_asset")
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
    vayu_dlt_resource, mock_load_info
):
    mock_load_info.asdict.return_value["destination_name"] = "unsupported"
    with pytest.raises(ValueError, match="Unsupported destination: unsupported"):
        vayu_dlt_resource.extract_dlt_metadata(mock_load_info, "test_asset")


def test_materialize_dlt_results(vayu_dlt_resource, mock_load_info):
    op_name = "test_op"
    dlt_asset_names = {
        op_name: [
            ["test_op", "table1"],
            ["test_op", "table2"],
        ]
    }

    results = list(
        vayu_dlt_resource.materialize_dlt_results(
            op_name, mock_load_info, dlt_asset_names
        )
    )

    assert len(results) == 2
    assert all(isinstance(result, MaterializeResult) for result in results)
    assert results[0].asset_key == AssetKey(["test_op", "table1"])
    assert results[1].asset_key == AssetKey(["test_op", "table2"])


def test_flatten_dict(vayu_dlt_resource):
    nested_dict = {"a": 1, "b": {"c": 2, "d": {"e": 3}}}
    flattened = vayu_dlt_resource._flatten_dict(nested_dict)
    assert flattened == {"a": 1, "b__c": 2, "b__d__e": 3}


@patch("tomllib.load")
@patch("builtins.open", new_callable=mock_open)
@patch.dict(os.environ, {}, clear=True)
@patch("os.path.exists")
def test_write_dlt_secrets_to_env(
    mock_exists, mock_open, mock_tomllib_load, vayu_dlt_resource
):
    # Mock os.path.exists to return True for our test file
    mock_exists.return_value = True

    mock_tomllib_load.return_value = {
        "section1": {"key1": "value1", "key2": "value2"},
        "section2": {"key3": "value3"},
    }

    vayu_dlt_resource._write_dlt_secrets_to_env("test_module")

    expected_file_path = "/path/to/dlt_project/test_module/.dlt/secrets.toml"
    mock_exists.assert_called_once_with(expected_file_path)
    mock_open.assert_called_once_with(expected_file_path, "rb")
    assert os.environ == {
        "SECTION1__KEY1": "value1",
        "SECTION1__KEY2": "value2",
        "SECTION2__KEY3": "value3",
    }
