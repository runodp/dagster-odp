import os
from unittest.mock import Mock, mock_open, patch

import pytest
from dagster import AssetKey, MaterializeResult
from dlt.common.pipeline import LoadInfo
from pendulum import DateTime, Timezone

from dagster_odp.config_manager.models.workflow_model import DLTParams, DLTTask
from dagster_odp.resources.definitions import OdpDltResource


@pytest.fixture
def odp_dlt_resource():
    return OdpDltResource(project_dir="/path/to/dlt_project")


@pytest.fixture
def sample_dlt_task():
    return DLTTask(
        asset_key="test/abc/dlt_asset",
        task_type="dlt",
        params=DLTParams(
            schema_file_path="schemas/export/test_function.schema.yaml",
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
        "started_at": "2023-05-17T12:00:00",
        "finished_at": "2023-05-17T12:05:00",
        "dataset_name": "test_dataset",
        "destination_name": "bigquery",
        "destination_type": "bigquery",
        "destination_displayable_credentials": "gs://test-bucket",
    }
    return load_info


def test_get_source(odp_dlt_resource):
    with patch("importlib.import_module") as mock_importlib:
        mock_module = Mock()
        mock_func = Mock()
        mock_module.test_function = mock_func
        mock_importlib.return_value = mock_module

        result = odp_dlt_resource._get_source(
            "test_module.test_function", {"param": "value"}
        )

        assert result == mock_func.return_value
        mock_importlib.assert_called_once_with("test_module")
        mock_func.assert_called_once_with(param="value")


def test_cast_load_info_metadata(odp_dlt_resource):
    test_data = {
        "datetime": DateTime(2023, 5, 17, 12, 0, 0),
        "timezone": Timezone("UTC"),
        "nested": {
            "datetime": DateTime(2023, 5, 17, 12, 0, 0),
            "list": [DateTime(2023, 5, 17, 12, 0, 0), "string", 123],
        },
    }
    result = odp_dlt_resource._cast_load_info_metadata(test_data)
    assert isinstance(result["datetime"], str)
    assert isinstance(result["timezone"], str)
    assert isinstance(result["nested"]["datetime"], str)
    assert isinstance(result["nested"]["list"][0], str)
    assert result["nested"]["list"][1] == "string"
    assert result["nested"]["list"][2] == 123


@pytest.mark.parametrize(
    "destination_name, dataset_name, expected_key, expected_value",
    [
        ("bigquery", "test_dataset", "destination_table_id", "test_dataset.test_asset"),
        ("duckdb", "test_dataset", "destination_table_id", "test_dataset.test_asset"),
        (
            "filesystem",
            "test_dataset",
            "destination_file_uri",
            "gs://test-bucket/test_dataset/test_asset",
        ),
        (
            "filesystem",
            None,
            None,
            None,
        ),
    ],
)
def test_extract_dlt_metadata(
    odp_dlt_resource,
    mock_load_info,
    destination_name,
    dataset_name,
    expected_key,
    expected_value,
):
    mock_load_info_dict = {
        "started_at": "2023-05-17T12:00:00",
        "finished_at": "2023-05-17T12:05:00",
        "destination_name": destination_name,
        "destination_type": destination_name,
        "destination_displayable_credentials": "gs://test-bucket",
    }
    mock_load_info_dict["dataset_name"] = dataset_name if dataset_name else None

    mock_load_info.asdict.return_value = mock_load_info_dict

    result = odp_dlt_resource.extract_dlt_metadata(mock_load_info, "test_asset")

    if dataset_name is not None:
        assert result[expected_key] == expected_value
    else:
        assert "destination_file_uri" not in result


def test_materialize_dlt_results(odp_dlt_resource, mock_load_info):
    dlt_asset_names = [
        ["test_op", "table1"],
        ["test_op", "table2"],
    ]

    results = list(
        odp_dlt_resource.materialize_dlt_results(mock_load_info, dlt_asset_names)
    )

    assert len(results) == 2
    assert all(isinstance(result, MaterializeResult) for result in results)
    assert results[0].asset_key == AssetKey(["test_op", "table1"])
    assert results[1].asset_key == AssetKey(["test_op", "table2"])


def test_flatten_dict(odp_dlt_resource):
    nested_dict = {"a": 1, "b": {"c": 2, "d": {"e": 3}}}
    flattened = odp_dlt_resource._flatten_dict(nested_dict)
    assert flattened == {"a": 1, "b__c": 2, "b__d__e": 3}


@patch("tomli.load")  # Changed from tomllib to tomli
@patch("builtins.open", new_callable=mock_open)
@patch.dict(os.environ, {}, clear=True)
@patch("os.path.exists")
def test_write_dlt_secrets_to_env(
    mock_exists, mock_open, mock_tomli_load, odp_dlt_resource
):
    # Mock os.path.exists to return True for our test file
    mock_exists.return_value = True

    mock_tomli_load.return_value = {
        "section1": {"key1": "value1", "key2": "value2"},
        "section2": {"key3": "value3"},
    }

    odp_dlt_resource._write_dlt_secrets_to_env(
        "test_directory.test_module.test_function"
    )

    expected_file_path = "/path/to/dlt_project/test_directory/.dlt/secrets.toml"
    mock_exists.assert_called_once_with(expected_file_path)
    mock_open.assert_called_once_with(expected_file_path, "rb")
    assert os.environ == {
        "SECTION1__KEY1": "value1",
        "SECTION1__KEY2": "value2",
        "SECTION2__KEY3": "value3",
    }
