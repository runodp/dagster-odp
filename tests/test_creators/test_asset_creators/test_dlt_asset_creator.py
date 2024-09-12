from unittest.mock import Mock, patch

import pytest
import yaml
from dagster import AssetsDefinition

from dagster_vayu.config_manager.models.workflow_model import DLTParams, DLTTask
from dagster_vayu.creators.asset_creators.dlt_asset_creator import DLTAssetCreator


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock()
    mock_wb.get_assets_with_task_type.return_value = [
        DLTTask(
            asset_key="test/dlt_asset",
            task_type="dlt",
            params=DLTParams(
                source_module="test_module",
                source_params={},
                destination="bigquery",
                destination_params={},
                pipeline_params={"dataset_name": "test_dataset"},
            ),
        )
    ]
    return mock_wb


@pytest.fixture
def mock_config_builder():
    mock_cb = Mock()
    mock_cb.get_config.return_value = Mock(
        resources=[
            Mock(resource_kind="dlt", params=Mock(project_dir="/path/to/dlt_project"))
        ]
    )
    mock_cb.resource_class_map = {"dlt": Mock(project_dir="/path/to/dlt_project")}
    return mock_cb


@pytest.fixture
def dlt_asset_creator(mock_workflow_builder, mock_config_builder):
    with (
        patch(
            "dagster_vayu.creators.asset_creators.base_asset_creator.WorkflowBuilder",
            return_value=mock_workflow_builder,
        ),
        patch(
            "dagster_vayu.creators.asset_creators.base_asset_creator.ConfigBuilder",
            return_value=mock_config_builder,
        ),
    ):
        return DLTAssetCreator()


def test_get_dlt_destination_objects(dlt_asset_creator, tmpdir):
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

    result = dlt_asset_creator._get_dlt_destination_objects(
        str(tmpdir), "test_module", "test_function"
    )

    assert result == ["table1", "table2"]
    assert "_hidden_table" not in result


def test_get_dlt_destination_objects_file_not_found(dlt_asset_creator, tmpdir):
    with pytest.raises(FileNotFoundError):
        dlt_asset_creator._get_dlt_destination_objects(
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
    dlt_asset_creator, schema, expected_error
):
    with pytest.raises(ValueError, match=expected_error):
        dlt_asset_creator._get_dlt_destination_objects(
            "/path/to/dlt", "test_module", "test_function", schema=schema
        )


@patch("dagster_vayu.creators.asset_creators.dlt_asset_creator.multi_asset")
@patch.object(DLTAssetCreator, "_get_dlt_destination_objects")
def test_build_asset(
    mock_get_dlt_destination_objects, mock_multi_asset, dlt_asset_creator
):
    mock_get_dlt_destination_objects.return_value = ["table1", "table2"]

    dlt_task = DLTTask(
        asset_key="test/abc/dlt_asset",
        task_type="dlt",
        group_name="test_group",
        params=DLTParams(
            source_module="test_module.test_function",
            source_params={},
            destination="bigquery",
            destination_params={},
            pipeline_params={"dataset_name": "test_dataset"},
        ),
    )

    result = dlt_asset_creator._build_asset(dlt_task, "/path/to/dlt")

    mock_multi_asset.assert_called_once()
    _, kwargs = mock_multi_asset.call_args
    assert kwargs["name"] == "test__abc"
    assert kwargs["group_name"] == "test_group"
    assert kwargs["required_resource_keys"] == {"sensor_context", "dlt"}
    assert kwargs["compute_kind"] == "dlt"
    assert len(kwargs["specs"]) == 2

    mock_get_dlt_destination_objects.assert_called_once_with(
        "/path/to/dlt", "test_module", "test_function"
    )

    assert callable(result)


@patch(
    "dagster_vayu.creators.asset_creators.dlt_asset_creator.external_asset_from_spec"
)
@patch.object(DLTAssetCreator, "_build_asset")
def test_get_assets(mock_build_asset, mock_external_asset_from_spec, dlt_asset_creator):
    dlt_asset_creator._wb.get_assets_with_task_type.return_value = [
        DLTTask(
            asset_key="parent/nested_asset",
            task_type="dlt",
            group_name="group1",
            description="Test asset",
            params=DLTParams(
                source_module="test_module",
                source_params={},
                destination="bigquery",
                destination_params={},
                pipeline_params={"dataset_name": "test_dataset"},
            ),
        )
    ]

    mock_build_asset.return_value = Mock(spec=AssetsDefinition)
    mock_external_asset_from_spec.return_value = Mock(spec=AssetsDefinition)

    result = dlt_asset_creator.get_assets()

    assert len(result) == 2  # 1 external asset + 1 DLT asset
    assert mock_external_asset_from_spec.call_count == 1
    assert mock_build_asset.call_count == 1

    # Check that the result includes both the external asset and the built asset
    assert result[0] == mock_external_asset_from_spec.return_value
    assert result[1] == mock_build_asset.return_value

    # Check that _build_asset was called with the correct dlt_path
    mock_build_asset.assert_called_once_with(
        dlt_asset_creator._wb.get_assets_with_task_type.return_value[0],
        "/path/to/dlt_project",
    )
