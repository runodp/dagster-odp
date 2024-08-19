from pathlib import Path
from unittest.mock import Mock, PropertyMock, mock_open, patch

import pytest
import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Output,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.partition import ScheduleType
from dagster_dbt import DbtCliEventMessage

from dagster_vayu.config_manager.models.workflow_model import (
    DBTParams,
    DBTTask,
    DBTTaskParams,
    PartitionParams,
)
from dagster_vayu.creators.asset_creators.dbt_asset_creator import (
    CustomDagsterDbtTranslator,
    DBTAssetCreator,
)


@pytest.fixture
def mock_dbt_task():
    return DBTTask(
        asset_key="test_dbt_asset",
        params=DBTTaskParams(
            selection="test_model",
            dbt_vars={"var1": "value1", "var2": "value2"},
        ),
        task_type="dbt",
    )


@pytest.fixture
def mock_workflow_builder(mock_dbt_task):
    mock_wb = Mock()
    mock_wb.get_assets_with_task_type.return_value = [mock_dbt_task]
    mock_wb.asset_key_dbt_params_map = {
        "test_asset": DBTParams(source_name="test_source", table_name="test_table")
    }
    mock_wb.asset_key_partition_map = {
        "test_dbt_asset": PartitionParams(schedule_type="DAILY", start="2023-01-01"),
    }
    return mock_wb


@pytest.fixture
def mock_config_builder():
    mock_cb = Mock()
    mock_cb.get_config.return_value = Mock(
        resources=Mock(
            dbt=Mock(
                project_dir="/path/to/dbt_project",
                profile="test_profile",
                sources_file_path_="models/sources.yml",
            ),
            model_dump=Mock(
                return_value={"dbt": {"project_dir": "/path/to/dbt_project"}}
            ),
        )
    )
    return mock_cb


@pytest.fixture
def mock_dbt_cli_resource():
    return Mock()


@pytest.fixture
def dbt_asset_creator(
    mock_workflow_builder, mock_config_builder, mock_dbt_cli_resource
):
    with (
        patch(
            "dagster_vayu.creators.asset_creators.base_asset_creator.WorkflowBuilder",
            return_value=mock_workflow_builder,
        ),
        patch(
            "dagster_vayu.creators.asset_creators.base_asset_creator.ConfigBuilder",
            return_value=mock_config_builder,
        ),
        patch.object(
            Path,
            "joinpath",
            return_value=Path("/path/to/dbt_project/target/manifest.json"),
        ),
    ):
        return DBTAssetCreator(mock_dbt_cli_resource)


def test_custom_dagster_dbt_translator_get_metadata():
    custom_metadata = {"custom_key": "custom_value"}
    translator = CustomDagsterDbtTranslator(custom_metadata)

    with patch(
        "dagster_dbt.DagsterDbtTranslator.get_metadata",
        return_value={"base_key": "base_value"},
    ):
        result = translator.get_metadata({})

    assert result == {"base_key": "base_value", "custom_key": "custom_value"}


def test_init(
    dbt_asset_creator, mock_workflow_builder, mock_config_builder, mock_dbt_cli_resource
):
    assert dbt_asset_creator._wb == mock_workflow_builder
    assert dbt_asset_creator._dagster_config == mock_config_builder.get_config()
    assert (
        dbt_asset_creator._dbt_resource
        == mock_config_builder.get_config().resources.dbt
    )
    assert dbt_asset_creator._dbt_cli_resource == mock_dbt_cli_resource
    assert dbt_asset_creator._dbt_manifest_path == Path(
        "/path/to/dbt_project/target/manifest.json"
    )


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='{"sources": {"test_source": {}}}',
)
def test_manifest_sources(mock_file, dbt_asset_creator):
    sources = dbt_asset_creator._manifest_sources
    assert sources == {"test_source": {}}
    mock_file.assert_called_once_with(
        dbt_asset_creator._dbt_manifest_path, "r", encoding="utf-8"
    )


@patch(
    "dagster_vayu.creators.asset_creators.dbt_asset_creator.external_assets_from_specs"
)
def test_build_dbt_external_sources(mock_external_assets, dbt_asset_creator):
    mock_manifest_sources = {
        "source1": {
            "name": "source1",
            "resource_type": "source",
            "source_name": "test_source",
            "meta": {"dagster": {}},
            "description": "test description",
        },
        "source2": {
            "name": "source2",
            "resource_type": "source",
            "source_name": "test_source",
            "meta": {"dagster": {"asset_key": ["custom", "key"]}},
            "description": "test description",
        },
    }

    with patch(
        "dagster_vayu.creators.asset_creators.dbt_asset_creator.DBTAssetCreator._manifest_sources",
        new_callable=PropertyMock,
        return_value=mock_manifest_sources,
    ):
        dbt_asset_creator.build_dbt_external_sources()

    mock_external_assets.assert_called_once()
    args = mock_external_assets.call_args[0][0]
    assert len(args) == 1
    assert args[0].key == AssetKey(["test_source", "source1"])


@patch("yaml.dump")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=yaml.dump({"version": 2, "sources": []}),
)
def test_update_dbt_sources(mock_file, mock_yaml_dump, dbt_asset_creator):
    dbt_asset_creator.update_dbt_sources()

    # Assert that the file was opened for writing
    mock_file.assert_any_call(
        "/path/to/dbt_project/models/sources.yml", "w", encoding="utf-8"
    )

    # Assert that yaml.dump was called
    mock_yaml_dump.assert_called_once()


def test_get_dbt_output_metadata(dbt_asset_creator):
    event = Mock(spec=DbtCliEventMessage)
    event.raw_event = {
        "data": {
            "node_info": {
                "node_relation": {"schema": "test_schema", "alias": "test_alias"}
            }
        }
    }
    metadata = dbt_asset_creator._get_dbt_output_metadata(event)
    assert metadata == {"destination_table_id": "test_schema.test_alias"}


@patch("dagster_vayu.creators.asset_creators.dbt_asset_creator.json.dumps")
def test_materialize_dbt_asset(mock_json_dumps, dbt_asset_creator):
    # Mock the context and dbt resource
    mock_context = Mock(spec=AssetExecutionContext)
    mock_dbt = Mock()
    mock_context.resources.dbt = mock_dbt

    # Mock the dbt CLI invocation
    mock_cli_invocation = Mock()
    mock_dbt.cli.return_value = mock_cli_invocation

    # Create a mock event and output
    mock_event = Mock(spec=DbtCliEventMessage)
    mock_output = Mock(spec=Output)
    mock_cli_invocation.stream_raw_events.return_value = [mock_event]
    mock_event.to_default_asset_events.return_value = [mock_output]

    # Set up the _get_dbt_output_metadata mock
    dbt_asset_creator._get_dbt_output_metadata = Mock(
        return_value={"metadata_key": "metadata_value"}
    )

    # Run the method
    result = list(
        dbt_asset_creator._materialize_dbt_asset(mock_context, {"var1": "value1"})
    )

    # Assertions
    mock_dbt.update_config_params.assert_called_once_with(
        context=mock_context, config={"var1": "value1"}
    )
    mock_dbt.cli.assert_called_once_with(
        ["build", "--vars", mock_json_dumps.return_value], context=mock_context
    )
    mock_context.add_output_metadata.assert_called_once_with(
        metadata={"metadata_key": "metadata_value"}, output_name=mock_output.output_name
    )
    assert result == [mock_output]


@patch("dagster_vayu.creators.asset_creators.dbt_asset_creator.dbt_assets")
@patch(
    "dagster_vayu.creators.asset_creators.dbt_asset_creator.generate_partition_params"
)
def test_build_asset(
    mock_generate_partition_params, mock_dbt_assets, dbt_asset_creator
):
    mock_generate_partition_params.return_value = {
        "start": "2023-01-01",
        "end": "2023-12-31",
        "fmt": "%Y-%m-%d",
        "schedule_type": ScheduleType.DAILY,
    }

    dbt_asset_creator._build_asset(
        name="test_asset",
        dbt_vars={"var1": "value1"},
        select="test_model",
        partition_params=mock_generate_partition_params.return_value,
    )

    mock_dbt_assets.assert_called_once()
    _, kwargs = mock_dbt_assets.call_args
    assert kwargs["manifest"] == dbt_asset_creator._dbt_manifest_path
    assert kwargs["select"] == "test_model"
    assert kwargs["name"] == "test_asset"
    assert kwargs["required_resource_keys"] == {"dbt", "sensor_context"}
    assert isinstance(kwargs["partitions_def"], TimeWindowPartitionsDefinition)
    assert isinstance(kwargs["dagster_dbt_translator"], CustomDagsterDbtTranslator)


@patch.object(DBTAssetCreator, "_build_asset")
@patch.object(DBTAssetCreator, "build_dbt_external_sources")
def test_get_assets(mock_build_external_sources, mock_build_asset, dbt_asset_creator):
    mock_asset1 = Mock(name="asset1")
    mock_unselected_assets = Mock(name="unselected_assets")
    mock_build_asset.side_effect = [mock_asset1, mock_unselected_assets]
    mock_external_source = Mock(name="external_source")
    mock_build_external_sources.return_value = [mock_external_source]

    assets = dbt_asset_creator.get_assets()

    assert len(assets) == 3  # 1 DBT asset + 1 unselected asset + 1 external source
    assert mock_build_asset.call_count == 2
    mock_build_external_sources.assert_called_once()
    assert assets == [mock_asset1, mock_unselected_assets, mock_external_source]
