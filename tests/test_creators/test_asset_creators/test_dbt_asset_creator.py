import json
from unittest.mock import Mock, patch

import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Output,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.partition import ScheduleType
from dagster_dbt import DbtCliEventMessage, DbtProject

from dagster_odp.config_manager.models.workflow_model import (
    DBTParams,
    DBTTask,
    DBTTaskParams,
    PartitionParams,
)
from dagster_odp.creators.asset_creators.dbt_asset_creator import (
    CustomDagsterDbtTranslator,
    DBTAssetCreator,
)


@pytest.fixture
def mock_manifest():
    return {
        "sources": {
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
                "meta": {"dagster": {"external": True}},
                "description": "test description",
            },
        }
    }


@pytest.fixture
def mock_dbt_project(tmp_path, mock_manifest):
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()

    # Create manifest.json
    manifest_dir = project_dir / "target"
    manifest_dir.mkdir()
    manifest_path = manifest_dir / "manifest.json"
    manifest_path.write_text(json.dumps(mock_manifest))

    # Create minimal dbt_project.yml
    (project_dir / "dbt_project.yml").write_text("name: test_project\nversion: 1.0.0")

    return DbtProject(project_dir=str(project_dir), target="dev")


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
def mock_config_builder(mock_dbt_project):
    mock_cb = Mock()
    mock_cb.get_config.return_value = Mock(
        resources=[
            Mock(
                resource_kind="dbt",
                params=Mock(
                    project_dir=str(mock_dbt_project.project_dir),
                    target="dev",
                    load_all_models=True,
                ),
            )
        ]
    )
    mock_cb.resource_config_map = {
        "dbt": {"project_dir": str(mock_dbt_project.project_dir)}
    }
    mock_cb.resource_class_map = {
        "dbt": Mock(
            project_dir=str(mock_dbt_project.project_dir),
            target="dev",
            load_all_models=True,
        )
    }
    return mock_cb


@pytest.fixture
def dbt_asset_creator(mock_workflow_builder, mock_config_builder, mock_dbt_project):
    with (
        patch(
            "dagster_odp.creators.asset_creators.base_asset_creator.WorkflowBuilder",
            return_value=mock_workflow_builder,
        ),
        patch(
            "dagster_odp.creators.asset_creators.base_asset_creator.ConfigBuilder",
            return_value=mock_config_builder,
        ),
        patch(
            "dagster_odp.creators.asset_creators.dbt_asset_creator.DbtProject",
            return_value=mock_dbt_project,
        ),
    ):
        return DBTAssetCreator()


# Rest of the test functions remain the same, but update build_dbt_external_sources test:


@patch(
    "dagster_odp.creators.asset_creators.dbt_asset_creator.external_assets_from_specs"
)
def test_build_dbt_external_sources(mock_external_assets, dbt_asset_creator):
    dbt_asset_creator.build_dbt_external_sources()

    mock_external_assets.assert_called_once()
    args = mock_external_assets.call_args[0][0]

    # Only source2 is marked as external
    assert len(args) == 1
    assert args[0].key == AssetKey(["test_source", "source2"])


def test_custom_dagster_dbt_translator_get_metadata():
    custom_metadata = {"custom_key": "custom_value"}
    translator = CustomDagsterDbtTranslator(custom_metadata)

    with patch(
        "dagster_dbt.DagsterDbtTranslator.get_metadata",
        return_value={"base_key": "base_value"},
    ):
        result = translator.get_metadata({})

    assert result == {"base_key": "base_value", "custom_key": "custom_value"}


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


@patch("dagster_odp.creators.asset_creators.dbt_asset_creator.json.dumps")
def test_materialize_dbt_asset(mock_json_dumps, dbt_asset_creator):
    mock_context = Mock(spec=AssetExecutionContext)
    mock_dbt = Mock()
    mock_context.resources.dbt = mock_dbt
    mock_cli_invocation = Mock()
    mock_dbt.cli.return_value = mock_cli_invocation
    mock_event = Mock(spec=DbtCliEventMessage)
    mock_output = Mock(spec=Output)
    mock_cli_invocation.stream_raw_events.return_value = [mock_event]
    mock_event.to_default_asset_events.return_value = [mock_output]
    dbt_asset_creator._get_dbt_output_metadata = Mock(
        return_value={"metadata_key": "metadata_value"}
    )

    result = list(
        dbt_asset_creator._materialize_dbt_asset(mock_context, {"var1": "value1"})
    )

    # Assertions
    mock_dbt.update_asset_params.assert_called_once_with(
        context=mock_context,
        resource_config=dbt_asset_creator._resource_config_map,
        asset_params={"var1": "value1"},
    )
    mock_dbt.cli.assert_called_once_with(
        ["build", "--vars", mock_json_dumps.return_value], context=mock_context
    )
    mock_context.add_output_metadata.assert_called_once_with(
        metadata={"metadata_key": "metadata_value"}, output_name=mock_output.output_name
    )
    assert result == [mock_output]


@patch("dagster_odp.creators.asset_creators.dbt_asset_creator.dbt_assets")
@patch(
    "dagster_odp.creators.asset_creators.dbt_asset_creator.generate_partition_params"
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
    assert kwargs["manifest"] == dbt_asset_creator._dbt_project.manifest_path
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

    # Test when load_all_models is True
    dbt_asset_creator._dbt_cli_resource.load_all_models = True
    assets = dbt_asset_creator.get_assets()

    assert len(assets) == 3  # 1 DBT asset + 1 unselected asset + 1 external source
    assert mock_build_asset.call_count == 2
    mock_build_external_sources.assert_called_once()
    assert assets == [mock_asset1, mock_unselected_assets, mock_external_source]

    # Reset mocks
    mock_build_asset.reset_mock()
    mock_build_external_sources.reset_mock()
    mock_build_asset.side_effect = [mock_asset1]  # Only one call expected

    # Test when load_all_models is False
    dbt_asset_creator._dbt_cli_resource.load_all_models = False
    assets = dbt_asset_creator.get_assets()

    assert len(assets) == 2  # 1 DBT asset + 1 external source
    assert mock_build_asset.call_count == 1
    mock_build_external_sources.assert_called_once()
    assert assets == [mock_asset1, mock_external_source]
