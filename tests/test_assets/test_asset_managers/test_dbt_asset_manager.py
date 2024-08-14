from pathlib import Path
from unittest.mock import MagicMock, Mock, PropertyMock, mock_open, patch

import pytest
import yaml
from dagster_dbt import DagsterDbtTranslator, DbtCliEventMessage
from task_nicely_core.assets.asset_managers.dbt_asset_manager import (
    CustomDagsterDbtTranslator,
    DBTAssetManager,
)
from task_nicely_core.configs.models.workflow_model import (
    DBTParams,
    DBTTask,
    DBTTaskParams,
    PartitionParams,
)

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Output,
    TimeWindowPartitionsDefinition,
)
from dagster._core.definitions.partition import ScheduleType


# New shared fixtures
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
def mock_manifest_sources():
    return {
        "source1": {
            "name": "source1",
            "meta": {"dagster": {}},
        },
        "source2": {
            "name": "source2",
            "meta": {"dagster": {"asset_key": ["custom", "key"]}},
        },
    }


# Existing fixtures
@pytest.fixture
def mock_workflow_builder(mock_dbt_task):
    mock_wb = Mock()
    mock_wb.get_assets_with_task_type.return_value = [mock_dbt_task]
    mock_wb.asset_key_dbt_params_map = {
        "test_asset": DBTParams(source_name="test_source", table_name="test_table")
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
    mock_cb.get_dagster_resources.return_value = {"dbt": Mock()}
    return mock_cb


@pytest.fixture
def dbt_asset_manager(mock_workflow_builder, mock_config_builder):
    with patch(
        "task_nicely_core.assets.asset_managers.base_asset_manager.WorkflowBuilder",
        return_value=mock_workflow_builder,
    ), patch(
        "task_nicely_core.assets.asset_managers.base_asset_manager.ConfigBuilder",
        return_value=mock_config_builder,
    ), patch(
        "task_nicely_core.assets.asset_managers.dbt_asset_manager.ConfigBuilder",
        return_value=mock_config_builder,
    ), patch(
        "task_nicely_core.assets.asset_managers.dbt_asset_manager.Path.joinpath",
        return_value=Path("/path/to/dbt_project/target/manifest.json"),
    ):
        return DBTAssetManager()


def test_custom_dagster_dbt_translator_get_metadata():
    custom_metadata = {"custom_key": "custom_value"}
    translator = CustomDagsterDbtTranslator(custom_metadata)

    mock_base_metadata = {"base_key": "base_value"}

    with patch(
        "task_nicely_core.assets.asset_managers.dbt_asset_manager"
        ".DagsterDbtTranslator.get_metadata",
        return_value=mock_base_metadata,
    ):
        result = translator.get_metadata({})

    assert result == {"base_key": "base_value", "custom_key": "custom_value"}


def test_init(dbt_asset_manager, mock_workflow_builder, mock_config_builder):
    assert dbt_asset_manager._wb == mock_workflow_builder
    assert dbt_asset_manager._dagster_config == mock_config_builder.get_config()
    assert (
        dbt_asset_manager._dbt_resource
        == mock_config_builder.get_config().resources.dbt
    )
    assert (
        dbt_asset_manager._dbt_cli_resource
        == mock_config_builder.get_dagster_resources()["dbt"]
    )
    assert dbt_asset_manager._dbt_manifest_path == Path(
        "/path/to/dbt_project/target/manifest.json"
    )


def test_parse_project(dbt_asset_manager):
    mock_cli = Mock()
    mock_cli.wait.return_value.target_path.joinpath.return_value = Path(
        "/path/to/manifest.json"
    )
    dbt_asset_manager._dbt_cli_resource.cli.return_value = mock_cli

    result = dbt_asset_manager._parse_project(Path("target"))

    assert result == Path("/path/to/manifest.json")
    dbt_asset_manager._dbt_cli_resource.cli.assert_called_once()


@pytest.mark.parametrize("env_var_set", [True, False])
def test_load_dbt_manifest_path(dbt_asset_manager, env_var_set):
    with patch.dict(
        "os.environ",
        {"DAGSTER_DBT_PARSE_PROJECT_ON_LOAD": "true" if env_var_set else ""},
    ), patch.object(
        DBTAssetManager, "update_dbt_sources"
    ) as mock_update_sources, patch.object(
        DBTAssetManager, "_parse_project"
    ) as mock_parse_project:

        mock_parse_project.return_value = Path("/path/to/parsed/manifest.json")

        dbt_asset_manager._load_dbt_manifest_path()

        expected_path = (
            "/path/to/parsed/manifest.json"
            if env_var_set
            else "/path/to/dbt_project/target/manifest.json"
        )
        assert str(dbt_asset_manager._dbt_manifest_path) == expected_path

        if env_var_set:
            mock_update_sources.assert_called_once()
            mock_parse_project.assert_called_once_with(Path("target"))
        else:
            mock_update_sources.assert_not_called()
            mock_parse_project.assert_not_called()


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='{"sources": {"test_source": {}}}',
)
def test_manifest_sources(mock_file, dbt_asset_manager):
    sources = dbt_asset_manager._manifest_sources
    assert sources == {"test_source": {}}
    mock_file.assert_called_once_with(
        dbt_asset_manager._dbt_manifest_path, "r", encoding="utf-8"
    )


@patch(
    "task_nicely_core.assets.asset_managers"
    ".dbt_asset_manager.external_assets_from_specs"
)
@patch("task_nicely_core.assets.asset_managers.dbt_asset_manager.DagsterDbtTranslator")
def test_build_dbt_external_sources(
    mock_translator_class,
    mock_external_assets,
    dbt_asset_manager,
    mock_manifest_sources,
):
    mock_translator = MagicMock(spec=DagsterDbtTranslator)
    mock_translator.get_asset_key.return_value = ["source", "source1"]
    mock_translator.get_group_name.return_value = "test_group"
    mock_translator.get_description.return_value = "Test description"
    mock_translator_class.return_value = mock_translator

    with patch.object(
        DBTAssetManager, "_manifest_sources", new_callable=PropertyMock
    ) as mock_manifest_sources_prop:
        mock_manifest_sources_prop.return_value = mock_manifest_sources

        dbt_asset_manager.build_dbt_external_sources()

        mock_external_assets.assert_called_once()
        args = mock_external_assets.call_args[0][0]
        assert len(args) == 1
        assert args[0].key == AssetKey(["source", "source1"])
        assert args[0].group_name == "test_group"
        assert args[0].description == "Test description"

        mock_translator.get_asset_key.assert_any_call(mock_manifest_sources["source1"])
        assert "source2" not in [
            call.args[0]["name"]
            for call in mock_translator.get_asset_key.call_args_list
        ]


@pytest.mark.parametrize(
    "file_exists,initial_content,expected_content",
    [
        (True, {"version": 2, "sources": []}, {"version": 2, "sources": []}),
        (True, None, {"version": 2, "sources": []}),
        (False, None, {"version": 2, "sources": []}),
    ],
)
def test_read_or_create_sources_yml(
    file_exists, initial_content, expected_content, dbt_asset_manager
):
    with patch("os.path.exists", return_value=file_exists), patch(
        "builtins.open",
        mock_open(read_data=yaml.dump(initial_content) if initial_content else ""),
    ):
        result = dbt_asset_manager._read_or_create_sources_yml("dummy_path")
        assert result == expected_content


def test_update_sources_yml_data(dbt_asset_manager):
    initial_data = {
        "version": 2,
        "sources": [
            {
                "name": "existing_source",
                "tables": [
                    {
                        "name": "existing_table",
                        "meta": {"dagster": {"asset_key": ["existing_asset"]}},
                    }
                ],
            }
        ],
    }
    dbt_asset_defs = {
        "new_asset": DBTParams(source_name="new_source", table_name="new_table"),
        "existing_asset": DBTParams(
            source_name="existing_source", table_name="existing_table"
        ),
        "additional_asset": DBTParams(
            source_name="existing_source", table_name="additional_table"
        ),
    }

    result = dbt_asset_manager._update_sources_yml_data(initial_data, dbt_asset_defs)

    assert len(result["sources"]) == 2
    assert any(source["name"] == "existing_source" for source in result["sources"])
    assert any(source["name"] == "new_source" for source in result["sources"])

    existing_source = next(
        source for source in result["sources"] if source["name"] == "existing_source"
    )
    assert len(existing_source["tables"]) == 2
    assert any(table["name"] == "existing_table" for table in existing_source["tables"])
    assert any(
        table["name"] == "additional_table" for table in existing_source["tables"]
    )

    new_source = next(
        source for source in result["sources"] if source["name"] == "new_source"
    )
    assert len(new_source["tables"]) == 1
    assert new_source["tables"][0]["name"] == "new_table"
    assert new_source["tables"][0]["meta"]["dagster"]["asset_key"] == ["new_asset"]


@patch("builtins.open", new_callable=mock_open)
@patch("yaml.dump")
@patch(
    "task_nicely_core.assets.asset_managers.dbt_asset_manager."
    "DBTAssetManager._update_sources_yml_data"
)
def test_update_dbt_sources(
    mock_update_data,
    mock_yaml_dump,
    mock_file,
    dbt_asset_manager,
):
    mock_update_data.return_value = {
        "version": 2,
        "sources": [{"name": "updated_source"}],
    }

    dbt_asset_manager.update_dbt_sources()

    mock_file.assert_any_call(
        "/path/to/dbt_project/models/sources.yml", "w", encoding="utf-8"
    )

    mock_update_data.assert_called_once_with(
        {"version": 2, "sources": []}, dbt_asset_manager._wb.asset_key_dbt_params_map
    )

    mock_yaml_dump.assert_called_once_with(
        {"version": 2, "sources": [{"name": "updated_source"}]},
        mock_file(),
        sort_keys=False,
    )


def test_get_dbt_output_metadata(dbt_asset_manager):
    event = Mock(spec=DbtCliEventMessage)
    event.raw_event = {
        "data": {
            "node_info": {
                "node_relation": {"schema": "test_schema", "alias": "test_alias"}
            }
        }
    }
    metadata = dbt_asset_manager._get_dbt_output_metadata(event)
    assert metadata == {"destination_table_id": "test_schema.test_alias"}


@patch("task_nicely_core.assets.asset_managers.dbt_asset_manager.ConfigReplacer")
def test_execute_dbt_asset_fn(mock_config_replacer, dbt_asset_manager):
    mock_context = Mock(spec=AssetExecutionContext)
    mock_dbt = Mock()
    mock_dbt_cli_invocation = mock_dbt.cli.return_value

    mock_event = Mock(spec=DbtCliEventMessage)
    mock_event.raw_event = {
        "data": {
            "node_info": {
                "node_relation": {"schema": "test_schema", "alias": "test_alias"}
            }
        }
    }
    mock_event.to_default_asset_events.return_value = [
        Output(output_name="test_output", value=None)
    ]

    mock_dbt_cli_invocation.stream_raw_events.return_value = [mock_event]

    mock_config_replacer.return_value.replace.return_value = {"replaced_var": "value"}

    list(
        dbt_asset_manager._execute_dbt_asset_fn(
            mock_context, mock_dbt, {"test_var": "value"}
        )
    )

    mock_dbt.cli.assert_called_once_with(
        ["build", "--vars", '{"replaced_var": "value"}'], context=mock_context
    )
    mock_context.add_output_metadata.assert_called_once_with(
        metadata={"destination_table_id": "test_schema.test_alias"},
        output_name="test_output",
    )


@patch("task_nicely_core.assets.asset_managers.dbt_asset_manager.dbt_assets")
def test_build_asset(mock_dbt_assets, dbt_asset_manager):
    # Test case 1: Valid input with select and partition_params
    dbt_asset_manager._build_asset(
        name="test_asset",
        dbt_vars={"var1": "value1"},
        select="test_model",
        partition_params={
            "start": "2023-01-01",
            "end": "2023-12-31",
            "fmt": "%Y-%m-%d",
            "schedule_type": ScheduleType.DAILY,
        },
    )

    mock_dbt_assets.assert_called_once()
    _, kwargs = mock_dbt_assets.call_args
    assert kwargs["manifest"] == dbt_asset_manager._dbt_manifest_path
    assert kwargs["select"] == "test_model"
    assert kwargs["name"] == "test_asset"
    assert kwargs["required_resource_keys"] == {"dbt", "sensor_context"}
    assert isinstance(kwargs["partitions_def"], TimeWindowPartitionsDefinition)
    assert isinstance(kwargs["dagster_dbt_translator"], CustomDagsterDbtTranslator)

    custom_metadata = kwargs["dagster_dbt_translator"].custom_metadata
    assert custom_metadata == {
        "dbt_vars": {"var1": "value1"},
        "select": "test_model",
        "exclude": None,
    }

    mock_dbt_assets.reset_mock()

    # Test case 2: Valid input with exclude and no partition_params
    dbt_asset_manager._build_asset(
        name="test_asset_2",
        dbt_vars={"var2": "value2"},
        exclude="excluded_model",
    )

    mock_dbt_assets.assert_called_once()
    _, kwargs = mock_dbt_assets.call_args
    assert kwargs["exclude"] == "excluded_model"
    assert kwargs["partitions_def"] is None
    custom_metadata = kwargs["dagster_dbt_translator"].custom_metadata
    assert custom_metadata == {
        "dbt_vars": {"var2": "value2"},
        "select": "fqn:*",
        "exclude": "excluded_model",
    }


@patch.object(DBTAssetManager, "_build_asset")
@patch.object(DBTAssetManager, "build_dbt_external_sources")
@patch(
    "task_nicely_core.assets.asset_managers.dbt_asset_manager.generate_partition_params"
)
def test_get_assets(
    mock_generate_partition_params,
    mock_build_external_sources,
    mock_build_asset,
    dbt_asset_manager,
):
    # Mock the workflow builder and asset_key_partition_map
    dbt_asset_manager._wb.get_assets_with_task_type.return_value = [
        DBTTask(
            asset_key="dbt_asset1",
            params=DBTTaskParams(selection="model1", dbt_vars={"var1": "value1"}),
            task_type="dbt",
        ),
        DBTTask(
            asset_key="dbt_asset2",
            params=DBTTaskParams(selection="model2", dbt_vars={"var2": "value2"}),
            task_type="dbt",
        ),
    ]
    dbt_asset_manager._wb.asset_key_partition_map = {
        "dbt_asset1": PartitionParams(schedule_type="DAILY", start="2023-01-01"),
        "dbt_asset2": None,
    }

    # Mock generate_partition_params
    mock_generate_partition_params.return_value = {
        "start": "2023-01-01",
        "schedule_type": ScheduleType.DAILY,
        "fmt": "%Y-%m-%d",
    }

    # Mock the _build_asset method to return unique mocks
    mock_asset1 = Mock(name="asset1")
    mock_asset2 = Mock(name="asset2")
    mock_unselected_assets = Mock(name="unselected_assets")
    mock_build_asset.side_effect = [mock_asset1, mock_asset2, mock_unselected_assets]

    # Mock the build_dbt_external_sources method
    mock_external_source = Mock(name="external_source")
    mock_build_external_sources.return_value = [mock_external_source]

    # Call get_assets
    assets = dbt_asset_manager.get_assets()

    # Assertions
    assert len(assets) == 4  # 2 DBT assets + 1 unselected asset + 1 external source

    # Check calls to _build_asset
    assert mock_build_asset.call_count == 3
    mock_build_asset.assert_any_call(
        name="dbt_asset1",
        select="model1",
        dbt_vars={"var1": "value1"},
        partition_params=mock_generate_partition_params.return_value,
    )
    mock_build_asset.assert_any_call(
        name="dbt_asset2",
        select="model2",
        dbt_vars={"var2": "value2"},
        partition_params=None,
    )
    mock_build_asset.assert_any_call(
        name="unselected_dbt_assets",
        exclude="model1 model2",
        dbt_vars={},
    )

    # Check call to generate_partition_params
    mock_generate_partition_params.assert_called_once_with(
        PartitionParams(schedule_type="DAILY", start="2023-01-01")
    )

    # Check call to build_dbt_external_sources
    mock_build_external_sources.assert_called_once()

    # Verify the returned assets
    assert assets == [
        mock_asset1,
        mock_asset2,
        mock_unselected_assets,
        mock_external_source,
    ]
