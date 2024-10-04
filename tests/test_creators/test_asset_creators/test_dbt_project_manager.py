from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

from dagster_odp.config_manager.models.workflow_model import DBTParams
from dagster_odp.creators.asset_creators.dbt_project_manager import DBTProjectManager


@pytest.fixture
def mock_dbt_cli_resource():
    return Mock(
        project_dir="/path/to/dbt_project",
        profile="test_profile",
        sources_file_path="models/sources.yml",
    )


@pytest.fixture
def mock_dbt_asset_defs():
    return {"test_asset": DBTParams(source_name="test_source", table_name="test_table")}


@pytest.fixture
def dbt_project_manager(mock_dbt_cli_resource, mock_dbt_asset_defs):
    with patch(
        "dagster_odp.creators.asset_creators.dbt_project_manager.DBTProjectManager._load_dbt_manifest_path"
    ):
        manager = DBTProjectManager(mock_dbt_cli_resource, mock_dbt_asset_defs)
        manager._dbt_manifest_path = Path("/path/to/dbt_project/target/manifest.json")
        return manager


def test_init(dbt_project_manager, mock_dbt_cli_resource, mock_dbt_asset_defs):
    assert dbt_project_manager._dbt_cli_resource == mock_dbt_cli_resource
    assert dbt_project_manager._dbt_asset_defs == mock_dbt_asset_defs


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='{"sources": {"test_source": {}}}',
)
def test_manifest_sources(mock_file, dbt_project_manager):
    sources = dbt_project_manager.manifest_sources
    assert sources == {"test_source": {}}
    mock_file.assert_called_once_with(
        dbt_project_manager._dbt_manifest_path, "r", encoding="utf-8"
    )


def test_update_sources_yml_data(dbt_project_manager):
    sources_yml_data = {"version": 2, "sources": []}
    dbt_asset_defs = {
        "asset1": DBTParams(source_name="source1", table_name="table1"),
        "asset2": DBTParams(source_name="source1", table_name="table2"),
        "asset3": DBTParams(source_name="source2", table_name="table3"),
    }

    result = dbt_project_manager._update_sources_yml_data(
        sources_yml_data, dbt_asset_defs
    )

    assert len(result["sources"]) == 2
    assert result["sources"][0]["name"] == "source1"
    assert len(result["sources"][0]["tables"]) == 2
    assert result["sources"][1]["name"] == "source2"
    assert len(result["sources"][1]["tables"]) == 1


@patch("yaml.dump")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=yaml.dump({"version": 2, "sources": []}),
)
def test_update_dbt_sources(mock_file, mock_yaml_dump, dbt_project_manager):

    dbt_project_manager.update_dbt_sources()
    # Assert that the file was opened for writing
    mock_file.assert_any_call(
        "/path/to/dbt_project/models/sources.yml", "w", encoding="utf-8"
    )

    # Assert that yaml.dump was called
    mock_yaml_dump.assert_called_once()


def test_update_dbt_sources_no_sources_file_path(dbt_project_manager):
    dbt_project_manager._dbt_cli_resource.sources_file_path = None

    with pytest.raises(
        ValueError, match="sources_file_path must be configured in dagster config"
    ):
        dbt_project_manager.update_dbt_sources()
