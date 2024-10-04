import json

import pytest

from dagster_odp.config_manager.builders.config_builder import ConfigBuilder
from dagster_odp.config_manager.models.config_model import DagsterConfig


@pytest.fixture
def mock_dbt_project(tmp_path):
    dbt_project_dir = tmp_path / "dbt_project"
    dbt_project_dir.mkdir()
    (
        dbt_project_dir / "dbt_project.yml"
    ).touch()  # Create an empty dbt_project.yml file
    return dbt_project_dir


@pytest.fixture
def sample_config_data(mock_dbt_project):
    return {
        "resources": [
            {
                "resource_kind": "bigquery",
                "params": {"project": "test-project", "location": "US"},
            },
            {
                "resource_kind": "dbt",
                "params": {
                    "project_dir": str(mock_dbt_project),
                    "profile": "test_profile",
                },
            },
        ],
    }


@pytest.fixture
def config_builder():
    return ConfigBuilder()


def test_load_config_with_data(config_builder, sample_config_data):
    config_builder.load_config(sample_config_data, None)
    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert len(config.resources) == 2
    assert config.resources[0].resource_kind == "bigquery"
    assert config.resources[1].resource_kind == "dbt"


def test_load_config_with_json_file(config_builder, sample_config_data, tmp_path):
    config_file = tmp_path / "dagster_config.json"
    with open(config_file, "w") as f:
        json.dump(sample_config_data, f)

    config_builder.load_config(None, tmp_path)
    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert len(config.resources) == 2


def test_load_config_multiple_files(config_builder, tmp_path):
    (tmp_path / "dagster_config.json").touch()
    (tmp_path / "dagster_config.yaml").touch()

    with pytest.raises(ValueError, match="Multiple configuration files found"):
        config_builder.load_config(None, tmp_path)


def test_load_config_no_file(config_builder, tmp_path):
    config_builder.load_config(None, tmp_path)
    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert len(config.resources) == 0


def test_resource_config_map(config_builder, sample_config_data):
    config_builder.load_config(sample_config_data, None)
    resource_config = config_builder.resource_config_map
    assert "bigquery" in resource_config
    assert resource_config["bigquery"]["project"] == "test-project"
    assert resource_config["bigquery"]["location"] == "US"


def test_resource_class_map(config_builder, sample_config_data):
    config_builder.load_config(sample_config_data, None)
    resource_class_map = config_builder.resource_class_map
    assert "bigquery" in resource_class_map
    assert resource_class_map["bigquery"].project == "test-project"
    assert resource_class_map["bigquery"].location == "US"
