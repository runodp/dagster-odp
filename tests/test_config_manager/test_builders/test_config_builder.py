import json

import pytest

from dagster_vayu.config_manager.builders.config_builder import ConfigBuilder
from dagster_vayu.config_manager.models.config_model import DagsterConfig


@pytest.fixture
def sample_config_data():
    return {
        "resources": {
            "bigquery": {"project": "test-project", "location": "US"},
            "dbt": {"project_dir": "/path/to/dbt", "profile": "test_profile"},
        },
        "tasks": [
            {
                "name": "test_task",
                "required_resources": ["bigquery", "dbt"],
                "compute_kind": "dbt",
            }
        ],
        "sensors": [{"name": "test_sensor", "required_resources": ["bigquery"]}],
    }


def test_load_config_from_data(sample_config_data):
    config_builder = ConfigBuilder()
    config_builder.load_config(sample_config_data, None)

    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert config.resources.bigquery.project == "test-project"
    assert config.resources.dbt.project_dir == "/path/to/dbt"
    assert len(config.tasks) == 1
    assert len(config.sensors) == 1


def test_load_config_from_file(sample_config_data, tmp_path):
    config_file = tmp_path / "dagster_config.json"
    with config_file.open("w") as f:
        json.dump(sample_config_data, f)

    config_builder = ConfigBuilder()
    config_builder.load_config(None, tmp_path)

    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert config.resources.bigquery.project == "test-project"
    assert config.resources.dbt.project_dir == "/path/to/dbt"
    assert len(config.tasks) == 1
    assert len(config.sensors) == 1


def test_load_config_with_no_data_no_file():
    config_builder = ConfigBuilder()
    config_builder.load_config(None, None)

    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert config.resources == DagsterConfig().resources
    assert config.tasks == []
    assert config.sensors == []
