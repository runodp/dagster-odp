import json

import pytest
from dagster import ConfigurableResource

from dagster_vayu.config_manager.builders.config_builder import ConfigBuilder
from dagster_vayu.config_manager.models.config_model import DagsterConfig
from dagster_vayu.resources.resource_registry import vayu_resource


# Mock ConfigurableResource classes for testing
@vayu_resource("bigquery")
class MockBigQueryResource(ConfigurableResource):
    project: str
    location: str


@vayu_resource("dbt")
class MockDbtResource(ConfigurableResource):
    project_dir: str
    profile: str


@pytest.fixture
def sample_config_data():
    return {
        "resources": [
            {
                "resource_kind": "bigquery",
                "params": {"project": "test-project", "location": "US"},
            },
        ],
        "tasks": [{"name": "test_task", "required_resources": ["bigquery"]}],
    }


@pytest.fixture
def sample_file_config(tmp_path):
    config = {
        "resources": [
            {
                "resource_kind": "dbt",
                "params": {"project_dir": "/path/to/dbt", "profile": "test_profile"},
            },
        ],
        "sensors": [{"name": "test_sensor", "required_resources": ["bigquery"]}],
    }
    config_file = tmp_path / "dagster_config.json"
    config_file.write_text(json.dumps(config))
    return tmp_path


def test_load_config_merging(sample_config_data, sample_file_config):
    builder = ConfigBuilder()
    builder.load_config(sample_config_data, sample_file_config)

    config = builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert len(config.resources) == 2
    assert len(config.tasks) == 1
    assert len(config.sensors) == 1

    resource_kinds = [r.resource_kind for r in config.resources]
    assert "bigquery" in resource_kinds
    assert "dbt" in resource_kinds


def test_resource_config_map(sample_config_data):
    builder = ConfigBuilder()
    builder.load_config(sample_config_data, None)

    resource_config = builder.resource_config_map
    assert isinstance(resource_config, dict)
    assert "bigquery" in resource_config
    assert resource_config["bigquery"] == {"project": "test-project", "location": "US"}


def test_resource_class_map(sample_config_data):
    builder = ConfigBuilder()
    builder.load_config(sample_config_data, None)

    resource_class_map = builder.resource_class_map
    assert isinstance(resource_class_map, dict)
    assert "bigquery" in resource_class_map
    assert isinstance(resource_class_map["bigquery"], MockBigQueryResource)
    assert resource_class_map["bigquery"].project == "test-project"
    assert resource_class_map["bigquery"].location == "US"
