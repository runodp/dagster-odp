import pytest
from dagster import ConfigurableResource

from dagster_vayu.config_manager.builders.config_builder import ConfigBuilder
from dagster_vayu.config_manager.models.config_model import (
    DagsterConfig,
    GenericResource,
)
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
            {
                "resource_kind": "dbt",
                "params": {"project_dir": "/path/to/dbt", "profile": "test_profile"},
            },
        ],
        "tasks": [{"name": "test_task", "required_resources": ["bigquery", "dbt"]}],
        "sensors": [{"name": "test_sensor", "required_resources": ["bigquery"]}],
    }


@pytest.fixture
def config_builder(sample_config_data):
    builder = ConfigBuilder()
    builder.load_config(sample_config_data, None)
    return builder


def test_load_config(config_builder):
    config = config_builder.get_config()
    assert isinstance(config, DagsterConfig)
    assert len(config.resources) == 2
    assert isinstance(config.resources[0], GenericResource)
    assert config.resources[0].resource_kind == "bigquery"
    assert isinstance(config.resources[0].params, MockBigQueryResource)


def test_resource_config_map(config_builder):
    resource_config = config_builder.resource_config_map
    assert isinstance(resource_config, dict)
    assert "bigquery" in resource_config
    assert resource_config["bigquery"] == {"project": "test-project", "location": "US"}


def test_resource_class_map(config_builder):
    resource_class_map = config_builder.resource_class_map
    assert isinstance(resource_class_map, dict)
    assert "bigquery" in resource_class_map
    assert isinstance(resource_class_map["bigquery"], MockBigQueryResource)
    assert resource_class_map["bigquery"].project == "test-project"
    assert resource_class_map["bigquery"].location == "US"
