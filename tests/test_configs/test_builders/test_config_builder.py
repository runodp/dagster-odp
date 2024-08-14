import pytest
from dagster_gcp import BigQueryResource, GCSResource
from task_nicely_core.configs.builders.config_builder import ConfigBuilder

SAMPLE_CONFIG = {
    "resources": {
        "bigquery": {"project": "test-project", "location": "us-east1"},
        "gcs": {"project": "test-project"},
    },
    "tasks": [
        {
            "name": "test_task",
            "required_resources": ["bigquery", "gcs"],
            "compute_kind": "bigquery",
            "storage_kind": "gcs",
        }
    ],
}


@pytest.fixture
def config_builder():
    return ConfigBuilder(config_data=SAMPLE_CONFIG)


def test_get_dagster_resources(config_builder):
    resources = config_builder.get_dagster_resources()
    assert isinstance(resources["bigquery"], BigQueryResource)
    assert isinstance(resources["gcs"], GCSResource)
    assert resources["bigquery"].project == "test-project"
    assert resources["bigquery"].location == "us-east1"
    assert resources["gcs"].project == "test-project"


def test_get_dagster_resources_empty():
    empty_config = {"resources": {}, "tasks": []}
    config_builder = ConfigBuilder(config_data=empty_config)
    resources = config_builder.get_dagster_resources()
    assert resources == {}


def test_filter_resource_params(config_builder):
    params = {"project_dir": "/path/to/dbt", "sources_file_path_": "models/sources.yml"}
    filtered_params = config_builder._filter_resource_params(params)
    assert filtered_params == {"project_dir": "/path/to/dbt"}
    assert "sources_file_path_" not in filtered_params
