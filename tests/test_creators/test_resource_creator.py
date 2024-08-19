from dagster_gcp import GCSResource

from dagster_vayu.creators.resource_creator import (
    _filter_resource_params,
    get_dagster_resources,
)
from dagster_vayu.resources import VayuBigQueryResource, VayuDltResource

SAMPLE_CONFIG = {
    "bigquery": {"project": "test-project", "location": "us-east1"},
    "gcs": {"project": "test-project"},
    "dlt": {"project_dir": "/path/to/dlt"},
}


def test_get_dagster_resources():
    resources = get_dagster_resources(SAMPLE_CONFIG)
    assert isinstance(resources["bigquery"], VayuBigQueryResource)
    assert isinstance(resources["gcs"], GCSResource)
    assert isinstance(resources["dlt"], VayuDltResource)
    assert resources["bigquery"].project == "test-project"
    assert resources["bigquery"].location == "us-east1"
    assert resources["gcs"].project == "test-project"
    assert resources["dlt"].project_dir == "/path/to/dlt"


def test_get_dagster_resources_empty():
    empty_config = {}
    resources = get_dagster_resources(empty_config)
    assert resources == {}


def test_filter_resource_params():
    params = {"project_dir": "/path/to/dbt", "sources_file_path_": "models/sources.yml"}
    filtered_params = _filter_resource_params(params)
    assert filtered_params == {"project_dir": "/path/to/dbt"}
    assert "sources_file_path_" not in filtered_params


def test_get_dagster_resources_invalid_resource():
    invalid_config = {"invalid_resource": {"some_param": "some_value"}}
    resources = get_dagster_resources(invalid_config)
    assert resources == {}


def test_get_dagster_resources_empty_params():
    config_with_empty_params = {
        "bigquery": {},
        "gcs": {"project": "test-project"},
    }
    resources = get_dagster_resources(config_with_empty_params)
    assert "bigquery" not in resources
    assert isinstance(resources["gcs"], GCSResource)
    assert resources["gcs"].project == "test-project"
