import pytest

from dagster_vayu.config_manager.models.config_model import (
    ResourceConfig,
    validate_resource_names,
)


def test_validate_resource_names():
    # Test valid resource names
    assert validate_resource_names("bigquery") == "bigquery"
    assert validate_resource_names("dbt") == "dbt"
    assert validate_resource_names("gcs") == "gcs"
    assert validate_resource_names("dlt") == "dlt"

    # Test invalid resource name
    with pytest.raises(ValueError) as excinfo:
        validate_resource_names("invalid_resource")
    assert (
        str(excinfo.value) == "Resource invalid_resource not defined in ResourceConfig"
    )

    # Test empty string
    with pytest.raises(ValueError) as excinfo:
        validate_resource_names("")
    assert str(excinfo.value) == "Resource  not defined in ResourceConfig"

    # Ensure ResourceConfig hasn't changed unexpectedly
    assert set(ResourceConfig.model_fields.keys()) == {"bigquery", "dbt", "gcs", "dlt"}
