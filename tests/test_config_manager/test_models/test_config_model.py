import pytest

from dagster_vayu.config_manager.models.config_model import validate_resource_names


def test_validate_resource_names():
    # Assume these resources are registered in the resource_registry
    valid_resources = ["dbt", "dlt"]

    # Test valid resource names
    for resource in valid_resources:
        assert validate_resource_names(resource) == resource

    # Test invalid resource name
    with pytest.raises(ValueError) as excinfo:
        validate_resource_names("invalid_resource")
    assert (
        str(excinfo.value) == "Resource invalid_resource not defined in ResourceConfig"
    )
