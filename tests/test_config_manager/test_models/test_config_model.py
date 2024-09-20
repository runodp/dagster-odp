from unittest.mock import patch

import pytest

from dagster_vayu.config_manager.models.config_model import (
    DagsterConfig,
    GenericResource,
    validate_resource_names,
)


# Mock resource class
class MockResource:
    @classmethod
    def model_validate(cls, value):
        return value


@pytest.fixture
def mock_resource_registry():
    return {
        "resource1": MockResource,
        "resource2": MockResource,
    }


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


def test_validate_unique_resources(mock_resource_registry):
    with patch.dict(
        "dagster_vayu.config_manager.models.config_model.resource_registry",
        mock_resource_registry,
        clear=True,
    ):

        # Test duplicate resource kinds
        with pytest.raises(ValueError, match="Duplicate resource kind: resource1"):
            DagsterConfig(
                resources=[
                    GenericResource(resource_kind="resource1", params={}),
                    GenericResource(resource_kind="resource1", params={}),
                ]
            )

        # Test undefined resource kind
        with pytest.raises(
            ValueError, match="Resource kind undefined_resource must be defined"
        ):
            DagsterConfig(
                resources=[
                    GenericResource(resource_kind="undefined_resource", params={})
                ]
            )
