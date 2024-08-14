import pytest
from pydantic import ValidationError
from task_nicely_core.configs.models.config_model import (
    BigQueryResourceParams,
    DagsterConfig,
    DbtResourceParams,
    GcsResourceParams,
    ResourceConfig,
    Task,
)


def test_validate_resource_names():
    # Define a sample ResourceConfig
    resource_config = ResourceConfig(
        bigquery=BigQueryResourceParams(
            **{"project": "test-project", "location": "us-east1"}
        ),
        gcs=GcsResourceParams(**{"project": "test-project"}),
        dbt=DbtResourceParams(
            **{"project_dir": "/path/to/dbt", "profile": "test_profile"}
        ),
    )

    # Test valid resource name
    valid_task = Task(name="test_task", required_resources=["bigquery"])
    config = DagsterConfig(resources=resource_config, tasks=[valid_task])
    assert config.tasks[0].required_resources == ["bigquery"]

    # Test invalid resource name
    with pytest.raises(
        ValidationError, match="Resource invalid_resource not defined in ResourceConfig"
    ):
        invalid_task = Task(name="test_task", required_resources=["invalid_resource"])
        DagsterConfig(resources=resource_config, tasks=[invalid_task])

    # Test mix of valid and invalid resource names
    with pytest.raises(
        ValidationError, match="Resource invalid_resource not defined in ResourceConfig"
    ):
        mixed_task = Task(
            name="test_task", required_resources=["bigquery", "invalid_resource"]
        )
        DagsterConfig(resources=resource_config, tasks=[mixed_task])

    # Test empty required_resources
    empty_resources_task = Task(name="test_task", required_resources=[])
    config = DagsterConfig(resources=resource_config, tasks=[empty_resources_task])
    assert config.tasks[0].required_resources == []
