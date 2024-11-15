import json

import pytest
import yaml

from dagster_odp.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_odp.config_manager.models.workflow_model import (
    DBTTask,
    DLTTask,
    GenericTask,
    WorkflowConfig,
)

SAMPLE_WORKFLOW_CONFIG = {
    "jobs": [
        {
            "job_id": "test_job",
            "triggers": [
                {
                    "trigger_id": "test_schedule",
                    "trigger_type": "schedule",
                    "params": {"schedule_kind": "partition", "schedule_params": {}},
                },
                {
                    "trigger_id": "test_sensor_1",
                    "trigger_type": "sensor",
                    "params": {
                        "sensor_kind": "gcs_sensor",
                        "sensor_params": {"bucket_name": "test-bucket-1"},
                    },
                },
            ],
            "asset_selection": ["asset1", "asset2", "asset3"],
        }
    ],
    "assets": [
        {
            "asset_key": "asset1",
            "task_type": "gcs_file_to_bq",
            "params": {
                "source_file_uri": "gs://test-bucket/test-file.parquet",
                "destination_table_id": "test_dataset.test_table",
                "job_config_params": {},
            },
        },
        {
            "asset_key": "asset2",
            "task_type": "dbt",
            "params": {"selection": "test_model", "dbt_vars": {}},
        },
        {
            "asset_key": "source_name/asset3",
            "task_type": "dlt",
            "params": {
                "schema_file_path": "schemas/export/test_function.schema.yaml",
                "source_module": "test_module.test_function",
                "source_params": {},
                "destination": "bigquery",
                "destination_params": {},
                "pipeline_params": {"dataset_name": "test_dataset"},
            },
        },
    ],
    "partitions": [
        {
            "assets": ["asset1", "asset2", "asset3"],
            "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
        }
    ],
}


@pytest.fixture
def workflow_builder():
    return WorkflowBuilder(config_data=SAMPLE_WORKFLOW_CONFIG)


def test_workflow_builder_initialization(workflow_builder):
    assert isinstance(workflow_builder._config, WorkflowConfig)
    assert len(workflow_builder._config.jobs) == 1
    assert len(workflow_builder._config.assets) == 3
    assert len(workflow_builder._config.partitions) == 1


def test_property_methods(workflow_builder):
    assert len(workflow_builder.triggers) == 1
    assert len(workflow_builder.generic_assets) == 1
    assert len(workflow_builder.sensors) == 1
    assert len(workflow_builder.jobs) == 1
    assert len(workflow_builder.partitions) == 1
    assert workflow_builder.job_ids == ["test_job"]


def test_asset_related_methods(workflow_builder):
    assert len(workflow_builder.get_assets_with_task_type(DBTTask)) == 1
    assert len(workflow_builder.get_assets_with_task_type(DLTTask)) == 1
    assert len(workflow_builder.get_assets_with_task_type(GenericTask)) == 1

    assert len(workflow_builder.asset_key_type_map) == 3
    assert workflow_builder.asset_key_type_map["asset1"] == GenericTask
    assert workflow_builder.asset_key_type_map["asset2"] == DBTTask
    assert workflow_builder.asset_key_type_map["source_name/asset3"] == DLTTask

    assert len(workflow_builder.asset_key_partition_map) == 3
    assert all(
        param.schedule_type == "MONTHLY"
        for param in workflow_builder.asset_key_partition_map.values()
    )


def test_job_id_trigger_map(workflow_builder):
    schedule_map = workflow_builder.job_id_trigger_map("schedule")
    assert len(schedule_map) == 1
    assert len(schedule_map["test_job"]) == 1
    assert schedule_map["test_job"][0].trigger_id == "test_schedule"

    with pytest.raises(ValueError):
        workflow_builder.job_id_trigger_map("invalid_type")


def test_consolidate_workflow_data(tmp_path):
    # Create JSON file
    file1 = tmp_path / "file1.json"
    file1.write_text(json.dumps(SAMPLE_WORKFLOW_CONFIG))

    # Create YAML file
    file2 = tmp_path / "file2.yaml"
    yaml_data = {
        "jobs": [
            {
                "job_id": "second_job",
                "triggers": [],
                "asset_selection": ["second_asset"],
            }
        ],
        "assets": [
            {
                "asset_key": "second_asset",
                "task_type": "bq_table_to_gcs",
                "params": {
                    "source_table_id": "test_dataset.test_table",
                    "destination_file_uri": "gs://test-bucket/output.parquet",
                    "job_config_params": {},
                },
            }
        ],
        "partitions": [
            {
                "assets": ["second_asset"],
                "params": {"start": "2023-06-01", "schedule_type": "DAILY"},
            }
        ],
    }
    with open(file2, "w") as f:
        yaml.dump(yaml_data, f)

    # Create YML file
    file3 = tmp_path / "file3.yml"
    yml_data = {
        "jobs": [
            {
                "job_id": "third_job",
                "triggers": [],
                "asset_selection": ["third_asset"],
            }
        ],
        "assets": [
            {
                "asset_key": "third_asset",
                "task_type": "gcs_file_to_bq",
                "params": {
                    "source_file_uri": "gs://test-bucket/test-file.parquet",
                    "destination_table_id": "test_dataset.another_table",
                    "job_config_params": {},
                },
            }
        ],
        "partitions": [],
    }
    with open(file3, "w") as f:
        yaml.dump(yml_data, f)

    workflow_builder = WorkflowBuilder()
    result = workflow_builder._consolidate_workflow_data(tmp_path)

    assert len(result["jobs"]) == 3
    assert len(result["assets"]) == 5
    assert len(result["partitions"]) == 2

    # Test that files with unsupported extensions are ignored
    unsupported_file = tmp_path / "unsupported.txt"
    unsupported_file.write_text("This should be ignored")

    result_with_unsupported = workflow_builder._consolidate_workflow_data(tmp_path)
    assert result == result_with_unsupported
