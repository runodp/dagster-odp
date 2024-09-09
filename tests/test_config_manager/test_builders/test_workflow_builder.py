import json

import pytest

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.workflow_model import (
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
            "dbt_params": {"source_name": "test_source", "table_name": "test_table"},
        },
        {
            "asset_key": "asset2",
            "task_type": "dbt",
            "params": {"selection": "test_model", "dbt_vars": {}},
        },
        {
            "asset_key": "asset3",
            "task_type": "dlt",
            "params": {
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
    assert workflow_builder.asset_key_type_map["asset3"] == DLTTask

    assert len(workflow_builder.asset_key_dbt_params_map) == 1
    assert (
        workflow_builder.asset_key_dbt_params_map["asset1"].source_name == "test_source"
    )

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
    file1 = tmp_path / "file1.json"
    file2 = tmp_path / "file2.json"
    file1.write_text(json.dumps(SAMPLE_WORKFLOW_CONFIG))
    file2.write_text(
        json.dumps(
            {
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
        )
    )

    workflow_builder = WorkflowBuilder()
    result = workflow_builder._consolidate_workflow_data(tmp_path)

    assert len(result["jobs"]) == 2
    assert len(result["assets"]) == 4
    assert len(result["partitions"]) == 2
    assert set(job["job_id"] for job in result["jobs"]) == {"test_job", "second_job"}
    assert set(asset["asset_key"] for asset in result["assets"]) == {
        "asset1",
        "asset2",
        "asset3",
        "second_asset",
    }
    assert set(
        partition["params"]["schedule_type"] for partition in result["partitions"]
    ) == {"MONTHLY", "DAILY"}
