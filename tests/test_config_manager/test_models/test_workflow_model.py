import pytest
from pydantic import ValidationError

from dagster_vayu.config_manager.models.workflow_model import (
    DLTTask,
    ScheduleCronTrigger,
    WorkflowConfig,
    task_discriminator,
)


def test_validate_cron():
    # Test valid cron schedule
    valid_cron = ScheduleCronTrigger(
        **{
            "schedule_params": {"cron_schedule": "0 * * * *"},
            "schedule_kind": "cron",
        }
    )
    assert valid_cron

    # Test invalid cron schedule
    with pytest.raises(ValidationError, match="not a valid cron schedule"):
        ScheduleCronTrigger(
            **{
                "schedule_params": {"cron_schedule": "invalid cron"},
                "schedule_kind": "cron",
            }
        )


def test_validate_pipeline():
    # Test valid DLT task
    valid_dlt_task = DLTTask(
        **{
            "asset_key": "valid_dlt_task",
            "task_type": "dlt",
            "params": {
                "source_module": "test_module",
                "source_params": {},
                "destination": "bigquery",
                "destination_params": {},
                "pipeline_params": {"dataset_name": "test_dataset"},
            },
        }
    )
    assert valid_dlt_task

    # Test invalid DLT task (missing dataset_name)
    with pytest.raises(ValidationError, match="dataset_name is required"):
        DLTTask(
            **{
                "asset_key": "invalid_dlt_task",
                "task_type": "dlt",
                "params": {
                    "source_module": "test_module",
                    "source_params": {},
                    "destination": "bigquery",
                    "destination_params": {},
                    "pipeline_params": {},
                },
            },
        )


@pytest.mark.parametrize(
    "input_data,expected_output",
    [
        ({"task_type": "dlt"}, "dlt"),
        ({"task_type": "gcs_file_to_bq"}, "generic"),
        ({"task_type": "bq_table_to_gcs"}, "generic"),
        ({"task_type": "dbt"}, "dbt"),
        ({"task_type": "custom_task"}, "generic"),
    ],
)
def test_task_discriminator(input_data, expected_output):
    assert task_discriminator(input_data) == expected_output

    # Test with object-like input
    class MockTask:
        def __init__(self, task_type):
            self.task_type = task_type

    mock_task = MockTask(input_data["task_type"])
    assert task_discriminator(mock_task) == expected_output


def test_validate_partition_schedule():
    # Test that only one partition schedule is allowed

    invalid_config = {
        "jobs": [
            {
                "job_id": "job1",
                "triggers": [
                    {
                        "trigger_id": "trigger1",
                        "trigger_type": "schedule",
                        "params": {"schedule_kind": "partition", "schedule_params": {}},
                    },
                    {
                        "trigger_id": "trigger2",
                        "trigger_type": "schedule",
                        "params": {"schedule_kind": "partition", "schedule_params": {}},
                    },
                ],
                "asset_selection": set(),
            }
        ],
        "assets": [],
        "partitions": [],
    }
    with pytest.raises(ValidationError, match="Only one partition schedule is allowed"):
        WorkflowConfig(**invalid_config)


def test_validate_unique_ids():
    # Test duplicate job_id
    invalid_config_duplicate_job = {
        "jobs": [
            {
                "job_id": "job1",
                "triggers": [
                    {
                        "trigger_id": "trigger1",
                        "trigger_type": "sensor",
                        "params": {
                            "sensor_kind": "gcs_sensor",
                            "sensor_params": {"bucket_name": "test-bucket-1"},
                        },
                    }
                ],
                "asset_selection": set(),
            },
            {
                "job_id": "job1",  # Duplicate job_id
                "triggers": [
                    {
                        "trigger_id": "trigger2",
                        "trigger_type": "sensor",
                        "params": {
                            "sensor_kind": "gcs_sensor",
                            "sensor_params": {"bucket_name": "test-bucket-1"},
                        },
                    }
                ],
                "asset_selection": set(),
            },
        ],
        "assets": [],
        "partitions": [],
    }
    with pytest.raises(ValidationError, match="Duplicate job_id: job1"):
        WorkflowConfig.model_validate(
            invalid_config_duplicate_job, context={"consolidated": True}
        )

    # Test duplicate trigger_id
    invalid_config_duplicate_trigger = {
        "jobs": [
            {
                "job_id": "job1",
                "triggers": [
                    {
                        "trigger_id": "trigger1",
                        "trigger_type": "sensor",
                        "params": {
                            "sensor_kind": "gcs_sensor",
                            "sensor_params": {"bucket_name": "test-bucket-1"},
                        },
                    },
                    {
                        "trigger_id": "trigger1",  # Duplicate trigger_id
                        "trigger_type": "sensor",
                        "params": {
                            "sensor_kind": "gcs_sensor",
                            "sensor_params": {"bucket_name": "test-bucket-2"},
                        },
                    },
                ],
                "asset_selection": set(),
            },
        ],
        "assets": [],
        "partitions": [],
    }
    with pytest.raises(ValidationError, match="Duplicate trigger_id found: trigger1"):
        WorkflowConfig.model_validate(
            invalid_config_duplicate_trigger, context={"consolidated": True}
        )

    # Test duplicate asset_key
    invalid_config_duplicate_asset = {
        "jobs": [],
        "assets": [
            {
                "asset_key": "asset1",
                "task_type": "gcs_file_to_bq",
                "params": {
                    "source_file_uri": "gs://test-bucket/test-file1.parquet",
                    "destination_table_id": "test_dataset.test_table1",
                },
            },
            {
                "asset_key": "asset1",  # Duplicate asset_key
                "task_type": "gcs_file_to_bq",
                "params": {
                    "source_file_uri": "gs://test-bucket/test-file2.parquet",
                    "destination_table_id": "test_dataset.test_table2",
                },
            },
        ],
        "partitions": [],
    }
    with pytest.raises(ValidationError, match="Duplicate asset key found: asset1"):
        WorkflowConfig.model_validate(
            invalid_config_duplicate_asset, context={"consolidated": True}
        )


def test_validate_partitioned_assets():
    # Test that assets in partitions are defined and not duplicated

    invalid_config_undefined_asset = {
        "jobs": [],
        "assets": [
            {
                "asset_key": "asset1",
                "task_type": "gcs_file_to_bq",
                "params": {
                    "source_file_uri": "gs://test-bucket/test-file.parquet",
                    "destination_table_id": "test_dataset.test_table",
                },
            }
        ],
        "partitions": [
            {
                "assets": ["asset2"],  # asset2 is not defined
                "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
            }
        ],
    }
    with pytest.raises(
        ValidationError, match="Asset 'asset2' in partition is not defined as a asset"
    ):
        WorkflowConfig.model_validate(
            invalid_config_undefined_asset, context={"consolidated": True}
        )

    invalid_config_duplicate_asset = {
        "jobs": [],
        "assets": [
            {
                "asset_key": "asset1",
                "task_type": "gcs_file_to_bq",
                "params": {
                    "source_file_uri": "gs://test-bucket/test-file.parquet",
                    "destination_table_id": "test_dataset.test_table",
                },
            },
            {
                "asset_key": "asset2",
                "task_type": "bq_table_to_gcs",
                "params": {
                    "destination_file_uri": "gs://test-bucket/output/test-file.parquet",
                    "source_table_id": "test_dataset.test_table",
                },
            },
        ],
        "partitions": [
            {
                "assets": ["asset1"],
                "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
            },
            {
                "assets": ["asset1"],  # asset1 is duplicated
                "params": {"start": "2023-01-01", "schedule_type": "DAILY"},
            },
        ],
    }
    with pytest.raises(
        ValidationError, match="Asset 'asset1' is defined in multiple partitions"
    ):
        WorkflowConfig.model_validate(
            invalid_config_duplicate_asset, context={"consolidated": True}
        )
