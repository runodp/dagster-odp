import pytest
from pydantic import ValidationError

from dagster_odp.config_manager.models.workflow_model import (
    DLTParams,
    DLTTask,
    PartitionParams,
    ScheduleCronTrigger,
    WorkflowConfig,
    task_discriminator,
)


@pytest.mark.parametrize(
    "config,expected_error",
    [
        # Cron schedule tests
        (
            {
                "schedule_params": {"cron_schedule": "0 * * * *"},
                "schedule_kind": "cron",
            },
            None,
        ),
        (
            {"schedule_params": {"cron_schedule": "invalid"}, "schedule_kind": "cron"},
            "not a valid cron schedule",
        ),
        # Partition schedule tests
        ({"start": "2023-01-01", "schedule_type": "DAILY"}, None),
        (
            {"start": "2023-01-01"},
            "One of schedule_type and cron_schedule must be provided",
        ),
        (
            {
                "cron_schedule": "0 0 * * *",
                "schedule_type": "DAILY",
                "start": "2023-01-01",
            },
            "If cron_schedule argument is provided, then schedule_type, minute_offset, "
            "hour_offset, and day_offset can't also be provided",
        ),
    ],
)
def test_schedule_validation(config, expected_error):
    """Test schedule configuration validation for both cron and partition schedules."""
    cls = ScheduleCronTrigger if "schedule_params" in config else PartitionParams

    if expected_error:
        with pytest.raises(ValidationError, match=expected_error):
            cls(**config)
    else:
        assert cls(**config)


def test_dlt_dataset_name_validation():
    """Test DLT dataset name validation."""

    with pytest.raises(ValidationError, match="dataset_name is required"):
        DLTParams(
            schema_file_path="schemas/export/test_function.schema.yaml",
            source_module="test_module",
            source_params={},
            destination="bigquery",
            destination_params={},
            pipeline_params={},  # Empty pipeline_params to trigger validation
        )


@pytest.mark.parametrize(
    "asset_key,expected_error",
    [
        ("github/pull_requests", None),  # Valid - has namespace and resource
        ("data/etl/customers", None),  # Valid - multiple levels
        (
            "pull_requests",
            "DLT asset key 'pull_requests' must contain at least one '/' "
            "to specify both external asset name and resource name",
        ),  # Invalid - missing namespace
    ],
)
def test_dlt_asset_key_format(asset_key, expected_error):
    """Test DLT asset key format validation."""
    config = {
        "asset_key": asset_key,
        "task_type": "dlt",
        "params": DLTParams(
            schema_file_path="schemas/export/test_function.schema.yaml",
            source_module="test_module",
            source_params={},
            destination="bigquery",
            destination_params={},
            pipeline_params={"dataset_name": "test_dataset"},
        ),
    }

    if expected_error is None:
        task = DLTTask(**config)
        assert task.asset_key == asset_key
    else:
        with pytest.raises(ValidationError, match=expected_error):
            DLTTask(**config)


@pytest.mark.parametrize(
    "task_type,expected_output",
    [
        ("dlt", "dlt"),  # DLT specific task
        ("dbt", "dbt"),  # DBT specific task
        ("custom_task", "generic"),  # Generic task case
    ],
)
def test_task_discriminator(task_type, expected_output):
    """Test task type discrimination logic."""
    assert task_discriminator({"task_type": task_type}) == expected_output


@pytest.mark.parametrize(
    "config,expected_error",
    [
        # Multiple partition schedules - not allowed
        (
            {
                "jobs": [
                    {
                        "job_id": "job1",
                        "triggers": [
                            {
                                "trigger_id": "t1",
                                "trigger_type": "schedule",
                                "params": {
                                    "schedule_kind": "partition",
                                    "schedule_params": {},
                                },
                            },
                            {
                                "trigger_id": "t2",
                                "trigger_type": "schedule",
                                "params": {
                                    "schedule_kind": "partition",
                                    "schedule_params": {},
                                },
                            },
                        ],
                        "asset_selection": set(),
                    }
                ],
                "assets": [],
                "partitions": [],
            },
            "Only one partition schedule is allowed",
        ),
        # Duplicate job IDs - not allowed
        (
            {
                "jobs": [
                    {"job_id": "job1", "triggers": [], "asset_selection": set()},
                    {"job_id": "job1", "triggers": [], "asset_selection": set()},
                ],
                "assets": [],
                "partitions": [],
            },
            "Duplicate job_id: job1",
        ),
        # Duplicate trigger IDs across jobs
        (
            {
                "jobs": [
                    {
                        "job_id": "job1",
                        "triggers": [
                            {
                                "trigger_id": "trigger1",
                                "trigger_type": "schedule",
                                "params": {
                                    "schedule_kind": "cron",
                                    "schedule_params": {"cron_schedule": "@daily"},
                                },
                            }
                        ],
                        "asset_selection": set(),
                    },
                    {
                        "job_id": "job2",
                        "triggers": [
                            {
                                "trigger_id": "trigger1",  # Duplicate trigger ID
                                "trigger_type": "schedule",
                                "params": {
                                    "schedule_kind": "cron",
                                    "schedule_params": {"cron_schedule": "@hourly"},
                                },
                            }
                        ],
                        "asset_selection": set(),
                    },
                ],
                "assets": [],
                "partitions": [],
            },
            "Duplicate trigger_id found: trigger1",
        ),
        # Duplicate asset keys
        (
            {
                "jobs": [],
                "assets": [
                    {
                        "asset_key": "asset1",
                        "task_type": "gcs_file_to_bq",
                        "params": {
                            "source_file_uri": "gs://b1/f1.parquet",
                            "destination_table_id": "d1.t1",
                        },
                    },
                    {
                        "asset_key": "asset1",  # Duplicate asset key
                        "task_type": "gcs_file_to_bq",
                        "params": {
                            "source_file_uri": "gs://b1/f2.parquet",
                            "destination_table_id": "d1.t2",
                        },
                    },
                ],
                "partitions": [],
            },
            "Duplicate asset key found: asset1",
        ),
        # Undefined partitioned asset
        (
            {
                "jobs": [],
                "assets": [
                    {
                        "asset_key": "asset1",
                        "task_type": "gcs_file_to_bq",
                        "params": {
                            "source_file_uri": "gs://b1/f1.parquet",
                            "destination_table_id": "d1.t1",
                        },
                    }
                ],
                "partitions": [
                    {
                        "assets": ["asset2"],  # Asset not defined
                        "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
                    }
                ],
            },
            "Asset 'asset2' in partition is not defined as a asset",
        ),
        # Duplicate asset in partitions
        (
            {
                "jobs": [],
                "assets": [
                    {
                        "asset_key": "asset1",
                        "task_type": "gcs_file_to_bq",
                        "params": {
                            "source_file_uri": "gs://b1/f1.parquet",
                            "destination_table_id": "d1.t1",
                        },
                    }
                ],
                "partitions": [
                    {
                        "assets": ["asset1"],
                        "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
                    },
                    {
                        "assets": ["asset1"],  # Duplicate partition definition
                        "params": {"start": "2023-01-01", "schedule_type": "DAILY"},
                    },
                ],
            },
            "Asset 'asset1' is defined in multiple partitions",
        ),
    ],
)
def test_workflow_validations(config, expected_error):
    """Test workflow-level validation rules."""
    with pytest.raises(ValidationError, match=expected_error):
        WorkflowConfig.model_validate(config, context={"consolidated": True})
