import pytest
from pydantic import ValidationError

from dagster_odp.config_manager.models.workflow_model import (
    DLTParams,
    DLTTask,
    ScheduleCronParams,
    ScheduleCronTrigger,
    WorkflowConfig,
    task_discriminator,
)


def test_validate_cron():
    valid_cron = ScheduleCronTrigger(
        schedule_params=ScheduleCronParams(cron_schedule="0 * * * *"),
        schedule_kind="cron",
    )
    assert valid_cron

    with pytest.raises(ValidationError, match="not a valid cron schedule"):
        ScheduleCronTrigger(
            schedule_params=ScheduleCronParams(cron_schedule="invalid cron"),
            schedule_kind="cron",
        )


def test_validate_dlt_task():
    valid_dlt_task = DLTTask(
        asset_key="valid_dlt_task",
        task_type="dlt",
        params=DLTParams(
            source_module="test_module",
            source_params={},
            destination="bigquery",
            destination_params={},
            pipeline_params={"dataset_name": "test_dataset"},
        ),
    )
    assert valid_dlt_task

    with pytest.raises(ValidationError, match="dataset_name is required"):
        DLTTask(
            asset_key="invalid_dlt_task",
            task_type="dlt",
            params=DLTParams(
                source_module="test_module",
                source_params={},
                destination="bigquery",
                destination_params={},
                pipeline_params={},
            ),
        )


@pytest.mark.parametrize(
    "task_type,expected_output",
    [
        ("dlt", "dlt"),
        ("gcs_file_to_bq", "generic"),
        ("bq_table_to_gcs", "generic"),
        ("dbt", "dbt"),
        ("custom_task", "generic"),
    ],
)
def test_task_discriminator(task_type, expected_output):
    assert task_discriminator({"task_type": task_type}) == expected_output
    assert (
        task_discriminator(type("MockTask", (), {"task_type": task_type})())
        == expected_output
    )


def test_validate_partition_schedule():
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
    configs = [
        (
            "job_id",
            {
                "jobs": [
                    {
                        "job_id": "job1",
                        "triggers": [
                            {
                                "trigger_id": "t1",
                                "trigger_type": "sensor",
                                "params": {
                                    "sensor_kind": "gcs_sensor",
                                    "sensor_params": {"bucket_name": "b1"},
                                },
                            }
                        ],
                        "asset_selection": set(),
                    },
                    {
                        "job_id": "job1",
                        "triggers": [
                            {
                                "trigger_id": "t2",
                                "trigger_type": "sensor",
                                "params": {
                                    "sensor_kind": "gcs_sensor",
                                    "sensor_params": {"bucket_name": "b2"},
                                },
                            }
                        ],
                        "asset_selection": set(),
                    },
                ],
                "assets": [],
                "partitions": [],
            },
            "Duplicate job_id: job1",
        ),
        (
            "trigger_id",
            {
                "jobs": [
                    {
                        "job_id": "job1",
                        "triggers": [
                            {
                                "trigger_id": "t1",
                                "trigger_type": "sensor",
                                "params": {
                                    "sensor_kind": "gcs_sensor",
                                    "sensor_params": {"bucket_name": "b1"},
                                },
                            },
                            {
                                "trigger_id": "t1",
                                "trigger_type": "sensor",
                                "params": {
                                    "sensor_kind": "gcs_sensor",
                                    "sensor_params": {"bucket_name": "b2"},
                                },
                            },
                        ],
                        "asset_selection": set(),
                    },
                ],
                "assets": [],
                "partitions": [],
            },
            "Duplicate trigger_id found: t1",
        ),
        (
            "asset_key",
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
                        "asset_key": "asset1",
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
    ]

    for _, invalid_config, error_message in configs:
        with pytest.raises(ValidationError, match=error_message):
            WorkflowConfig.model_validate(
                invalid_config, context={"consolidated": True}
            )


def test_validate_partitioned_assets():
    configs = [
        (
            "undefined_asset",
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
                        "assets": ["asset2"],
                        "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
                    }
                ],
            },
            "Asset 'asset2' in partition is not defined as a asset",
        ),
        (
            "duplicate_asset",
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
                        "asset_key": "asset2",
                        "task_type": "bq_table_to_gcs",
                        "params": {
                            "destination_file_uri": "gs://b1/f2.parquet",
                            "source_table_id": "d1.t1",
                        },
                    },
                ],
                "partitions": [
                    {
                        "assets": ["asset1"],
                        "params": {"start": "2023-01-01", "schedule_type": "MONTHLY"},
                    },
                    {
                        "assets": ["asset1"],
                        "params": {"start": "2023-01-01", "schedule_type": "DAILY"},
                    },
                ],
            },
            "Asset 'asset1' is defined in multiple partitions",
        ),
    ]

    for _, invalid_config, error_message in configs:
        with pytest.raises(ValidationError, match=error_message):
            WorkflowConfig.model_validate(
                invalid_config, context={"consolidated": True}
            )
