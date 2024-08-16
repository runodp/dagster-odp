import json

import pytest

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.workflow_model import (
    DBTTask,
    DLTTask,
    GenericTask,
    ScheduleTrigger,
    SensorTrigger,
    WorkflowConfig,
    WorkflowJob,
)

SAMPLE_WORKFLOW_CONFIG = {
    "jobs": [
        {
            "job_id": "test_job",
            "triggers": [
                {
                    "trigger_id": "test_schedule",
                    "trigger_type": "schedule",
                    "description": "Test schedule",
                    "params": {"schedule_kind": "partition"},
                },
                {
                    "trigger_id": "test_sensor_1",
                    "trigger_type": "sensor",
                    "description": "Test sensor 1",
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
            "description": "GCS to BQ task",
            "group_name": "test_group",
            "params": {
                "source_file_uri": "gs://test-bucket/test-file.parquet",
                "destination_table_id": "test_dataset.test_table",
                "job_config_params": {
                    "autodetect": True,
                    "source_format": "PARQUET",
                    "write_disposition": "WRITE_APPEND",
                },
            },
            "dbt_params": {"source_name": "test_source", "table_name": "test_table"},
        },
        {
            "asset_key": "asset2",
            "task_type": "dbt",
            "params": {
                "selection": "test_model",
                "dbt_vars": {"var1": "value1", "var2": "value2"},
            },
        },
        {
            "asset_key": "asset3",
            "task_type": "dlt",
            "description": "DLT task",
            "group_name": "test_group",
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


def test_load_config(workflow_builder):
    assert isinstance(workflow_builder._config, WorkflowConfig)
    assert len(workflow_builder._config.jobs) == 1
    assert len(workflow_builder._config.assets) == 3
    assert len(workflow_builder._config.partitions) == 1


def test_triggers(workflow_builder):
    triggers = workflow_builder.triggers
    assert len(triggers) == 1
    assert "test_job" in triggers[0]
    assert len(triggers[0]["test_job"]) == 2
    assert isinstance(triggers[0]["test_job"][0], ScheduleTrigger)
    assert isinstance(triggers[0]["test_job"][1], SensorTrigger)


def test_generic_assets(workflow_builder):
    generic_assets = workflow_builder.generic_assets
    assert len(generic_assets) == 1
    assert isinstance(generic_assets[0], GenericTask)
    assert generic_assets[0].asset_key == "asset1"


def test_sensors(workflow_builder):
    sensors = workflow_builder.sensors
    assert len(sensors) == 1
    assert isinstance(sensors[0], SensorTrigger)
    assert sensors[0].trigger_id == "test_sensor_1"


def test_jobs(workflow_builder):
    jobs = workflow_builder.jobs
    assert len(jobs) == 1
    assert isinstance(jobs[0], WorkflowJob)
    assert jobs[0].job_id == "test_job"


def test_partitions(workflow_builder):
    partitions = workflow_builder.partitions
    assert len(partitions) == 1
    assert partitions[0].assets == ["asset1", "asset2", "asset3"]


def test_get_assets_with_task_type(workflow_builder):
    dbt_assets = workflow_builder.get_assets_with_task_type(DBTTask)
    assert len(dbt_assets) == 1
    assert dbt_assets[0].asset_key == "asset2"

    dlt_assets = workflow_builder.get_assets_with_task_type(DLTTask)
    assert len(dlt_assets) == 1
    assert dlt_assets[0].asset_key == "asset3"

    generic_assets = workflow_builder.get_assets_with_task_type(GenericTask)
    assert len(generic_assets) == 1
    assert generic_assets[0].asset_key == "asset1"


def test_job_ids(workflow_builder):
    job_ids = workflow_builder.job_ids
    assert job_ids == ["test_job"]


def test_asset_key_type_map(workflow_builder):
    asset_key_type_map = workflow_builder.asset_key_type_map
    assert len(asset_key_type_map) == 3
    assert asset_key_type_map["asset1"] == GenericTask
    assert asset_key_type_map["asset2"] == DBTTask
    assert asset_key_type_map["asset3"] == DLTTask


def test_asset_key_dbt_params_map(workflow_builder):
    dbt_params_map = workflow_builder.asset_key_dbt_params_map
    assert len(dbt_params_map) == 1
    assert "asset1" in dbt_params_map
    assert dbt_params_map["asset1"].source_name == "test_source"
    assert dbt_params_map["asset1"].table_name == "test_table"


def test_asset_key_partition_map(workflow_builder):
    asset_key_partition_map = workflow_builder.asset_key_partition_map
    assert len(asset_key_partition_map) == 3
    assert "asset1" in asset_key_partition_map
    assert "asset2" in asset_key_partition_map
    assert "asset3" in asset_key_partition_map
    assert asset_key_partition_map["asset1"].schedule_type == "MONTHLY"


def test_job_id_trigger_map(workflow_builder):
    schedule_map = workflow_builder.job_id_trigger_map("schedule")
    assert len(schedule_map) == 1
    assert "test_job" in schedule_map
    assert len(schedule_map["test_job"]) == 1
    assert schedule_map["test_job"][0].trigger_id == "test_schedule"


def test_job_id_trigger_map_invalid_type(workflow_builder):
    with pytest.raises(ValueError):
        workflow_builder.job_id_trigger_map("invalid_type")


def test_consolidate_workflow_data(tmp_path):
    # Create a second config to simulate multiple files
    second_config = {
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
                "description": "Second BQ to GCS task",
                "group_name": "second_group",
                "params": {
                    "destination_file_uri": "gs://second-bucket/second-file.parquet",
                    "source_table_id": "second_dataset.second_table",
                    "job_config_params": {"destination_format": "PARQUET"},
                },
            },
        ],
        "partitions": [
            {
                "assets": ["second_asset"],
                "params": {"start": "2023-06-01", "schedule_type": "DAILY"},
            }
        ],
    }

    # Write the config files to the temporary directory
    (tmp_path / "file1.json").write_text(json.dumps(SAMPLE_WORKFLOW_CONFIG))
    (tmp_path / "file2.json").write_text(json.dumps(second_config))

    # Create a new WorkflowBuilder instance for this test
    workflow_builder = WorkflowBuilder()

    # Run the consolidate_workflow_data method
    result = workflow_builder._consolidate_workflow_data(tmp_path)

    assert "jobs" in result
    assert "assets" in result
    assert "partitions" in result

    assert len(result["jobs"]) == 2
    assert len(result["assets"]) == 4
    assert len(result["partitions"]) == 2

    assert any(job["job_id"] == "test_job" for job in result["jobs"])
    assert any(job["job_id"] == "second_job" for job in result["jobs"])

    assert any(asset["asset_key"] == "asset1" for asset in result["assets"])
    assert any(asset["asset_key"] == "second_asset" for asset in result["assets"])

    partition_assets = [partition["assets"] for partition in result["partitions"]]
    assert ["asset1", "asset2", "asset3"] in partition_assets
    assert ["second_asset"] in partition_assets

    partition_types = [
        partition["params"]["schedule_type"] for partition in result["partitions"]
    ]
    assert "MONTHLY" in partition_types
    assert "DAILY" in partition_types


def test_update_consolidated_data(workflow_builder):
    # Start with empty consolidated data
    consolidated_data = {"jobs": [], "assets": [], "partitions": []}

    # Call _update_consolidated_data with the existing config
    workflow_builder._update_consolidated_data(
        consolidated_data, workflow_builder._config.model_dump()
    )

    # Assert the initial state after update
    assert len(consolidated_data["jobs"]) == 1
    assert len(consolidated_data["assets"]) == 3
    assert len(consolidated_data["partitions"]) == 1

    # Create a simple new config
    new_config = WorkflowConfig(
        jobs=[WorkflowJob(job_id="new_job", triggers=[], asset_selection=set())],
        assets=[],
        partitions=[],
    )

    # Update consolidated data with the new config
    workflow_builder._update_consolidated_data(
        consolidated_data, new_config.model_dump()
    )

    # Assert the final state after second update
    assert len(consolidated_data["jobs"]) == 2
    assert len(consolidated_data["assets"]) == 3
    assert len(consolidated_data["partitions"]) == 1
    assert any(job["job_id"] == "new_job" for job in consolidated_data["jobs"])
