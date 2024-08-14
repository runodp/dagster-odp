from unittest.mock import Mock, call, patch

import pytest
from task_nicely_core.configs.builders.workflow_builder import WorkflowBuilder
from task_nicely_core.configs.models.workflow_model import (
    GCSSensorParams,
    GCSSensorTrigger,
    SensorTrigger,
)
from task_nicely_core.sensors.sensor_creator import SensorCreator
from task_nicely_core.sensors.sensor_registry import sensor_registry

from dagster import SensorDefinition


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock(spec=WorkflowBuilder)
    mock_wb.job_id_trigger_map.return_value = {
        "job1": [
            SensorTrigger(
                trigger_id="sensor1",
                description="Test sensor 1",
                trigger_type="sensor",
                params=GCSSensorTrigger(
                    sensor_kind="gcs_sensor",
                    sensor_params=GCSSensorParams(bucket_name="test-bucket-1"),
                ),
            ),
            SensorTrigger(
                trigger_id="sensor2",
                description="Test sensor 2",
                trigger_type="sensor",
                params=GCSSensorTrigger(
                    sensor_kind="gcs_sensor",
                    sensor_params=GCSSensorParams(bucket_name="test-bucket-2"),
                ),
            ),
        ],
        "job2": [
            SensorTrigger(
                trigger_id="sensor3",
                description="Test sensor 3",
                trigger_type="sensor",
                params=GCSSensorTrigger(
                    sensor_kind="gcs_sensor",
                    sensor_params=GCSSensorParams(bucket_name="test-bucket-3"),
                ),
            )
        ],
    }
    return mock_wb


@pytest.fixture
def sensor_creator(mock_workflow_builder):
    with patch(
        "task_nicely_core.sensors.sensor_creator.WorkflowBuilder",
        return_value=mock_workflow_builder,
    ):
        return SensorCreator()


def test_get_sensors(sensor_creator):
    mock_sensor_builder = Mock(return_value=Mock(spec=SensorDefinition))

    with patch.dict(sensor_registry, {"gcs_sensor": mock_sensor_builder}):
        sensors = sensor_creator.get_sensors()

    assert len(sensors) == 3
    assert all(isinstance(sensor, Mock) for sensor in sensors)

    expected_calls = [
        call(
            job_id="job1",
            trigger_id="sensor1",
            description="Test sensor 1",
            params={"bucket_name": "test-bucket-1", "path_prefix_filter": None},
        ),
        call(
            job_id="job1",
            trigger_id="sensor2",
            description="Test sensor 2",
            params={"bucket_name": "test-bucket-2", "path_prefix_filter": None},
        ),
        call(
            job_id="job2",
            trigger_id="sensor3",
            description="Test sensor 3",
            params={"bucket_name": "test-bucket-3", "path_prefix_filter": None},
        ),
    ]
    mock_sensor_builder.assert_has_calls(expected_calls, any_order=True)
    assert mock_sensor_builder.call_count == 3


def test_get_sensors_undefined_sensor(sensor_creator):
    # Use an empty sensor_registry to simulate the undefined sensor scenario
    with patch.dict(sensor_registry, {}, clear=True):
        with pytest.raises(
            ValueError, match="Sensor 'gcs_sensor' is not defined with the decorator."
        ):
            sensor_creator.get_sensors()


def test_get_sensors_empty(sensor_creator):
    sensor_creator._wb.job_id_trigger_map.return_value = {}

    sensors = sensor_creator.get_sensors()

    assert len(sensors) == 0
