from unittest.mock import Mock, patch

import pytest
from dagster import SensorDefinition
from dagster._core.definitions.sensor_definition import DefaultSensorStatus

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.workflow_model import (
    GenericSensor,
    SensorTrigger,
)
from dagster_vayu.creators.sensor_creator import _get_sensor_def, get_sensors
from dagster_vayu.sensors.definitions.gcs_sensor import GCSSensor


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock(spec=WorkflowBuilder)
    mock_wb.job_id_trigger_map.return_value = {
        "job1": [
            SensorTrigger(
                trigger_id="sensor1",
                description="Test sensor 1",
                trigger_type="sensor",
                params=GenericSensor(
                    sensor_kind="gcs_sensor",
                    sensor_params=GCSSensor(bucket_name="test-bucket-1"),
                ),
            ),
        ],
    }
    return mock_wb


def test_get_sensors(mock_workflow_builder):
    sensors = get_sensors(mock_workflow_builder)

    assert len(sensors) == 1
    assert isinstance(sensors[0], SensorDefinition)
    assert sensors[0].name == "sensor1"
    assert sensors[0].job_name == "job1"


def test_get_sensors_empty(mock_workflow_builder):
    mock_workflow_builder.job_id_trigger_map.return_value = {}
    sensors = get_sensors(mock_workflow_builder)
    assert len(sensors) == 0


def test_get_sensor_def():
    spec = Mock(
        trigger_id="test_sensor",
        description="Test sensor description",
        params=Mock(sensor_kind="gcs_sensor", sensor_params=Mock()),
    )

    with patch(
        "dagster_vayu.creators.sensor_creator.sensor_registry",
        {"gcs_sensor": {"required_resources": {"gcs"}}},
    ):
        sensor_def = _get_sensor_def("test_job", spec)

    assert isinstance(sensor_def, SensorDefinition)
    assert sensor_def.name == "test_sensor"
    assert sensor_def.job_name == "test_job"
    assert sensor_def.description == "Test sensor description"
    assert sensor_def.default_status == DefaultSensorStatus.RUNNING
    assert sensor_def.required_resource_keys == {"gcs"}
