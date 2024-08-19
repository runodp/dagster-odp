from unittest.mock import Mock, call, patch

import pytest
from dagster import SensorDefinition
from dagster._core.definitions.sensor_definition import DefaultSensorStatus

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder
from dagster_vayu.config_manager.models.config_model import SensorConfig
from dagster_vayu.config_manager.models.workflow_model import (
    GenericSensor,
    SensorTrigger,
)
from dagster_vayu.creators.sensor_creator import _get_sensor_def, get_sensors
from dagster_vayu.sensors.definitions.gcs_sensor import GCSSensor
from dagster_vayu.sensors.manager.sensor_registry import sensor_registry


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
                    sensor_params=GCSSensor(
                        bucket_name="test-bucket-1", path_prefix_filter=None
                    ),
                ),
            ),
            SensorTrigger(
                trigger_id="sensor2",
                description="Test sensor 2",
                trigger_type="sensor",
                params=GenericSensor(
                    sensor_kind="gcs_sensor",
                    sensor_params=GCSSensor(
                        bucket_name="test-bucket-2", path_prefix_filter="prefix/"
                    ),
                ),
            ),
        ],
        "job2": [
            SensorTrigger(
                trigger_id="sensor3",
                description="Test sensor 3",
                trigger_type="sensor",
                params=GenericSensor(
                    sensor_kind="gcs_sensor",
                    sensor_params=GCSSensor(
                        bucket_name="test-bucket-3",
                        path_prefix_filter="another/prefix/",
                    ),
                ),
            )
        ],
    }
    return mock_wb


@pytest.fixture
def mock_sensor_config():
    return [SensorConfig(name="gcs_sensor", required_resources=["gcs"])]


def test_get_sensors(mock_workflow_builder, mock_sensor_config):
    mock_sensor_instance = Mock()
    mock_sensor_instance.run = Mock()
    mock_sensor_builder = Mock(return_value=mock_sensor_instance)

    with patch.dict(sensor_registry, {"gcs_sensor": mock_sensor_builder}):
        sensors = get_sensors(mock_workflow_builder, mock_sensor_config)

    assert len(sensors) == 3
    assert all(isinstance(sensor, SensorDefinition) for sensor in sensors)

    expected_calls = [
        call(bucket_name="test-bucket-1", path_prefix_filter=None),
        call(bucket_name="test-bucket-2", path_prefix_filter="prefix/"),
        call(bucket_name="test-bucket-3", path_prefix_filter="another/prefix/"),
    ]
    mock_sensor_builder.assert_has_calls(expected_calls, any_order=True)
    assert mock_sensor_builder.call_count == 3


def test_get_sensors_undefined_sensor(mock_workflow_builder, mock_sensor_config):
    with patch.dict(sensor_registry, {}, clear=True):
        with pytest.raises(
            ValueError, match="Sensor 'gcs_sensor' is not defined with the decorator."
        ):
            get_sensors(mock_workflow_builder, mock_sensor_config)


def test_get_sensors_empty(mock_workflow_builder, mock_sensor_config):
    mock_workflow_builder.job_id_trigger_map.return_value = {}
    sensors = get_sensors(mock_workflow_builder, mock_sensor_config)
    assert len(sensors) == 0


def test_get_sensor_def():
    mock_sensor_instance = Mock()
    mock_sensor_instance.run = Mock()
    mock_sensor_cls = Mock(return_value=mock_sensor_instance)

    sensor_resource_map = {"test_sensor": ["resource1", "resource2"]}
    spec = Mock(
        trigger_id="test_sensor",
        description="Test sensor description",
        params=Mock(
            sensor_kind="test_sensor",
            sensor_params=Mock(model_dump=Mock(return_value={"param1": "value1"})),
        ),
    )

    with patch.dict(sensor_registry, {"test_sensor": mock_sensor_cls}):
        sensor_def = _get_sensor_def("test_job", sensor_resource_map, spec)

    assert isinstance(sensor_def, SensorDefinition)
    assert sensor_def.name == "test_sensor"
    assert sensor_def.job_name == "test_job"
    assert sensor_def.description == "Test sensor description"
    assert sensor_def.default_status == DefaultSensorStatus.RUNNING
    assert sensor_def.required_resource_keys == {"resource1", "resource2"}

    mock_sensor_cls.assert_called_once_with(param1="value1")
