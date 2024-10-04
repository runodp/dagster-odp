from unittest.mock import Mock, patch

import pytest
from dagster import RunRequest, SensorEvaluationContext, SkipReason
from dagster_gcp import GCSResource

from dagster_odp.sensors.definitions.gcs_sensor import GCSSensor


@pytest.fixture
def mock_gcs_resource():
    mock_gcs = Mock(spec=GCSResource)
    mock_gcs.get_client.return_value = Mock()
    return mock_gcs


@pytest.fixture
def mock_context(mock_gcs_resource):
    context = Mock(spec=SensorEvaluationContext)
    context.resources.gcs = mock_gcs_resource
    return context


@patch("dagster_odp.sensors.definitions.gcs_sensor.get_gcs_keys")
def test_gcs_sensor_no_new_objects(mock_get_gcs_keys, mock_context):
    mock_get_gcs_keys.return_value = []

    sensor = GCSSensor(bucket_name="test-bucket")
    result = sensor.run(mock_context)

    with pytest.raises(StopIteration) as exc_info:
        next(result)

    # Check that the StopIteration exception contains a SkipReason
    skip_reason = exc_info.value.value
    assert isinstance(skip_reason, SkipReason)
    assert skip_reason.skip_message == "No new objects in bucket 'test-bucket'"

    # Verify that update_cursor was not called
    mock_context.update_cursor.assert_not_called()


@patch("dagster_odp.sensors.definitions.gcs_sensor.get_gcs_keys")
def test_gcs_sensor_new_objects_no_filter(mock_get_gcs_keys, mock_context):
    mock_get_gcs_keys.return_value = ["object1", "object2"]
    sensor = GCSSensor(bucket_name="test-bucket")
    result = list(sensor.run(mock_context))

    assert len(result) == 2
    assert all(isinstance(r, RunRequest) for r in result)
    assert [r.run_key for r in result] == ["object1", "object2"]
    assert all(
        "file_uri"
        in r.run_config["resources"]["sensor_context"]["config"][
            "sensor_context_config"
        ]
        for r in result
    )
    mock_context.update_cursor.assert_called_with("object2")


@patch("dagster_odp.sensors.definitions.gcs_sensor.get_gcs_keys")
def test_gcs_sensor_new_objects_with_filter(mock_get_gcs_keys, mock_context):
    mock_get_gcs_keys.return_value = [
        "prefix/object1",
        "prefix/object2",
        "other/object3",
    ]
    sensor = GCSSensor(bucket_name="test-bucket", path_prefix_filter="prefix/")
    result = list(sensor.run(mock_context))

    assert len(result) == 2
    assert all(isinstance(r, RunRequest) for r in result)
    assert [r.run_key for r in result] == ["prefix/object1", "prefix/object2"]


@patch("dagster_odp.sensors.definitions.gcs_sensor.get_gcs_keys")
def test_gcs_sensor_cursor_usage(mock_get_gcs_keys, mock_context):
    mock_context.cursor = "last_object"
    mock_get_gcs_keys.return_value = []  # Simulating no new objects
    sensor = GCSSensor(bucket_name="test-bucket")
    result = list(sensor.run(mock_context))

    assert len(result) == 0
    mock_get_gcs_keys.assert_called_once_with(
        "test-bucket", since_key="last_object", gcs_session=mock_context.resources.gcs
    )
    mock_context.update_cursor.assert_not_called()


def test_gcs_sensor_creation():
    sensor = GCSSensor(bucket_name="test-bucket", path_prefix_filter="prefix/")
    assert sensor.bucket_name == "test-bucket"
    assert sensor.path_prefix_filter == "prefix/"
