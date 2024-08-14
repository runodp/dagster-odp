from unittest.mock import Mock, patch

import pytest
from dagster_gcp import GCSResource
from task_nicely_core.sensors.sensor_definitions.gcs_sensor import _get_run_requests

from dagster import RunRequest, SensorEvaluationContext, SkipReason


@pytest.fixture
def mock_gcs_resource():
    mock_gcs = Mock(spec=GCSResource)
    mock_gcs.get_client.return_value = Mock()
    return mock_gcs


@pytest.fixture
def mock_context():
    return Mock(spec=SensorEvaluationContext)


@patch("task_nicely_core.sensors.sensor_definitions.gcs_sensor.get_gcs_keys")
def test_getrun_requests_no_new_objects(
    mock_get_gcs_keys, mock_context, mock_gcs_resource
):
    mock_get_gcs_keys.return_value = []

    result = _get_run_requests(mock_context, mock_gcs_resource, "test-bucket", None)

    with pytest.raises(StopIteration) as exc_info:
        next(result)

    # Check that the StopIteration exception contains a SkipReason
    skip_reason = exc_info.value.value
    assert isinstance(skip_reason, SkipReason)
    assert skip_reason.skip_message == "No new objects in bucket 'test-bucket'"

    # Verify that update_cursor was not called
    mock_context.update_cursor.assert_not_called()


@patch("task_nicely_core.sensors.sensor_definitions.gcs_sensor.get_gcs_keys")
def test_get_run_requests_new_objects_no_filter(
    mock_get_gcs_keys, mock_context, mock_gcs_resource
):
    mock_get_gcs_keys.return_value = ["object1", "object2"]

    result = list(
        _get_run_requests(mock_context, mock_gcs_resource, "test-bucket", None)
    )

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


@patch("task_nicely_core.sensors.sensor_definitions.gcs_sensor.get_gcs_keys")
def test_get_run_requests_new_objects_with_filter(
    mock_get_gcs_keys, mock_context, mock_gcs_resource
):
    mock_get_gcs_keys.return_value = [
        "prefix/object1",
        "prefix/object2",
        "other/object3",
    ]

    result = list(
        _get_run_requests(mock_context, mock_gcs_resource, "test-bucket", "prefix/")
    )

    assert len(result) == 2
    assert all(isinstance(r, RunRequest) for r in result)
    assert [r.run_key for r in result] == ["prefix/object1", "prefix/object2"]


@patch("task_nicely_core.sensors.sensor_definitions.gcs_sensor.get_gcs_keys")
def test_get_run_requests_cursor_usage(
    mock_get_gcs_keys, mock_context, mock_gcs_resource
):
    mock_context.cursor = "last_object"
    mock_get_gcs_keys.return_value = []  # Simulating no new objects

    # Consume the generator
    result = list(
        _get_run_requests(mock_context, mock_gcs_resource, "test-bucket", None)
    )

    # Assert that the result is empty (as we're simulating no new objects)
    assert len(result) == 0

    # Now we can assert that get_gcs_keys was called with the correct arguments
    mock_get_gcs_keys.assert_called_once_with(
        "test-bucket",
        since_key="last_object",
        gcs_session=mock_gcs_resource.get_client.return_value,
    )

    # Verify that update_cursor was not called (as there were no new objects)
    mock_context.update_cursor.assert_not_called()


def test_gcs_sensor_function_creation():
    from task_nicely_core.sensors.sensor_definitions.gcs_sensor import gcs_sensor

    sensor_fn = gcs_sensor(
        job_id="test_job",
        trigger_id="test_trigger",
        params={"bucket_name": "test-bucket", "path_prefix_filter": None},
        description="Test GCS sensor",
    )

    assert callable(sensor_fn)
    # We can't easily test the decorated function directly,
    # but we can check that it's a callable
