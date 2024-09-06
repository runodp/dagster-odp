from unittest.mock import Mock, patch

import pytest
from dagster_vayu.tasks.definitions.gcp_tasks import BQTableToGCS, GCSFileToBQ
from google.cloud import bigquery as bq


@pytest.fixture
def mock_bigquery():
    with patch("google.cloud.bigquery.Client") as mock_client:
        mock_instance = mock_client.return_value
        mock_instance.__enter__.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_job():
    job = Mock()
    job.result.return_value = None
    return job


@pytest.fixture
def mock_table():
    table = Mock()
    table.num_rows = 100
    return table


def assert_job_config(called_job_config, expected_config_type, job_config_params):
    assert isinstance(called_job_config, expected_config_type)
    for key, value in job_config_params.items():
        assert getattr(called_job_config, key) == value


def test_gcs_file_to_bq(mock_bigquery, mock_job, mock_table):
    # Arrange
    source_file_uri = "gs://test-bucket/test-file.parquet"
    destination_table_id = "test_dataset.destination_table"
    job_config_params = {
        "autodetect": True,
        "source_format": "PARQUET",
        "write_disposition": "WRITE_APPEND",
    }
    mock_bigquery.load_table_from_uri.return_value = mock_job
    mock_bigquery.get_table.return_value = mock_table

    # Create an instance of GCSFileToBQ
    task = GCSFileToBQ(
        source_file_uri=source_file_uri,
        destination_table_id=destination_table_id,
        job_config_params=job_config_params,
    )
    task._resources = {"bigquery": mock_bigquery}

    # Act
    result = task.run()

    # Assert
    mock_bigquery.load_table_from_uri.assert_called_once()
    call_args = mock_bigquery.load_table_from_uri.call_args
    assert call_args[0][0] == source_file_uri
    assert call_args[0][1] == destination_table_id
    assert_job_config(call_args[1]["job_config"], bq.LoadJobConfig, job_config_params)
    mock_job.result.assert_called_once()
    mock_bigquery.get_table.assert_called_once_with(destination_table_id)
    assert result == {
        "source_file_uri": source_file_uri,
        "destination_table_id": destination_table_id,
        "row_count": mock_table.num_rows,
    }


def test_bq_table_to_gcs(mock_bigquery, mock_job, mock_table):
    # Arrange
    source_table_id = "test_dataset.source_table"
    destination_file_uri = "gs://test-bucket/output/test-file.parquet"
    job_config_params = {"destination_format": "PARQUET"}
    mock_bigquery.extract_table.return_value = mock_job
    mock_bigquery.get_table.return_value = mock_table
    mock_job.destination_uris = [destination_file_uri]

    # Create an instance of BQTableToGCS
    task = BQTableToGCS(
        source_table_id=source_table_id,
        destination_file_uri=destination_file_uri,
        job_config_params=job_config_params,
    )
    task._resources = {"bigquery": mock_bigquery}

    # Act
    result = task.run()

    # Assert
    mock_bigquery.extract_table.assert_called_once()
    call_args = mock_bigquery.extract_table.call_args
    assert call_args[0][0] == source_table_id
    assert call_args[0][1] == destination_file_uri
    assert_job_config(
        call_args[1]["job_config"], bq.ExtractJobConfig, job_config_params
    )
    mock_job.result.assert_called_once()
    mock_bigquery.get_table.assert_called_once_with(source_table_id)
    assert result == {
        "source_table_id": source_table_id,
        "destination_file_uri": "gs://test-bucket/output",
        "row_count": mock_table.num_rows,
    }
