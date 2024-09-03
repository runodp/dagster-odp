from unittest.mock import MagicMock, patch

import pytest

from dagster_vayu.tasks.definitions import FileToDuckDb


@pytest.fixture
def mock_duckdb_connection():
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    return mock_conn


@pytest.fixture
def file_to_duckdb_task():
    return FileToDuckDb(
        destination_table_id="test_table",
        source_file_uri="test_file.parquet",
    )


def test_file_to_duckdb_run_local_file(file_to_duckdb_task, mock_duckdb_connection):
    mock_duckdb_connection.sql.return_value.fetchall.return_value = [[100]]
    file_to_duckdb_task.set_resources({"duckdb": mock_duckdb_connection})

    result = file_to_duckdb_task.run()

    assert result == {
        "rows_loaded": 100,
        "destination_table_id": "test_table",
        "source_file_uri": "test_file.parquet",
    }
    mock_duckdb_connection.sql.assert_any_call(
        "CREATE OR REPLACE TABLE test_table AS SELECT * FROM 'test_file.parquet'"
    )
    mock_duckdb_connection.sql.assert_any_call("SELECT COUNT(*) FROM test_table")


def test_file_to_duckdb_run_gcs_file(file_to_duckdb_task, mock_duckdb_connection):
    file_to_duckdb_task.source_file_uri = "gs://bucket/test_file.parquet"
    mock_duckdb_connection.sql.return_value.fetchall.return_value = [[200]]
    file_to_duckdb_task.set_resources({"duckdb": mock_duckdb_connection})

    with patch(
        "dagster_vayu.tasks.definitions.duckdb_tasks.filesystem"
    ) as mock_filesystem:
        result = file_to_duckdb_task.run()

    assert result == {
        "rows_loaded": 200,
        "destination_table_id": "test_table",
        "source_file_uri": "gcs:///bucket/test_file.parquet",
    }
    mock_duckdb_connection.register_filesystem.assert_called_once_with(
        mock_filesystem.return_value
    )
    mock_duckdb_connection.sql.assert_any_call(
        "CREATE OR REPLACE TABLE test_table AS "
        "SELECT * FROM 'gcs:///bucket/test_file.parquet'"
    )
    mock_duckdb_connection.sql.assert_any_call("SELECT COUNT(*) FROM test_table")


def test_file_to_duckdb_run_missing_resource():
    task = FileToDuckDb(
        destination_table_id="test_table",
        source_file_uri="test_file.parquet",
    )
    with pytest.raises(
        ValueError, match="required 'duckdb' resource not passed to the asset"
    ):
        task.run()
