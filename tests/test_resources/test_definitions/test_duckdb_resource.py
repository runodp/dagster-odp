from unittest.mock import Mock

import duckdb
import pytest

from dagster_odp.resources.definitions import DuckDbResource


def test_duckdb_resource_get_connection_valid_path(tmp_path):
    # Create a temporary database file path
    db_path = tmp_path / "test.db"
    resource = DuckDbResource(database_path=str(db_path))

    # Use the get_connection method
    with resource.get_connection() as conn:
        # Check if the connection is a valid DuckDB connection
        assert isinstance(conn, duckdb.DuckDBPyConnection)

        # Optionally, perform a simple query to ensure the connection works
        result = conn.execute("SELECT 1").fetchone()
        assert result[0] == 1  # type: ignore

    # Check if the database file was created
    assert db_path.exists()


def test_duckdb_resource_get_connection_invalid_path():
    resource = DuckDbResource(database_path="/nonexistent/path/to/invalid.db")

    with pytest.raises(
        ValueError,
        match="Invalid path: /nonexistent/path/to/invalid.db. "
        "Directory does not exist.",
    ):
        with resource.get_connection():
            pass


def test_prepare_gcs_uri():
    resource = DuckDbResource(database_path="test.db")

    # Test GCS URI
    gcs_uri = "gs://bucket/file.parquet"
    assert resource.prepare_gcs_uri(gcs_uri) == "gcs:///bucket/file.parquet"

    # Test non-GCS URI
    local_uri = "/path/to/file.parquet"
    assert resource.prepare_gcs_uri(local_uri) == local_uri


def test_register_gcs_if_needed():
    resource = DuckDbResource(database_path="test.db")
    mock_con = Mock()

    # Test GCS URI
    gcs_uri = "gs://bucket/file.parquet"
    resource.register_gcs_if_needed(mock_con, gcs_uri)
    mock_con.register_filesystem.assert_called_once()

    # Test non-GCS URI
    local_uri = "/path/to/file.parquet"
    mock_con.reset_mock()
    resource.register_gcs_if_needed(mock_con, local_uri)
    mock_con.register_filesystem.assert_not_called()
