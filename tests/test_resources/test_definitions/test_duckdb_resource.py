import duckdb
import pytest

from dagster_vayu.resources.definitions import DuckDbResource


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
        assert result[0] == 1

    # Check if the database file was created
    assert db_path.exists()


def test_duckdb_resource_get_connection_invalid_path():
    resource = DuckDbResource(database_path="/nonexistent/path/to/invalid.db")

    with pytest.raises(
        ValueError,
        match="Invalid path: /nonexistent/path/to/invalid.db."
        " Directory does not exist.",
    ):
        with resource.get_connection():
            pass
