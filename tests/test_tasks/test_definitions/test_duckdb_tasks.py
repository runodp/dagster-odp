import tempfile
from pathlib import Path

import duckdb
import pytest

from dagster_vayu.tasks.definitions.duckdb_tasks import (
    DuckDbQuery,
    DuckDbTableToFile,
    FileToDuckDb,
)


class DuckDBResource:
    def __init__(self):
        self.db = duckdb.connect(":memory:")

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # We don't close the connection here anymore

    def close(self):
        self.db.close()


@pytest.fixture(scope="function")
def duckdb_resource():
    resource = DuckDBResource()
    yield resource
    resource.close()  # Close the connection after the test


@pytest.fixture(scope="function")
def temp_parquet_file():
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        temp_file_path = temp_file.name

    # Create a sample Parquet file
    conn = duckdb.connect(":memory:")
    conn.sql("CREATE TABLE test_data AS SELECT 1 AS id, 'test' AS name")
    conn.sql(f"COPY test_data TO '{temp_file_path}' (FORMAT PARQUET)")
    conn.close()

    yield temp_file_path

    Path(temp_file_path).unlink()


def test_file_to_duckdb(duckdb_resource, temp_parquet_file):
    task = FileToDuckDb(
        destination_table_id="test_table", source_file_uri=temp_parquet_file
    )
    task._resources = {"duckdb": duckdb_resource}

    result = task.run()

    assert result["row_count"] == 1
    assert result["destination_table_id"] == "test_table"
    assert result["source_file_uri"] == temp_parquet_file

    # Verify the data was actually loaded
    with duckdb_resource as con:
        data = con.sql("SELECT * FROM test_table").fetchall()
        assert len(data) == 1
        assert data[0] == (1, "test")


def test_duckdb_query(duckdb_resource):
    # First, create a table to query
    with duckdb_resource as con:
        con.sql("CREATE TABLE test_table AS SELECT 1 AS id, 'test' AS name")

    task = DuckDbQuery(query="SELECT * FROM test_table")
    task._resources = {"duckdb": duckdb_resource}

    result = task.run()

    assert result["row_count"] == 1
    assert result["column_names"] == ["id", "name"]


def test_duckdb_query_from_file(duckdb_resource, tmp_path):
    # Create a query file
    query_file = tmp_path / "test_query.sql"
    query_file.write_text("SELECT 1 AS id, 'test' AS name")

    task = DuckDbQuery(query=str(query_file), is_file=True)
    task._resources = {"duckdb": duckdb_resource}

    result = task.run()

    assert result["row_count"] == 1
    assert result["column_names"] == ["id", "name"]


def test_duckdb_table_to_file(duckdb_resource, tmp_path):
    # First, create a table to export
    with duckdb_resource as con:
        con.sql("CREATE TABLE test_table AS SELECT 1 AS id, 'test' AS name")

    output_file = tmp_path / "output.parquet"
    task = DuckDbTableToFile(
        source_table_id="test_table", destination_file_uri=str(output_file)
    )
    task._resources = {"duckdb": duckdb_resource}

    result = task.run()

    assert result["row_count"] == 1
    assert result["source_table_id"] == "test_table"
    assert result["destination_file_uri"] == str(output_file)

    # Verify the file was actually created and contains the correct data
    assert output_file.exists()
    verify_con = duckdb.connect(":memory:")
    data = verify_con.sql(f"SELECT * FROM '{output_file}'").fetchall()
    verify_con.close()
    assert len(data) == 1
    assert data[0] == (1, "test")
