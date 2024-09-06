import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from dagster_vayu.tasks.definitions import DuckDbQuery, DuckDbTableToFile, FileToDuckDb


@pytest.fixture(scope="module")
def sample_data():
    return pd.DataFrame(
        {
            "id": range(1, 11),
            "name": [f"Name_{i}" for i in range(1, 11)],
            "value": [i * 1.5 for i in range(1, 11)],
        }
    )


@pytest.fixture(scope="module")
def duckdb_database(sample_data):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_db.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE sample_table AS SELECT * FROM sample_data")
        yield conn
        conn.close()


@pytest.fixture(scope="module")
def duckdb_connection(duckdb_database):
    class ConnectionWrapper:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass  # Don't close the connection

    return ConnectionWrapper(duckdb_database)


def test_file_to_duckdb(sample_data, duckdb_connection):
    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        sample_data.to_parquet(temp_file.name)

        task = FileToDuckDb(
            destination_table_id="test_table", source_file_uri=temp_file.name
        )
        task.set_resources({"duckdb": duckdb_connection})

        result = task.run()

        assert result["row_count"] == 10
        assert result["destination_table_id"] == "test_table"
        assert result["source_file_uri"] == temp_file.name

        query_result = duckdb_connection.conn.execute(
            "SELECT COUNT(*) FROM test_table"
        ).fetchone()
        assert query_result[0] == 10


def test_duckdb_query(duckdb_connection):
    task = DuckDbQuery(query="SELECT * FROM sample_table WHERE id > 5")
    task.set_resources({"duckdb": duckdb_connection})

    result = task.run()

    assert result["row_count"] == 5
    assert "id" in result["column_names"]
    assert "name" in result["column_names"]
    assert "value" in result["column_names"]


def test_duckdb_query_from_file(duckdb_connection):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql") as temp_file:
        temp_file.write("SELECT * FROM sample_table WHERE id <= 5")
        temp_file.flush()

        task = DuckDbQuery(query=temp_file.name, is_file=True)
        task.set_resources({"duckdb": duckdb_connection})

        result = task.run()

        assert result["row_count"] == 5
        assert "id" in result["column_names"]
        assert "name" in result["column_names"]
        assert "value" in result["column_names"]


def test_duckdb_to_file(duckdb_connection):
    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        task = DuckDbTableToFile(
            source_table_id="sample_table", destination_file_uri=temp_file.name
        )
        task.set_resources({"duckdb": duckdb_connection})

        result = task.run()

        assert result["row_count"] == 10
        assert result["source_table_id"] == "sample_table"
        assert result["destination_file_uri"] == temp_file.name

        # Verify the file was created and contains data
        df = pd.read_parquet(temp_file.name)
        assert len(df) == 10
        assert list(df.columns) == ["id", "name", "value"]


def test_tasks_missing_resource():
    tasks = [
        FileToDuckDb(destination_table_id="test_table", source_file_uri="test.parquet"),
        DuckDbQuery(query="SELECT * FROM test_table"),
        DuckDbTableToFile(
            source_table_id="test_table", destination_file_uri="test.parquet"
        ),
    ]

    for task in tasks:
        with pytest.raises(
            ValueError, match="[Rr]equired 'duckdb' resource not passed to the asset"
        ):
            task.run()
