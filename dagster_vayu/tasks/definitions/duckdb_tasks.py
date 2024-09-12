from pathlib import Path
from typing import Any, Dict

from fsspec import filesystem

from dagster_vayu.tasks.manager import BaseTask, vayu_task


@vayu_task(
    "file_to_duckdb",
    required_resources=["duckdb"],
    compute_kind="duckdb",
    storage_kind="duckdb",
)
class FileToDuckDb(BaseTask):
    """
    A task that loads data from a file (e.g., Parquet) into a DuckDB table.

    This task supports loading data from local files or Google Cloud Storage (GCS).

    Attributes:
        destination_table_id (str): The name of the destination table in DuckDB.
        source_file_uri (str): The URI of the source file to be loaded.
    """

    destination_table_id: str
    source_file_uri: str

    def run(self) -> Dict[str, Any]:
        duckdb_connection = self._resources["duckdb"]

        # Connect to the DuckDB database
        with duckdb_connection as con:
            if self.source_file_uri.startswith("gs://"):
                self.source_file_uri = "gcs:///" + self.source_file_uri[5:]
                con.register_filesystem(filesystem("gcs"))

            data = con.sql(f"SELECT * FROM '{self.source_file_uri}'")
            con.execute(
                f"CREATE OR REPLACE TABLE {self.destination_table_id} "
                "AS SELECT * FROM data"
            )

            return {
                "row_count": data.shape[0],
                "destination_table_id": self.destination_table_id,
                "source_file_uri": self.source_file_uri,
            }


@vayu_task("duckdb_query", required_resources=["duckdb"], compute_kind="duckdb")
class DuckDbQuery(BaseTask):
    """
    A task that executes a SQL query on a DuckDB database.

    This task supports executing a query from either a file or a provided query string.

    Attributes:
        query (str): The query to execute or the path to a file containing the query.
        is_file (bool): If True, 'query' is treated as a file path.
                        Otherwise, it's treated as a SQL string.
    """

    query: str
    is_file: bool = False

    def run(self) -> Dict[str, Any]:
        duckdb_connection = self._resources["duckdb"]

        # Connect to the DuckDB database
        with duckdb_connection as con:
            # Load query from file if is_file is True
            if self.is_file:
                query_path = Path(self.query)
                if not query_path.exists():
                    raise FileNotFoundError(f"Query file not found: {self.query}")
                with open(query_path, "r", encoding="utf-8") as file:
                    query = file.read()
            else:
                query = self.query

            # Execute the query
            result = con.sql(query)

            metadata = (
                {"row_count": result.shape[0], "column_names": result.columns}
                if result
                else {}
            )

            return metadata


@vayu_task("duckdb_table_to_file", required_resources=["duckdb"], compute_kind="duckdb")
class DuckDbTableToFile(BaseTask):
    """
    A task that writes a DuckDB table to a file.
    This task supports writing to local files or Google Cloud Storage (GCS).

    Attributes:
        source_table_id (str): The name of the source table in DuckDB.
        destination_file_uri (str): The URI of the destination file to be written.
    """

    source_table_id: str
    destination_file_uri: str

    def run(self) -> Dict[str, Any]:
        duckdb_connection = self._resources["duckdb"]

        # Connect to the DuckDB database
        with duckdb_connection as con:

            # Prepare the destination URI
            if self.destination_file_uri.startswith("gs://"):
                destination_uri = "gcs:///" + self.destination_file_uri[5:]
                con.register_filesystem(filesystem("gcs"))
            else:
                destination_uri = self.destination_file_uri

            # Write the table to the file
            con.sql(f"COPY {self.source_table_id} TO '{destination_uri}'")

            row_count = con.sql(
                f"SELECT COUNT(*) FROM {self.source_table_id}"
            ).fetchone()[0]

            return {
                "row_count": row_count,
                "source_table_id": self.source_table_id,
                "destination_file_uri": self.destination_file_uri,
            }
