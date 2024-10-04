from pathlib import Path
from typing import Any, Dict

from dagster_odp.tasks.manager import BaseTask, odp_task

from ...resources.definitions.duckdb_resource import DuckDbResource


@odp_task(
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
        duckdb_client: DuckDbResource = self._resources["duckdb"]

        duckdb_connection = duckdb_client.get_connection()
        # Connect to the DuckDB database
        with duckdb_connection as con:
            source_uri = duckdb_client.prepare_gcs_uri(self.source_file_uri)
            duckdb_client.register_gcs_if_needed(con, self.source_file_uri)

            data = con.sql(f"SELECT * FROM '{source_uri}'")
            con.execute(
                f"CREATE OR REPLACE TABLE {self.destination_table_id} "
                "AS SELECT * FROM data"
            )

            return {
                "row_count": data.shape[0],
                "destination_table_id": self.destination_table_id,
                "source_file_uri": self.source_file_uri,
            }


@odp_task("duckdb_query", required_resources=["duckdb"], compute_kind="duckdb")
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

    def _get_query(self) -> str:
        if self.is_file:
            query_path = Path(self.query)
            if not query_path.exists():
                raise FileNotFoundError(f"Query file not found: {self.query}")
            return query_path.read_text(encoding="utf-8")
        return self.query

    def run(self) -> Dict[str, Any]:
        duckdb_client = self._resources["duckdb"]

        duckdb_connection = duckdb_client.get_connection()

        # Connect to the DuckDB database
        with duckdb_connection as con:
            query = self._get_query()
            result = con.sql(query)

            return (
                {"row_count": result.shape[0], "column_names": result.columns}
                if result
                else {}
            )


@odp_task("duckdb_table_to_file", required_resources=["duckdb"], compute_kind="duckdb")
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
        duckdb_client: DuckDbResource = self._resources["duckdb"]

        duckdb_connection = duckdb_client.get_connection()

        with duckdb_connection as con:

            destination_uri = duckdb_client.prepare_gcs_uri(self.destination_file_uri)
            duckdb_client.register_gcs_if_needed(con, destination_uri)

            con.sql(f"COPY {self.source_table_id} TO '{destination_uri}'")
            row_count = con.sql(
                f"SELECT COUNT(*) FROM {self.source_table_id}"
            ).fetchone()

            return {
                "row_count": row_count[0] if row_count else None,
                "source_table_id": self.source_table_id,
                "destination_file_uri": self.destination_file_uri,
            }
