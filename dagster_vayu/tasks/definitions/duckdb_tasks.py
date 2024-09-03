from typing import Dict

from fsspec import filesystem

from dagster_vayu.tasks.manager import BaseTask, vayu_task


@vayu_task("file_to_duckdb")
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

    def run(self) -> Dict:
        if not self._resources or "duckdb" not in self._resources:
            raise ValueError("required 'duckdb' resource not passed to the asset")

        duckdb_connection = self._resources["duckdb"]

        # Connect to the DuckDB database
        with duckdb_connection as con:
            if self.source_file_uri.startswith("gs://"):
                self.source_file_uri = "gcs:///" + self.source_file_uri[5:]
                con.register_filesystem(filesystem("gcs"))

            # Read the Parquet file and create a table
            con.sql(
                f"CREATE OR REPLACE TABLE {self.destination_table_id} AS "
                f"SELECT * FROM '{self.source_file_uri}'"
            )

            # Verify that the data was loaded
            row_count = con.sql(
                f"SELECT COUNT(*) FROM {self.destination_table_id}"
            ).fetchall()
            return {
                "rows_loaded": row_count[0][0],
                "destination_table_id": self.destination_table_id,
                "source_file_uri": self.source_file_uri,
            }
