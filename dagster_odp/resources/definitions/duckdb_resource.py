from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
from dagster import ConfigurableResource
from fsspec import filesystem

from dagster_odp.resources import odp_resource


@odp_resource("duckdb")
class DuckDbResource(ConfigurableResource):
    """
    A configurable resource for managing DuckDB connections and related operations.

    This resource provides methods for creating and managing DuckDB connections,
    as well as utility functions for working with Google Cloud Storage (GCS) URIs.

    Attributes:
        database_path (str): The path to the DuckDB database file.

    Methods:
        get_connection: A context manager that yields a DuckDB connection.
        prepare_gcs_uri: Prepares a GCS URI for use with DuckDB.
        register_gcs_if_needed: Registers the GCS filesystem if needed.
    """

    database_path: str

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """
        A context manager that yields a DuckDB connection.

        Raises:
            ValueError: If the provided path is not a valid file path.

        Yields:
            duckdb.DuckDBPyConnection: A connection to the DuckDB database.
        """
        path = Path(self.database_path)
        if not path.parent.exists():
            raise ValueError(
                f"Invalid path: {self.database_path}. Directory does not exist."
            )

        yield duckdb.connect(str(path))

    def prepare_gcs_uri(self, uri: str) -> str:
        """
        Prepares a GCS URI for use with DuckDB.
        """
        if uri.startswith("gs://"):
            return "gcs:///" + uri[5:]
        return uri

    def register_gcs_if_needed(self, con: duckdb.DuckDBPyConnection, uri: str) -> None:
        """
        Registers the GCS filesystem if the URI is a GCS URI.
        """
        if uri.startswith("gs://"):
            con.register_filesystem(filesystem("gcs"))
