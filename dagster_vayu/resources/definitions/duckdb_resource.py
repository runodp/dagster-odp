from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import duckdb
from dagster import ConfigurableResource, IAttachDifferentObjectToOpContext

from dagster_vayu.resources import vayu_resource


@vayu_resource("duckdb")
class DuckDbResource(ConfigurableResource, IAttachDifferentObjectToOpContext):
    """
    A configurable resource for managing DuckDB connections.

    This resource provides methods for creating and managing DuckDB connections,
    as well as attaching the connection to the execution context.

    Attributes:
        database_path (str): The path to the DuckDB database file.

    Methods:
        get_connection: A context manager that yields a DuckDB connection.
        get_object_to_set_on_execution_context: Overrides the method from
            IAttachDifferentObjectToOpContext to return the connection object.
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
        if not Path(self.database_path).is_file():
            raise ValueError(f"Invalid path: {self.database_path}")

        yield duckdb.connect(self.database_path)

    def get_object_to_set_on_execution_context(self) -> Any:
        return self.get_connection()
