from unittest.mock import MagicMock, patch

import pytest

from dagster_vayu.resources.definitions import DuckDbResource


@pytest.fixture
def mock_duckdb_connect():
    with patch(
        "dagster_vayu.resources.definitions.duckdb_resource.duckdb.connect"
    ) as mock_connect:
        yield mock_connect


@pytest.fixture
def mock_path_is_file():
    with patch("pathlib.Path.is_file") as mock_is_file:
        yield mock_is_file


def test_duckdb_resource_get_connection_valid_path(
    mock_duckdb_connect, mock_path_is_file
):
    resource = DuckDbResource(database_path="test.db")
    mock_connection = MagicMock()
    mock_duckdb_connect.return_value = mock_connection
    mock_path_is_file.return_value = True

    with resource.get_connection() as conn:
        assert conn == mock_connection

    mock_duckdb_connect.assert_called_once_with("test.db")
    mock_path_is_file.assert_called_once_with()


def test_duckdb_resource_get_connection_invalid_path(mock_path_is_file):
    resource = DuckDbResource(database_path="invalid.db")
    mock_path_is_file.return_value = False

    with pytest.raises(ValueError, match="Invalid path: invalid.db"):
        with resource.get_connection():
            pass

    mock_path_is_file.assert_called_once_with()
