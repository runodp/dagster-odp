# import os
from unittest.mock import Mock, patch

import pytest
from soda.scan import Scan

from dagster_vayu.resources.definitions import SodaResource


@pytest.fixture
def soda_resource():
    return SodaResource(project_dir="/path/to/project", checks_dir="checks")


def test_get_config_file_path(soda_resource):
    with patch("os.path.exists", return_value=True):
        assert (
            soda_resource._get_config_file_path()
            == "/path/to/project/configuration.yml"
        )


def test_get_config_file_path_not_found(soda_resource):
    with patch("os.path.exists", return_value=False):
        with pytest.raises(FileNotFoundError):
            soda_resource._get_config_file_path()


def test_get_check_file_path(soda_resource):
    with patch("os.path.exists", return_value=True):
        assert (
            soda_resource._get_check_file_path("test.yml")
            == "/path/to/project/checks/test.yml"
        )


def test_get_check_file_path_not_found(soda_resource):
    with patch("os.path.exists", return_value=False):
        with pytest.raises(FileNotFoundError):
            soda_resource._get_check_file_path("test.yml")


def test_extract_scan_results(soda_resource):
    mock_scan = Mock(spec=Scan)
    mock_outcome = Mock()
    mock_outcome.name = "PASS"
    mock_check = Mock()
    mock_check.name = "test_check"
    mock_check.outcome = mock_outcome
    mock_check.check_cfg.source_line = "test: check"
    mock_check.get_log_diagnostic_dict.return_value = {"diagnostic": "info"}
    mock_scan._checks = [mock_check]

    results = soda_resource._extract_scan_results(mock_scan)

    assert len(results) == 1
    assert results[0] == {
        "name": "test_check",
        "outcome": "PASS",
        "check": "test: check",
        "result": {"diagnostic": "info"},
    }
