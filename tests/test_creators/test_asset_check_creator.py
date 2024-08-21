from unittest.mock import Mock

import pytest
from dagster import AssetChecksDefinition, AssetCheckSeverity, TextMetadataValue

from dagster_vayu.creators.asset_check_creator import (
    _get_asset_check_def,
    _get_check_result,
    get_asset_checks,
)


@pytest.fixture
def mock_workflow_builder():
    mock_wb = Mock()
    mock_wb.soda_checks = [
        Mock(
            model_dump=Mock(
                return_value={
                    "check_file_path": "/path/to/check1.yml",
                    "asset_key": "test_asset",
                    "blocking": True,
                    "data_source": "test_source",
                }
            )
        ),
        Mock(
            model_dump=Mock(
                return_value={
                    "check_file_path": "/path/to/check2.yml",
                    "asset_key": "test_asset2",
                    "blocking": False,
                    "data_source": "test_source2",
                }
            )
        ),
    ]
    return mock_wb


@pytest.mark.parametrize(
    "outcomes,expected_passed,expected_severity",
    [
        (["PASS", "PASS"], True, AssetCheckSeverity.WARN),
        (["PASS", "WARN"], False, AssetCheckSeverity.WARN),
        (["PASS", "FAIL"], False, AssetCheckSeverity.ERROR),
    ],
)
def test_get_check_result(outcomes, expected_passed, expected_severity):
    check_results = [
        {"outcome": outcome, "check": f"check{i}"} for i, outcome in enumerate(outcomes)
    ]

    result = _get_check_result(check_results)

    assert result.passed == expected_passed
    assert result.severity == expected_severity
    assert result.metadata == {
        f"check{i}": TextMetadataValue(text=outcome)
        for i, outcome in enumerate(outcomes)
    }


def test_get_asset_check_def():
    check_params = {
        "check_file_path": "/path/to/test_check.yml",
        "asset_key": "test_asset",
        "blocking": True,
        "data_source": "test_source",
    }

    result = _get_asset_check_def(check_params)

    assert isinstance(result, AssetChecksDefinition)
    assert "test_asset" in result.keys_by_input_name
    assert "soda" in result.required_resource_keys


def test_get_asset_checks(mock_workflow_builder):
    result = get_asset_checks(mock_workflow_builder)

    assert len(result) == 2
    assert all(isinstance(check, AssetChecksDefinition) for check in result)
    assert "test_asset" in result[0].keys_by_input_name
    assert "test_asset2" in result[1].keys_by_input_name
