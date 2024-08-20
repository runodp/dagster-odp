from unittest.mock import Mock, patch

import pytest
from dagster import AssetsDefinition

from dagster_vayu.creators.asset_creators import get_assets


@pytest.fixture
def mock_asset_creators():
    with (
        patch("dagster_vayu.creators.asset_creators.DBTAssetCreator") as mock_dbt,
        patch("dagster_vayu.creators.asset_creators.DLTAssetCreator") as mock_dlt,
        patch(
            "dagster_vayu.creators.asset_creators.GenericAssetCreator"
        ) as mock_generic,
    ):

        mock_dbt.return_value.get_assets.return_value = [Mock(spec=AssetsDefinition)]
        mock_dlt.return_value.get_assets.return_value = [Mock(spec=AssetsDefinition)]
        mock_generic.return_value.get_assets.return_value = [
            Mock(spec=AssetsDefinition)
        ]

        yield mock_dbt, mock_dlt, mock_generic


def test_get_assets(mock_asset_creators):
    mock_dbt, mock_dlt, mock_generic = mock_asset_creators

    # Test with all resources
    dagster_resources = {"dbt": Mock(), "dlt": Mock()}
    result = get_assets(dagster_resources)

    assert len(result) == 3  # 1 DBT + 1 DLT + 1 Generic
    mock_dbt.assert_called_once()
    mock_dlt.assert_called_once()
    mock_generic.assert_called_once()

    # Test with no special resources
    result = get_assets({})
    assert len(result) == 1  # Only Generic
