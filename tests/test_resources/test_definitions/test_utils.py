from unittest.mock import Mock, patch

from dagster import AssetExecutionContext

from dagster_odp.resources.definitions.utils import update_asset_params


@patch("dagster_odp.resources.definitions.utils.ConfigParamReplacer")
def test_update_asset_params(MockConfigParamReplacer):
    # Setup
    mock_context = Mock(spec=AssetExecutionContext)
    mock_resource_config = {"resource1": {"param1": "value1"}}
    mock_asset_params = {"asset_param1": "value2"}
    mock_replacer = Mock()
    MockConfigParamReplacer.return_value = mock_replacer

    # Execute
    result = update_asset_params(mock_context, mock_resource_config, mock_asset_params)

    # Assert
    MockConfigParamReplacer.assert_called_once_with(
        mock_context, None, mock_resource_config
    )
    mock_replacer.replace.assert_called_once_with(mock_asset_params)
    assert result == mock_replacer.replace.return_value
