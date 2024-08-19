from unittest.mock import Mock, patch

from dagster_vayu.resources.utils import update_config_params


@patch("dagster_vayu.resources.utils.ConfigParamReplacer")
def test_update_config_params(MockConfigParamReplacer):
    # Setup
    mock_context = Mock()
    mock_config = Mock()
    mock_replacer = Mock()
    MockConfigParamReplacer.return_value = mock_replacer

    # Execute
    update_config_params(mock_context, mock_config)

    # Assert
    mock_replacer.replace.assert_called_once_with(mock_config)
