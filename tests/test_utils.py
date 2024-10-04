from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from dagster import AssetExecutionContext, DagsterInvariantViolationError
from freezegun import freeze_time

from dagster_odp.utils import ConfigParamReplacer, has_partition_def


@pytest.fixture
def mock_context():
    context = Mock(spec=AssetExecutionContext)
    context.resources.sensor_context.sensor_context_config = {
        "sensor_key": "sensor_value"
    }
    context.run.run_id = "test-run-id-12345"
    context.has_partition_key = True
    context.partition_key = "2023-05-15"
    context.partition_time_window.start = datetime(2023, 5, 15, tzinfo=timezone.utc)
    context.partition_time_window.end = datetime(2023, 5, 16, tzinfo=timezone.utc)
    context.asset_partition_keys_for_input = Mock(return_value=["2023-05-15"])
    return context


@pytest.fixture
def config_replacer(mock_context):
    return ConfigParamReplacer(
        context=mock_context,
        depends_on=["parent_asset"],
        resource_config={"resource_key": "resource_value"},
    )


def test_has_partition_def(mock_context):
    mock_context.asset_partitions_def_for_input.return_value = Mock()
    assert has_partition_def(mock_context, "input_name") is True

    mock_context.asset_partitions_def_for_input.side_effect = (
        DagsterInvariantViolationError("Test error")
    )
    assert has_partition_def(mock_context, "input_name") is False


class TestConfigParamReplacer:
    def test_format_date(self, config_replacer):
        assert config_replacer._format_date("2023-05-15|%Y-%m-%d") == "2023-05-15"
        assert (
            config_replacer._format_date("2023-05-15 14:30:00|%H:%M:%S") == "14:30:00"
        )

        with pytest.raises(ValueError):
            config_replacer._format_date("invalid_date|%Y-%m-%d")

    def test_replace_config(self, config_replacer):
        params = {
            "simple": "value",
            "template": "{{var}}",
            "date": "{{#date}}2023-05-15|%Y-%m-%d{{/date}}",
            "nested": {"key": "{{var}}"},
            "list": ["{{var}}"],
        }
        config = {"var": "replaced"}

        result = config_replacer._replace_config(params, config)

        assert result == {
            "simple": "value",
            "template": "replaced",
            "date": "2023-05-15",
            "nested": {"key": "replaced"},
            "list": ["replaced"],
        }

    @freeze_time("2023-05-17T10:30:00")
    @patch("dagster_odp.utils.ConfigParamReplacer._get_materialization")
    def test_replace(self, mock_get_materialization, config_replacer):
        mock_get_materialization.return_value = Mock(
            metadata={"parent_key": Mock(text="parent_value")}
        )

        params = {
            "sensor_param": "{{sensor.sensor_key}}",
            "resource_param": "{{resource.resource_key}}",
            "run_id": "{{context.run_id}}",
            "partition_key": "{{context.partition_key}}",
            "partition_start": "{{context.partition_window_start}}",
            "partition_end": "{{context.partition_window_end}}",
            "parent_param": "{{parent_asset.parent_key}}",
            "current_time": "{{utils.now}}",
        }

        result = config_replacer.replace(params)

        expected = {
            "sensor_param": "sensor_value",
            "resource_param": "resource_value",
            "run_id": "test",
            "partition_key": "2023-05-15",
            "partition_start": "2023-05-15T00:00:00+00:00",
            "partition_end": "2023-05-16T00:00:00+00:00",
            "parent_param": "parent_value",
            "current_time": datetime.now().isoformat(),
        }

        assert result == expected

    @patch("dagster_odp.utils.ConfigParamReplacer._get_materialization")
    def test_add_parent_materializations(
        self, mock_get_materialization, config_replacer
    ):
        mock_materialization = Mock()
        mock_materialization.metadata = {"key": Mock(text="value")}
        mock_get_materialization.return_value = mock_materialization

        replacement_config = {}
        config_replacer._add_parent_materializations(replacement_config)

        assert replacement_config == {"parent_asset": {"key": "value"}}
        mock_get_materialization.assert_called_once()

    def test_get_materialization(self, config_replacer, mock_context):
        mock_instance = Mock()
        mock_event = Mock()
        mock_event.asset_materialization.metadata = {"key": Mock(text="value")}
        mock_instance.get_event_records.return_value = [mock_event]
        mock_context.instance = mock_instance

        with patch("dagster_odp.utils.has_partition_def", return_value=True):
            result = config_replacer._get_materialization(
                mock_instance, "parent_asset", "parent_asset"
            )
        assert result == mock_event.asset_materialization

        with patch("dagster_odp.utils.has_partition_def", return_value=False):
            mock_instance.get_latest_materialization_event.return_value = mock_event
            result = config_replacer._get_materialization(
                mock_instance, "parent_asset", "parent_asset"
            )
        assert result == mock_event.asset_materialization

    def test_materialization_metadata_to_dict(self, config_replacer):
        mock_metadata = {
            "key1": Mock(text="value1"),
            "key2": Mock(text="value2"),
        }
        result = config_replacer._materialization_metadata_to_dict(mock_metadata)
        assert result == {"key1": "value1", "key2": "value2"}

        with pytest.raises(ValueError, match="Invalid metadata format"):
            config_replacer._materialization_metadata_to_dict(
                {"invalid_key": "invalid_value"}
            )
