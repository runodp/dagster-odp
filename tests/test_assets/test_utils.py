from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import TimePartitioning
from task_nicely_core.assets.utils import (
    format_date,
    generate_partition_params,
    replace_bq_job_params,
)
from task_nicely_core.configs.models.workflow_model import PartitionParams
from task_nicely_core.utils import (
    ConfigParamReplacer,
    has_partition_def,
    replace_config,
)

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    DagsterInvariantViolationError,
    DailyPartitionsDefinition,
    asset,
    materialize,
)
from dagster._core.definitions.partition import ScheduleType
from dagster._utils.partitions import DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE


def test_format_date():
    assert format_date("2023-05-15|%Y-%m-%d") == "2023-05-15"
    assert format_date("2023-05-15 14:30:00|%H:%M:%S") == "14:30:00"
    assert (
        format_date("2023-05-15T14:30:00+00:00|%Y-%m-%dT%H:%M:%S%z")
        == "2023-05-15T14:30:00+0000"
    )

    with pytest.raises(ValueError):
        format_date("invalid_date|%Y-%m-%d")


@pytest.mark.parametrize(
    "params,config,expected",
    [
        ({"key": "value"}, {}, {"key": "value"}),
        ({"key": "{{var}}"}, {"var": "value"}, {"key": "value"}),
        (
            {"date": "{{#date}}2023-05-15|%Y-%m-%d{{/date}}"},
            {},
            {"date": "2023-05-15"},
        ),
        (
            {"nested": {"key": "{{var}}"}},
            {"var": "value"},
            {"nested": {"key": "value"}},
        ),
        (["{{var}}"], {"var": "value"}, ["value"]),
        ({"key": "{{var}}"}, {}, {"key": ""}),
        (
            {"key": "{{var1}}{{var2}}"},
            {"var1": "hello", "var2": "world"},
            {"key": "helloworld"},
        ),
    ],
)
def test_replace_config(params, config, expected):
    assert replace_config(params, config) == expected


def test_replace_bq_job_params():
    # Test case with time_partitioning
    params_with_partitioning = {
        "time_partitioning": {"type_": "DAY", "field": "date"},
        "other_param": "value",
    }
    result_with_partitioning = replace_bq_job_params(params_with_partitioning)
    assert isinstance(result_with_partitioning["time_partitioning"], TimePartitioning)
    assert result_with_partitioning["time_partitioning"].type_ == "DAY"
    assert result_with_partitioning["time_partitioning"].field == "date"
    assert result_with_partitioning["other_param"] == "value"

    # Test case without time_partitioning
    params_without_partitioning = {
        "destination_table": "project.dataset.table",
        "write_disposition": "WRITE_APPEND",
        "create_disposition": "CREATE_IF_NEEDED",
    }
    result_without_partitioning = replace_bq_job_params(params_without_partitioning)
    assert result_without_partitioning == params_without_partitioning


@pytest.mark.parametrize(
    "partition_params,expected",
    [
        (
            PartitionParams(schedule_type="DAILY", start="2023-01-01"),
            {
                "schedule_type": ScheduleType.DAILY,
                "start": "2023-01-01",
                "fmt": "%Y-%m-%d",
            },
        ),
        (
            PartitionParams(schedule_type="MONTHLY", start="2023-01-01"),
            {
                "schedule_type": ScheduleType.MONTHLY,
                "start": "2023-01-01",
                "fmt": "%Y-%m-%d",
                "day_offset": 1,
            },
        ),
        (
            PartitionParams(
                schedule_type="HOURLY",
                start="2023-01-01",
                fmt="%Y-%m-%d-%H",
            ),
            {
                "schedule_type": ScheduleType.HOURLY,
                "start": "2023-01-01",
                "fmt": "%Y-%m-%d-%H",
            },
        ),
        (
            PartitionParams(schedule_type="HOURLY", start="2023-01-01", fmt=None),
            {
                "schedule_type": ScheduleType.HOURLY,
                "start": "2023-01-01",
                "fmt": DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE,
            },
        ),
    ],
)
def test_generate_partition_params(partition_params, expected):
    result = generate_partition_params(partition_params)
    assert result == expected


def test_has_partition_def():
    context = MagicMock(spec=AssetExecutionContext)
    context.asset_partitions_def_for_input.return_value = MagicMock()
    assert has_partition_def(context, "input_name") is True

    context.asset_partitions_def_for_input.side_effect = DagsterInvariantViolationError(
        "Test error"
    )
    assert has_partition_def(context, "input_name") is False


class TestConfigReplacer:
    @pytest.fixture
    def mock_context(self):
        context = MagicMock(spec=AssetExecutionContext)
        context.resources.sensor_context.sensor_context_config = {
            "sensor_key": "sensor_value"
        }
        context.run.run_id = "test-run-id-12345"
        context.has_partition_key = True
        context.partition_key = "2023-05-15"
        context.partition_time_window.start = datetime(2023, 5, 15, tzinfo=timezone.utc)
        context.partition_time_window.end = datetime(2023, 5, 16, tzinfo=timezone.utc)
        context.asset_partition_keys_for_input = MagicMock(return_value=["2023-05-15"])
        return context

    @pytest.fixture
    def config_replacer(self, mock_context):
        return ConfigParamReplacer(
            context=mock_context,
            depends_on=["parent_asset"],
            resource_config={"resource_key": "resource_value"},
        )

    def test_config_replacer_initialization(self, mock_context):
        replacer = ConfigParamReplacer(
            context=mock_context,
            depends_on=["asset1", "asset2"],
            resource_config={"key": "value"},
        )
        assert replacer.context == mock_context
        assert replacer.depends_on == ["asset1", "asset2"]
        assert replacer.resource_config == {"key": "value"}

    def test_replace(self, config_replacer):
        params = {
            "sensor_param": "{{sensor.sensor_key}}",
            "resource_param": "{{resource.resource_key}}",
            "run_id": "{{context.run_id}}",
            "partition_key": "{{context.partition_key}}",
            "partition_start": "{{context.partition_window_start}}",
            "partition_end": "{{context.partition_window_end}}",
            "parent_param": "{{parent_asset.parent_key}}",
        }

        with patch.object(
            config_replacer, "_add_parent_materializations"
        ) as mock_add_parent:
            mock_add_parent.side_effect = lambda x: x.update(
                {"parent_asset": {"parent_key": "parent_value"}}
            )
            result = config_replacer.replace(params)

        expected = {
            "sensor_param": "sensor_value",
            "resource_param": "resource_value",
            "run_id": "test",
            "partition_key": "2023-05-15",
            "partition_start": "2023-05-15T00:00:00+00:00",
            "partition_end": "2023-05-16T00:00:00+00:00",
            "parent_param": "parent_value",
        }
        assert result == expected

    def test_add_parent_materializations(self, config_replacer, mock_context):
        replacement_config = {}
        mock_instance = MagicMock()
        mock_event = MagicMock()
        mock_event.asset_materialization.metadata = {"key": MagicMock(text="value")}
        mock_instance.get_event_records.return_value = [mock_event]
        mock_context.instance = mock_instance

        config_replacer._add_parent_materializations(replacement_config)

        assert replacement_config == {"parent_asset": {"key": "value"}}

    def test_get_materialization_with_partition(self, config_replacer, mock_context):
        @asset(partitions_def=DailyPartitionsDefinition("2023-01-01"))
        def sample_asset():
            return 1

        with DagsterInstance.ephemeral() as instance:
            materialize([sample_asset], instance=instance, partition_key="2023-05-15")

            with patch(
                "task_nicely_core.assets.utils.has_partition_def", return_value=True
            ):
                result = config_replacer._get_materialization(
                    instance, "sample_asset", "parent_asset"
                )
            mock_context.asset_partition_keys_for_input.assert_called_with(
                "parent_asset"
            )
            assert isinstance(result, AssetMaterialization)
            assert result.asset_key == AssetKey(["sample_asset"])

    def test_get_materialization_without_partition(self, config_replacer):
        @asset
        def sample_asset():
            return 1

        with DagsterInstance.ephemeral() as instance:
            materialize([sample_asset], instance=instance)

            with patch(
                "task_nicely_core.assets.utils.has_partition_def", return_value=False
            ):
                result = config_replacer._get_materialization(
                    instance, "sample_asset", "parent_asset"
                )

            assert isinstance(result, AssetMaterialization)
            assert result.asset_key == AssetKey(["sample_asset"])

    def test_materialization_metadata_to_dict(self, config_replacer):
        mock_metadata = {
            "key1": MagicMock(text="value1"),
            "key2": MagicMock(text="value2"),
        }
        result = config_replacer._materialization_metadata_to_dict(mock_metadata)
        assert result == {"key1": "value1", "key2": "value2"}

        with pytest.raises(ValueError, match="Invalid metadata format"):
            config_replacer._materialization_metadata_to_dict(
                {"invalid_key": "invalid_value"}
            )
