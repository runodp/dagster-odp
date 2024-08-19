import pytest
from dagster._core.definitions.partition import DEFAULT_DATE_FORMAT, ScheduleType
from dagster._utils.partitions import DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE

from dagster_vayu.creators.asset_creators.utils import generate_partition_params


@pytest.mark.parametrize(
    "partition_params,expected",
    [
        (
            {"schedule_type": "DAILY", "start": "2023-01-01"},
            {
                "schedule_type": ScheduleType.DAILY,
                "start": "2023-01-01",
                "fmt": DEFAULT_DATE_FORMAT,
            },
        ),
        (
            {"schedule_type": "MONTHLY", "start": "2023-01-01"},
            {
                "schedule_type": ScheduleType.MONTHLY,
                "start": "2023-01-01",
                "fmt": DEFAULT_DATE_FORMAT,
                "day_offset": 1,
            },
        ),
        (
            {
                "schedule_type": "HOURLY",
                "start": "2023-01-01",
                "fmt": "%Y-%m-%d-%H",
            },
            {
                "schedule_type": ScheduleType.HOURLY,
                "start": "2023-01-01",
                "fmt": "%Y-%m-%d-%H",
            },
        ),
        (
            {"schedule_type": "HOURLY", "start": "2023-01-01"},
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
