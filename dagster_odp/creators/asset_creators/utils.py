from typing import Any, Dict

from dagster._core.definitions.partition import DEFAULT_DATE_FORMAT, ScheduleType
from dagster._utils.partitions import DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE


def generate_partition_params(partition_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates dagster partition parameters.

    Applies default values and converts schedule_type to ScheduleType enum.

    Args:
        partition_params (Dict[str, Any]): Input partition parameters.

    Returns:
        Dict[str, Any]: Normalized partition parameters.
    """
    if not partition_params.get("fmt"):
        partition_params["fmt"] = (
            DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE
            if partition_params.get("schedule_type") == "HOURLY"
            else DEFAULT_DATE_FORMAT
        )

    if partition_params.get("schedule_type"):
        if partition_params["schedule_type"] == "MONTHLY" and not partition_params.get(
            "day_offset"
        ):
            partition_params["day_offset"] = 1

        partition_params["schedule_type"] = getattr(
            ScheduleType, partition_params["schedule_type"]
        )
    return partition_params
