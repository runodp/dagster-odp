from typing import Any, Dict

from dagster._core.definitions.partition import DEFAULT_DATE_FORMAT, ScheduleType
from dagster._utils.partitions import DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE

from ..config_manager.models.workflow_model import PartitionParams


def generate_partition_params(partition_params: PartitionParams) -> Dict[str, Any]:
    """
    Generates dagster partition parameters
    """
    params = partition_params.model_dump(exclude_defaults=True, exclude_unset=True)
    if not params.get("fmt"):
        params["fmt"] = (
            DEFAULT_HOURLY_FORMAT_WITHOUT_TIMEZONE
            if params["schedule_type"] == "HOURLY"
            else DEFAULT_DATE_FORMAT
        )
    if params["schedule_type"] == "MONTHLY" and not params.get("day_offset"):
        params["day_offset"] = 1
    params["schedule_type"] = getattr(ScheduleType, params["schedule_type"])
    return params
