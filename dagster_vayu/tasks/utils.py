from typing import Any, Dict

from google.cloud.bigquery.table import TimePartitioning


def replace_bq_job_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replaces placeholders in the BigQuery job parameters with the corresponding values.

    Args:
        params (Dict[str, Any]): BigQuery job parameters.

    Returns:
        Dict[str, Any]: Updated BigQuery job parameters.
    """

    if "time_partitioning" in params:
        params["time_partitioning"] = TimePartitioning(**params["time_partitioning"])
    return params
