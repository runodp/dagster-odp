from typing import Any, Dict

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import TimePartitioning


def replace_bq_job_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replaces placeholders in the BigQuery job parameters with the corresponding values.

    Args:
        params (Dict[str, Any]): BigQuery job parameters.

    Returns:
        Dict[str, Any]: Updated BigQuery job parameters.
    """
    if "_time_partitioning" in params:
        params["time_partitioning"] = TimePartitioning(**params["_time_partitioning"])

    if "_schema" in params:
        params["schema"] = [create_schema_field(field) for field in params["_schema"]]

    return params


def create_schema_field(field_dict: Dict[str, Any]) -> SchemaField:
    """
    Creates a SchemaField object from a dictionary.

    Args:
        field_dict (Dict[str, Any]): Dictionary containing field properties.

    Returns:
        SchemaField: A BigQuery SchemaField object.
    """
    # Handle nested fields recursively
    if "fields" in field_dict:
        field_dict["fields"] = [create_schema_field(f) for f in field_dict["fields"]]

    # Create and return the SchemaField object using unpacking
    return SchemaField(**field_dict)
