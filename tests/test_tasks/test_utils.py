from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import TimePartitioning

from dagster_vayu.tasks.utils import replace_bq_job_params


def test_replace_bq_job_params():
    # Test case with time_partitioning
    params_with_partitioning = {
        "_time_partitioning": {"type_": "DAY", "field": "date"},
        "other_param": "value",
    }
    result = replace_bq_job_params(params_with_partitioning)
    assert isinstance(result["time_partitioning"], TimePartitioning)
    assert result["other_param"] == "value"

    # Test case with schema
    params_with_schema = {
        "_schema": [
            {"name": "id", "field_type": "INTEGER"},
            {"name": "name", "field_type": "STRING"},
        ],
    }
    result = replace_bq_job_params(params_with_schema)
    assert isinstance(result["schema"], list)
    assert all(isinstance(field, SchemaField) for field in result["schema"])

    # Test case without special parameters
    params_without_special = {
        "destination_table": "project.dataset.table",
        "write_disposition": "WRITE_APPEND",
    }
    result = replace_bq_job_params(params_without_special)
    assert result == params_without_special
