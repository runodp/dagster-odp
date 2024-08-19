from google.cloud.bigquery.table import TimePartitioning

from dagster_vayu.tasks.utils import replace_bq_job_params


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
