from dagster_vayu.resources.definitions import VayuBigQueryResource


def test_vayu_bigquery_resource():
    resource = VayuBigQueryResource()
    result = resource.get_object_to_set_on_execution_context()
    assert callable(result), "Result should be callable (a context manager)"
