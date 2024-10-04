from dagster_odp.resources.definitions import OdpBigQueryResource


def test_odp_bigquery_resource():
    resource = OdpBigQueryResource()
    result = resource.get_object_to_set_on_execution_context()
    assert callable(result), "Result should be callable (a context manager)"
