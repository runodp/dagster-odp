DEFAULT_CONFIG = {
    "tasks": [
        {
            "name": "gcs_file_to_bq",
            "required_resources": ["bigquery"],
            "storage_kind": "bigquery",
            "compute_kind": "bigquery",
        },
        {
            "name": "bq_table_to_gcs",
            "required_resources": ["bigquery"],
            "storage_kind": "googlecloud",
            "compute_kind": "bigquery",
        },
        {
            "name": "file_to_duckdb",
            "required_resources": ["duckdb"],
            "storage_kind": "duckdb",
            "compute_kind": "duckdb",
        },
        {
            "name": "duckdb_query",
            "required_resources": ["duckdb"],
            "compute_kind": "duckdb",
        },
        {
            "name": "duckdb_table_to_file",
            "required_resources": ["duckdb"],
            "compute_kind": "duckdb",
        },
    ],
    "sensors": [{"name": "gcs_sensor", "required_resources": ["gcs"]}],
}
