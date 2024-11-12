# Tasks and Assets

Tasks and assets are the foundational building blocks in dagster-odp. While Dagster provides software-defined assets through Python code, ODP introduces tasks as reusable components that create assets through configuration. This enables a declarative approach to building data pipelines.

## Understanding Tasks

### Tasks as Asset Generators

A task in ODP represents a single, reusable operation that produces a Dagster asset. Every task:

1. **Defines Parameters**: Uses Pydantic for automatic validation of configuration parameters
2. **Specifies Resources**: Declares what Dagster resources it needs through the `required_resources` parameter
3. **Implements Logic**: Contains the code to create or modify an asset in the `run()` method
4. **Returns Metadata**: Returns Dagster materialization metadata, which can be referenced by downstream assets

Here's an example of a task that downloads files:

```python
@odp_task(
    "url_file_download",
    compute_kind="python",
    storage_kind="filesystem",
)
class UrlFileDownload(BaseTask):
    """Downloads a file from a URL."""
    source_url: str
    destination_file_path: str

    def run(self) -> Dict:
        response = requests.get(self.source_url)
        with open(self.destination_file_path, 'wb') as f:
            f.write(response.content)
        return {
            "file_size": len(response.content),
            "destination": self.destination_file_path
        }
```

### Creating Assets from Tasks

While tasks are defined in Python, assets are created by configuring tasks in YAML/JSON. The same task can be used to create multiple different assets. For example, the `url_file_download` task can create multiple assets that download different files:

```yaml title="workflow.yaml"
assets:
  - asset_key: raw_weather_data
    task_type: url_file_download  # References our UrlFileDownload task
    description: "Download weather data from URL"
    group_name: data_ingestion
    params:
      source_url: https://example.com/weather_2024_01.parquet
      destination_file_path: ./data/weather_2024_01.parquet

  - asset_key: raw_country_data
    task_type: url_file_download  # Same task, different parameters
    description: "Download country data from URL" 
    group_name: data_ingestion
    params:
      source_url: https://example.com/country_codes.csv
      destination_file_path: ./data/country_codes.csv
```

Each asset definition requires:

1. **asset_key**: A unique identifier for the asset
2. **task_type**: The registered name of the task to use
3. **params**: Configuration parameters required by the task
4. **group_name** (optional): Groups related assets together in the UI
5. **description** (optional): Documents the asset's purpose
6. **depends_on** (optional): Lists asset dependencies

### Asset Dependencies and Metadata Communication

Assets can depend on other assets and communicate through metadata. This creates a powerful way to build data pipelines:

1. **Declaring Dependencies**: Use the `depends_on` field to specify upstream assets
2. **Metadata Communication**: Tasks return metadata that downstream assets can access
3. **Variable Reference**: Use handlebar syntax to reference parent metadata:
    - Single level: `{{asset_key.metadata_field}}`
    - For assets with prefixes: Replace '/' with '_' (e.g., `{{parent/asset.field}}` becomes `{{parent_asset.field}}`)

Example of dependency and metadata communication:

```yaml title="workflow.yaml"
assets:
  # Parent asset
  - asset_key: raw_data
    task_type: gcs_file_download
    params:
      source_file_uri: "gs://bucket/data.parquet"
      destination_file_path: "data/"

  # Child asset using parent's metadata
  - asset_key: processed_data
    task_type: file_to_duckdb
    depends_on: [raw_data]  # Declares dependency
    params:
      # Access parent's metadata using handlebar syntax
      source_file_uri: "{{raw_data.destination_file_path}}/data.parquet"
      destination_table_id: "processed_table"
```

### Creating Custom Tasks

Tasks are Python classes that inherit from `BaseTask`. Here's how to create a custom task:

1. **Define the Class**:
```python
@odp_task(
    "custom_task_name",
    compute_kind="python",
    storage_kind="database",
    required_resources=["resource1", "resource2"]
)
class CustomTask(BaseTask):
    input_param: str
    optional_param: Optional[int] = None
    complex_param: Dict[str, List[float]]
```

2. **Add Parameter Validation**:
```python
from typing import List, Optional
from pydantic import Field, field_validator

@odp_task("validated_task")
class ValidatedTask(BaseTask):
    file_paths: List[str]
    batch_size: int = Field(gt=0, lt=1000)  # Must be between 0 and 1000
    prefix: Optional[str] = None

    @field_validator("file_paths")
    def validate_paths(cls, paths):
        for path in paths:
            if not path.endswith(('.csv', '.parquet')):
                raise ValueError("Only CSV and Parquet files supported")
        return paths
```

3. **Implement the Run Method**:
```python
def run(self) -> Dict[str, Any]:
    # Access resources through self._resources
    resource = self._resources["resource1"]
    
    # Access context through self._context
    self._context.log.info("Processing...")
    
    # The _resources and _context attributes are automatically 
    # injected by ODP when the task runs
    
    # Perform operations
    result = process_data()
    
    # Return metadata for downstream assets
    return {
        "output_path": result.path,
        "row_count": result.count
    }
```

When this task is used in a workflow configuration, ODP will:

- Validate all parameters have correct types
- Ensure required parameters are provided
- Check values meet any defined constraints
- Apply custom validation rules
- Make metadata available to downstream assets

Each task has access to:

- `self._context`: The `AssetExecutionContext` from Dagster
- `self._resources`: Dictionary of configured required resources

!!!info "Important"
    After creating a custom task, you must import it in your `__init__.py` or `definitions.py` file for Python to execute the decorator and register the task with ODP. Simply defining the task class isn't enough - it needs to be imported where Dagster loads your code.

    For example:
    ```python
    # definitions.py
    from .tasks.custom_task import CustomTask  # Import your custom task
    from dagster_odp import build_definitions

    defs = build_definitions("odp_config")
    ```

## Pre-built Tasks

ODP provides several pre-built tasks for common data operations:

### DuckDB Operations

#### 1. file_to_duckdb
Loads data from a file into a DuckDB table. Supports both local files and Google Cloud Storage (GCS).

```yaml
task_type: file_to_duckdb
params:
  source_file_uri: "path/to/file.parquet"  # Local path or gs:// URI
  destination_table_id: "my_table"         # Table name to create
```

**Required Resources**: `duckdb`

**Returns**:

- `row_count`: Number of rows loaded
- `destination_table_id`: Name of created table
- `source_file_uri`: Source file location

#### 2. duckdb_query
Executes SQL queries against DuckDB. Can read queries from strings or files.

```yaml
task_type: duckdb_query
params:
  query: "SELECT * FROM my_table"  # SQL query or file path
  is_file: false                   # Set true if query is in a file
```

**Required Resources**: `duckdb`

**Returns**:

- `row_count`: Number of rows in result (if applicable)
- `column_names`: List of columns in result

#### 3. duckdb_table_to_file
Exports a DuckDB table to a file. Supports local files and GCS.

```yaml
task_type: duckdb_table_to_file
params:
  source_table_id: "my_table"              # Table to export
  destination_file_uri: "output/data.csv"  # Where to save
```

**Required Resources**: `duckdb`

**Returns**:

- `row_count`: Number of rows exported
- `source_table_id`: Source table name
- `destination_file_uri`: Output location

### Google Cloud Operations

ODP provides tasks for common Google Cloud Storage (GCS) and BigQuery operations, with direct pass-through of configuration parameters to the underlying Google Cloud libraries.

#### 1. gcs_file_to_bq
Loads data from Google Cloud Storage into BigQuery.

```yaml
task_type: gcs_file_to_bq
params:
  # Required parameters
  source_file_uri: "gs://bucket/path/file.csv"     # GCS file location
  destination_table_id: "project.dataset.table"     # Target BigQuery table
  
  # Optional BigQuery LoadJobConfig parameters
  job_config_params:
    # Special handling for time partitioning
    _time_partitioning:
      type_: "DAY"
      field: "date_column"
    
    # Special handling for schema definition
    _schema:
      - name: "column1"
        field_type: "STRING"
    
    # All other parameters passed directly to LoadJobConfig
    autodetect: true
    source_format: "PARQUET"
```

**Required Resources**: `bigquery`

**Returns**:

- `source_file_uri`: The original source URI
- `destination_table_id`: The target table ID
- `row_count`: Number of rows loaded

All parameters in `job_config_params` are passed directly to BigQuery's `LoadJobConfig`, except for `_time_partitioning` and `_schema` which are preprocessed into their respective BigQuery objects.

#### 2. bq_table_to_gcs
Exports BigQuery tables to GCS files.

```yaml
task_type: bq_table_to_gcs
params:
  # Required parameters
  source_table_id: "project.dataset.table"        # Source BigQuery table
  destination_file_uri: "gs://bucket/path/file-*.csv"  # Target GCS location
  
  # Optional ExtractJobConfig parameters
  job_config_params:
    destination_format: "PARQUET"
    compression: "SNAPPY"
```

**Required Resources**: `bigquery`

**Returns**:

- `source_table_id`: The source table ID
- `destination_file_uri`: Base path where files were exported
- `row_count`: Number of rows exported

All parameters in `job_config_params` are passed directly to BigQuery's `ExtractJobConfig`.

#### 3. gcs_file_download
Downloads files from GCS to local filesystem. Supports downloading single files or multiple files matching a prefix.

```yaml
task_type: gcs_file_download
params:
  source_file_uri: "gs://bucket/path/"        # GCS path or prefix
  destination_file_path: "/local/path/"       # Local directory path
```

- The `source_file_uri` must start with `gs://`
- `destination_file_path` must be a directory path, not a file path
- Directory structure from GCS is preserved locally

**Required Resources**: `gcs`

**Returns**:

- `source_file_uri`: The original source URI
- `destination_file_path`: The local directory path
- `file_count`: Number of files downloaded
- `total_size_bytes`: Total size of downloaded files


### Utility Operations

#### shell_command
Executes shell commands with optional environment variables and working directory.

```yaml
task_type: shell_command
params:
  command: "python script.py"           # Command to execute
  env_vars:                            # Optional environment variables
    MY_VAR: "value"
  working_dir: "/path/to/dir"         # Optional working directory
```

**Returns**:

- `command`: Executed command
- `return_code`: Command's exit code

## Best Practices

When working with tasks and assets in ODP, follow these key practices:

1. **Task Design**
    - Design tasks to be reusable across different pipelines and asset types
    - Validate parameter values using Pydantic data types.
    - Use the `_context` attribute when required to access the Dagster `AssetExecutionContext`
    - Use appropriate `compute_kind` and `storage_kind` tags to help with asset visualization
    - Return standardized metadata keys across similar tasks (e.g., always use `destination_table_id` for table names)

2. **Asset Configuration**
    - Group related assets using `group_name` to organize assets in the Dagster UI
    - Break up large workflow files into logical groups rather than having one large configuration

3. **Resources**
    - Only declare required resources that the task actually uses through `required_resources`
    - Use ODP's resource parameter substitution (`{{resource.resource_kind.param}}`) in workflow files instead of hardcoding values
    - Leverage ODP pre-built resources where possible for their enhanced capabilities (like DLT's automatic asset creation)

This configuration-driven approach lets you build complex pipelines by composing and configuring tasks, making the most of ODP's features for asset management and dependency handling.
