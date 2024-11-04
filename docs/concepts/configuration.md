# Configuration

Dagster ODP's configuration system is the heart of its "configuration over code" approach. This page explains how ODP's configuration files work together to create data pipelines without extensive coding.

## Configuration Structure

ODP uses two main types of configuration files:

1. **dagster_config file**: Defines resources and their parameters
2. **Workflow files**: Define pipelines and their components

```yaml
# Project Structure Example
odp_config/           # Required directory name
├── dagster_config.yaml     # Resource configuration (YAML or JSON)
└── workflows/             # Required directory name
    ├── initial_load.yaml
    ├── daily_pipeline.json  # Can mix YAML and JSON
    └── monthly_jobs.yaml
```

!!!info "Important Directory and File Names"
    Both directory names (`odp_config` and `workflows`) and the resource configuration file name (`dagster_config`) are required and cannot be modified. ODP uses these specific names to locate and process configuration files.

### Dagster Config File

The `dagster_config` file configures resources that your pipelines will use. Only one `dagster_config` file is allowed per project (either YAML or JSON format).

```yaml
resources:
  - resource_kind: duckdb
    params:
      database_path: data/analysis.db

  - resource_kind: bigquery
    params:
      project: my-project
      location: us-east1
```

Key features:

- Each resource type can only be defined once in a code location
- Parameters are validated against the resource's Pydantic model
- Resources must be defined if any task requires them

### Workflow Configuration

ODP supports both YAML and JSON formats for workflow configuration files. You can split your configuration across multiple files in the `workflows` directory and mix formats as needed. Each workflow file can contain:

```yaml
# Basic workflow file structure
assets:
  - asset_key: asset_1
    task_type: task_1
    params:
      param_1: value_1
      
jobs:
  - job_id: job_1
    description: "Process data daily"
    asset_selection:
      - asset_1*
    triggers:
      - trigger_id: daily_schedule
        trigger_type: schedule
        params:
          schedule_kind: cron
          schedule_params:
            cron_schedule: "@daily"

partitions:
  - assets: ["asset_1"]
    params:
      start: "2022-01-01"
      schedule_type: MONTHLY

soda_checks:
  - asset_key: asset_1
    check_file_path: checks.yml
    data_source: source_1
```

Workflow files don't need to be self-contained. For example, you can:

- Define assets in one file and reference them in jobs defined in another file
- Split related assets across files for better organization
- Use different files for different stages of your pipeline (ingestion, transformation, etc.)


## Available Variables

ODP supports variable substitution in configuration files using mustache syntax (`{{variable}}`). Understanding the scope and limitations of each variable type is crucial for effective pipeline configuration.

### Context Variables
Access runtime information about the current execution:

```yaml
params:
  # Partition information (only available for partitioned assets)
  partition: "{{context.partition_key}}"          # Current partition's key
  start: "{{context.partition_window_start}}"     # Start of partition window
  end: "{{context.partition_window_end}}"         # End of partition window
  
  # Current run information
  run_id: "{{context.run_id}}"                   # Dagster run ID (first part only)
```

- Partition variables are only available when running a partitioned asset
- The partition key and window come from the asset's own partition definition
- The Run ID is Dagster's from current job run

### Resource Variables
Reference any parameter from your resource configurations:

```yaml
params:
  # Access resource parameters defined in dagster_config.yaml
  project: "{{resource.bigquery.project}}"
  dbt_dir: "{{resource.dbt.project_dir}}"
```

- Only parameters defined in `dagster_config.yaml` are accessible
- The syntax is `{{resource.resource_kind.parameter_name}}`
- The resource must be defined in the configuration

### Parent Asset Variables
Access metadata from direct upstream assets:

```yaml
params:
  # Reference metadata from a parent asset
  input_table: "{{parent_asset.destination_table_id}}"
  
  # For parent assets with prefixes in their key
  # parent key: data/ingestion/raw_data
  table_name: "{{data_ingestion_raw_data.destination_table_id}}"
```

- Only metadata from direct parent assets (those listed in `depends_on`) is accessible
- Cannot access metadata from any upstream asset that isn't a direct parent
- When referencing assets with prefixes, replace "/" with "_" in the variable name
- Only metadata returned by the parent's task is available

### Sensor Variables
Access data provided by sensors in triggered jobs:

```yaml
params:
  # Access data from sensor that triggered the job
  file_path: "{{sensor.file_uri}}"
  status: "{{sensor.new_status}}"
```

- Only values explicitly set in the sensor's `sensor_context_config` are accessible
- Variables are only available in jobs triggered by the sensor
- The syntax is `{{sensor.field_name}}` where field_name matches a key in sensor_context_config
- All assets in a sensor-triggered job can access these variables

### Date Formatting

ODP provides a powerful date formatting helper that allows you to transform date variables into specific formats using the mustache syntax. This is particularly useful when different components of your pipeline require dates in different formats.

The syntax for date formatting is:
```
{{#date}}date_value|format_string{{/date}}
```

Components:

- `#date`: Opens the date helper
- `date_value`: The date to format (e.g., `{{context.partition_key}}`)
- `|`: Separator between the date and the format
- `format_string`: Python datetime format string (e.g., `%Y/%m`)
- `/date`: Closes the date helper

Example:
```yaml
params:
  # Transform partition key "2024-01-01" to "2024/01"
  month: "{{#date}}{{context.partition_key}}|%Y/%m{{/date}}"
  
  # Transform partition start "2024-01-01 00:00:00" to "20240101"
  day: "{{#date}}{{context.partition_window_start}}|%Y%m%d{{/date}}"
```

!!!tip "Format String Reference"
    - Use standard Python datetime format codes
    - Common formats:
        - `%Y/%m`: 2024/01
        - `%Y%m%d`: 20240101
        - `%Y-%m-%d`: 2024-01-01

## Partitions

ODP supports defining Dagster time-based partitions for assets through configuration. Partitions allow you to:

- Process data in time-based chunks (e.g., daily, monthly)
- Reprocess specific time periods
- Schedule jobs based on partition boundaries
- Track asset materialization by time period

```yaml
partitions:
  - assets: ["monthly_data", "monthly_metrics"]
    params:
      start: "2022-01-01"
      schedule_type: MONTHLY
      # Optional parameters
      fmt: "%Y-%m-%d"
      day_offset: 1  # For monthly partitions, which day of month

jobs:
  - job_id: monthly_processing
    triggers:
      - trigger_id: monthly_schedule
        trigger_type: schedule
        params:
          schedule_kind: partition  # Uses partition configuration
```

- Each asset can only have one partition definition
- Multiple assets can share the same partition configuration
- Only time-based partitions are currently supported
- Partition parameters are passed into Dagster's [TimeWindowPartitionsDefinition](https://docs.dagster.io/_apidocs/partitions#dagster.TimeWindowPartitionsDefinition), and all arguments of the method are supported
- Assets referenced in partition definitions must exist but can be defined in any workflow file


## Validation

ODP validates configuration at multiple levels:

1. **Schema Validation**: 
    - The `dagster_config` file must match ODP's resource configuration schema
    - Workflow files must match ODP's workflow configuration schema, including assets, jobs, partitions, and soda checks

2. **Resource Validation**: 
    - Resource parameters are validated against their Pydantic models\
    - Resource names must be defined in ODP's resource registry

3. **Task Validation**: 
    - Task parameters are validated against their defined types
    - Tasks must exist in ODP's task registry

4. **Uniqueness Checks**: 
    - Asset keys must be unique across all workflow files
    - Job IDs must be unique across all workflow files
    - Trigger IDs must be unique across all workflow files
    - You cannot use the same name for an asset key, job ID, or trigger ID
    - Resource kinds must be unique across the `dagster_config` file

5. **Partition Validation**: 
    - Assets can only have one partition definition
    - Assets in partition definitions must exist

These validations help catch configuration errors early and ensure your pipeline definitions are consistent and well-formed.

## Best Practices

- Split workflow files by logical grouping (e.g., ingestion, transformation)
- Keep related assets and jobs in the same workflow file
- Use variables as asset definition parameter values wherever possible
- Keep configuration in version control and review configuration changes like code

By leveraging ODP's configuration system effectively, you can create maintainable, scalable data pipelines with minimal code while preserving the power and flexibility of Dagster.
