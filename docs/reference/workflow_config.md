# Workflow Configuration Reference

Workflow files define the pipelines in your Dagster ODP project through assets, jobs, partitions, and data quality checks. Unlike the `dagster_config` file, you can have multiple workflow files in YAML or JSON format. For a broader understanding of ODP's configuration system, see the [Configuration](../concepts/configuration.md) documentation.

## Overview

### Key Characteristics

- **Multiple Files**: Can split configuration across multiple files
- **Required Location**: Must be placed in the `odp_config/workflows` directory
- **Format Support**: Can mix YAML and JSON files
- **Modular Configuration**: Files don't need to be self-contained
- **Cross-File References**: Can reference assets defined in other workflow files

## Basic Structure

Each workflow file can contain any combination of these top-level sections:

```yaml
assets:
  - asset_key: asset_1
    task_type: task_1
    params:
      param1: value1

jobs:
  - job_id: job_1
    asset_selection: ["asset_1"]
    triggers:
      - trigger_id: trigger_1
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
    check_file_path: checks/check1.yml
    data_source: my_source
```

### Validation Rules

- Asset keys must be unique across all workflow files
- Job IDs must be unique across all workflow files
- Trigger IDs must be unique across all workflow files
- Asset keys, job IDs, and trigger IDs cannot overlap
- Assets referenced in partitions must be defined in workflow files
- Each asset can only have one partition definition

### Asset Structure

Assets are the core building blocks of ODP pipelines, created by configuring tasks. For more details on tasks, see [Tasks and Assets](../concepts/tasks_and_assets.md).

```yaml
assets:
  - asset_key: my_asset           # Required: Unique identifier
    task_type: my_task           # Required: Registered task type
    description: "Description"    # Optional: Human-readable description
    group_name: "my_group"       # Optional: Group for UI organization
    depends_on: ["other_asset"]  # Optional: Asset dependencies
    params:                      # Required only if task defines required params
      param1: value1
      param2: value2
```

### Asset Dependencies

Dependencies between assets are managed through the `depends_on` field and materialization metadata:

```yaml
assets:
  - asset_key: raw_data
    task_type: file_download
    params:
      destination: "data/file.csv"

  - asset_key: processed_data
    task_type: process_file
    depends_on: ["raw_data"]
    params:
      # Access parent's metadata
      input_file: "{{raw_data.destination}}"
```

For assets with prefixes in their keys, replace `/` with `_` when referencing metadata:
```yaml
# Parent: data/ingestion/raw_data
input_path: "{{data_ingestion_raw_data.destination}}"
```

## Task-Specific Configurations

ODP provides several pre-built tasks for common data operations. Each task type has specific configuration requirements.

### Data Integration Tasks

#### DLT Task
Ingest data from various sources (APIs, databases) using Data Load Tool with automatic asset creation and dependency management.

[Learn more about DLT integration →](../integrations/dlt.md)

```yaml
- asset_key: github/api/pull_requests  # Must contain at least one '/'
  task_type: dlt
  params:
    source_module: github.source       # Path to DLT source
    schema_file_path: schemas/github.schema.yaml
    source_params:                     # Passed to DLT source
      param1: value1
    destination: duckdb               # DLT destination type
    destination_params: {}            # Destination configuration
    pipeline_params:                  # DLT pipeline configuration
      dataset_name: github           # Required
    run_params:                      # DLT run configuration
      write_disposition: append
```

#### DBT Task
Transform data using DBT models with support for variables, partitioning, and external sources.

[Learn more about DBT integration →](../integrations/dbt.md)

```yaml
- asset_key: transform_data
  task_type: dbt
  params:
    selection: "tag:transform"     # DBT selection syntax
    dbt_vars:                     # Variables passed to DBT
      var1: value1
```

### Data Operation Tasks

[Pre-built task details →](../concepts/tasks_and_assets.md#pre-built-tasks)

#### DuckDB Operations
- **file_to_duckdb**: Load data from files (local or GCS) into DuckDB tables
- **duckdb_query**: Execute SQL queries against DuckDB tables
- **duckdb_table_to_file**: Export DuckDB tables to files

#### Cloud Operations
- **gcs_file_to_bq**: Load data from Google Cloud Storage into BigQuery
- **bq_table_to_gcs**: Export BigQuery tables to Google Cloud Storage
- **gcs_file_download**: Download files from Google Cloud Storage to local filesystem

#### Utility Operations
- **shell_command**: Execute shell commands with configurable environment and working directory

## Job Configuration

Jobs define what assets to materialize and how to trigger materializations. See [Automation](../concepts/automation.md/#jobs) for more details.

### Basic Job Structure

```yaml
jobs:
  - job_id: my_job              # Required: Unique identifier
    description: "Description"   # Optional: Human-readable description
    asset_selection:            # Required: Assets to materialize
      - asset_1*               # Supports Dagster selection syntax
    triggers:                   # Optional: Automation triggers
      - trigger_id: trigger_1
        trigger_type: schedule  # or sensor
        params:
          # Trigger-specific configuration
```

### Trigger Types

#### Schedule Trigger
Define time-based execution using either cron expressions or partition configurations.

```yaml
triggers:
  # Cron-based schedule
  - trigger_id: daily_run
    trigger_type: schedule
    params:
      schedule_kind: cron
      schedule_params:
        cron_schedule: "@daily"  # or "0 0 * * *"

  # Partition-based schedule
  - trigger_id: monthly_run
    trigger_type: schedule
    params:
      schedule_kind: partition   # Uses partition configuration
```

#### Sensor Trigger
Monitor external systems and trigger runs in response to events or state changes.

```yaml
triggers:
  - trigger_id: new_file_trigger
    trigger_type: sensor
    params:
      sensor_kind: gcs_sensor   # Must be registered
      sensor_params:
        bucket_name: my-bucket
        path_prefix: data/
```

## Partition Configuration

Partitions enable time-based processing of assets. ODP only supports time-based partitions. See [Configuration](../concepts/configuration.md#partitions) for more details.

### Basic Structure

```yaml
partitions:
  - assets: ["asset_1", "asset_2"]  # Assets to partition
    params:
      start: "2022-01-01"          # Required: Start date
      schedule_type: MONTHLY        # Required unless cron_schedule set
      # Optional parameters
      end: "2024-01-01"           
      timezone: "UTC"
      fmt: "%Y-%m-%d"              # Date format
      day_offset: 1                # For monthly partitions
```

## Soda Check Configuration

Soda checks provide data quality monitoring through Dagster asset checks. See [Soda Integration](../integrations/soda.md) for more details.

### Basic Structure

```yaml
soda_checks:
  - asset_key: my_asset          # Asset to check
    check_file_path: check.yml   # Path to Soda check file
    data_source: my_source      # Defined in Soda configuration
    blocking: true              # Optional: Block on failure
    description: "Description"   # Optional
```

## Variable Substitution

ODP supports variable substitution in configuration using mustache syntax (`{{variable}}`). For details, see [Configuration](../concepts/configuration.md#available-variables).

### Available Variables

1. **Context Variables**

    ```yaml
    params:
      # Basic context
      run_id: "{{context.run_id}}"
      
      # Partition context
      partition_key: "{{context.partition_key}}"
      window_start: "{{context.partition_window_start}}"
      window_end: "{{context.partition_window_end}}"
    ```

2. **Resource Variables**

    ```yaml
    params:
      project: "{{resource.bigquery.project}}"
    ```

3. **Parent Asset Variables**

    ```yaml
    params:
      input: "{{parent_asset.destination_table_id}}"
    ```

4. **Sensor Variables**

    ```yaml
    params:
      file: "{{sensor.file_uri}}"
    ```

### Date Formatting

Transform date variables using the date helper:
```yaml
formatted_date: "{{#date}}{{context.partition_key}}|%Y/%m{{/date}}"
```

By using these configuration options effectively, you can build sophisticated data pipelines that leverage ODP's task system and integrations.
