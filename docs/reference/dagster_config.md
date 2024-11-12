# Dagster Config Reference

The `dagster_config` file is the central configuration file for defining resources in your dagster-odp project. It serves as the single source of truth for resource configurations that tasks and sensors can use. For a broader understanding of ODP's configuration system, see the [Configuration](../concepts/configuration.md) documentation.

## Overview

### Key Characteristics

- **Single File**: Only one `dagster_config` file is allowed per project
- **Required Name**: Must be named exactly `dagster_config.yaml` or `dagster_config.json`
- **Required Location**: Must be placed in the `odp_config` directory
- **Format Support**: Can be written in either YAML or JSON
- **Resource Uniqueness**: Each resource type can only be defined once

## Basic Structure

The file follows this structure:

```yaml
resources:
  - resource_kind: resource_1
    params:
      param1: value1
      param2: value2

  - resource_kind: resource_2
    params:
      param1: value1

  - resource_kind: resource_3  # Resources without params are valid
```

## Resource Configuration

Each resource configuration requires:

1. `resource_kind`: String identifying the resource type (must be registered with [@odp_resource](../concepts/resources.md#creating-custom-resources))
2. `params` (optional): Configuration parameters specific to that resource

### Validation Rules

- Resource kinds must be registered in ODP's resource registry
- Resource kinds must be unique across the file
- Parameters are validated against the resource's [Pydantic model](../concepts/resources.md#creating-custom-resources)
- Resource must be defined if any task requires it

## Pre-built Resources

### DuckDB Resource

Embedded analytics database integration for local development and testing. [Learn more →](../concepts/resources.md#duckdb-resource)
```yaml
- resource_kind: duckdb
  params:
    database_path: path/to/database.db  # Required
```
Provides connection management, GCS filesystem integration, and automatic connection cleanup.

### BigQuery Resource

Google BigQuery integration for cloud data warehousing. [Learn more →](../concepts/resources.md#google-cloud-resources)
```yaml
- resource_kind: bigquery
  params:
    project: my-project       # Required
    location: us-east1       # Optional
```
Provides direct access to BigQuery's client functionality.

### Google Cloud Storage Resource

Google Cloud Storage integration for cloud object storage. [Learn more →](../concepts/resources.md#google-cloud-resources)
```yaml
- resource_kind: gcs
  params:
    project: my-project      # Required
```
Provides access to Google Cloud Storage operations.

### DLT Resource

Enhanced Data Load Tool integration for configuration-driven data ingestion. [Learn more →](../integrations/dlt.md)
```yaml
- resource_kind: dlt
  params:
    project_dir: dlt_project  # Required: Path to DLT project
```
Manages DLT pipeline execution, secrets, schema creation, and materialization metadata.

### DBT Resource

Enhanced DBT integration for configuration-driven data transformation. [Learn more →](../integrations/dbt.md)
```yaml
- resource_kind: dbt
  params:
    project_dir: dbt_project    # Required: Path to DBT project
    target: dev                 # Optional: DBT target
    load_all_models: true       # Optional: Auto-import all models (default: true)
```
Provides DBT CLI resource creation, external source assets, and enhanced metadata capabilities.

### Soda Resource

Soda Core integration for data quality monitoring. [Learn more →](../integrations/soda.md)
```yaml
- resource_kind: soda
  params:
    project_dir: soda_project     # Required: Path to Soda project
    checks_dir: scans             # Optional: Directory containing check files
```
Enables Soda-powered asset checks with severity mapping and pipeline control.

## Using Resource Configuration

Resources defined in `dagster_config` can be:

1. **Required by Tasks**: 

    Specify resources a task needs through the `required_resources` parameter:
    ```python
    @odp_task(
        "my_task",
        required_resources=["duckdb", "gcs"]
    )
    class MyTask(BaseTask):
        ...
    ```
    See [Tasks and Assets](../concepts/tasks_and_assets.md#creating-custom-tasks) for details on creating tasks that use resources.

2. **Required by Sensors**: 

    Specify resources a sensor needs:
    ```python
    @odp_sensor(
        sensor_type="my_sensor",
        required_resources=["bigquery"]
    )
    class MySensor(BaseSensor):
        ...
    ```

3. **Referenced in Workflows**: 

    Access resource parameters in workflow configurations using [ODP's variable substitution](../concepts/configuration.md#available-variables):
    ```yaml
    params:
      database: "{{resource.duckdb.database_path}}"
      project: "{{resource.bigquery.project}}"
    ```

By properly configuring your `dagster_config` file, you define and validate all the resources your pipeline needs in one place. Rather than writing Python code to create Dagster resources, ODP handles the resource creation and management automatically from your configuration.