# DBT Integration

Dagster ODP provides a configuration-driven approach to using Dagster's DBT integration. While Dagster already offers comprehensive DBT support, ODP makes it easier to configure and manage DBT assets through YAML/JSON configuration rather than Python code.

!!!info "Prerequisites"
    This guide assumes familiarity with DBT and Dagster's DBT integration. If you're new to DBT, we recommend reading the [DBT Documentation](https://docs.getdbt.com/docs/introduction) first.

## Resource Configuration

The DBT resource in ODP creates a Dagster `DbtCliResource` using configuration:

```yaml title="dagster_config.yaml"
resources:
  - resource_kind: dbt
    params:
      project_dir: dbt_project     # Path to your DBT project
      profile: my_profile          # Must match profiles.yml
      target: dev                  # Target from profiles.yml
      load_all_models: true        # ODP-specific parameter
```

All parameters supported by Dagster's [DbtCliResource](https://docs.dagster.io/_apidocs/libraries/dagster-dbt#cli-resource) can be used in the configuration. ODP adds one additional parameter:

- `load_all_models`: Controls whether all DBT models are automatically imported as Dagster assets (default: true)

## Asset Creation and Management

ODP provides two approaches to creating Dagster assets from DBT models:

### 1. Automatic Import

When `load_all_models` is true (default), ODP automatically creates Dagster assets for all DBT models in your project. This is the recommended approach as it:

- Creates assets for all DBT models and their dependencies
- Requires minimal configuration

The asset creation process is handled by Dagster's DBT integration, with ODP providing the configuration-driven resource setup.

### 2. Explicit Asset Definition

Regardless of the `load_all_models` setting, you can define explicit DBT assets in your workflow files for several use cases:

1. Import only specific DBT models into Dagster
2. Add time-based partitioning to incremental models
3. Pass specific variables to a set of models
4. Configure different partitions for different model selections

```yaml title="workflow_config.yaml"
assets:
  - asset_key: monthly_metrics
    task_type: dbt
    description: "Calculate monthly business metrics"
    group_name: metrics
    params:
      selection: "tag:metrics"  # DBT selection syntax
      dbt_vars:                # Variables passed to DBT
        start_date: "{{context.partition_window_start}}"
```

When `load_all_models` is true and you define explicit DBT assets:

- The specified models are configured with your custom settings (partitions, variables, etc.)
- Any remaining models are automatically created as a single `unselected_dbt_models` asset
- All models remain available in Dagster

When `load_all_models` is false:

- Only models specified in explicit DBT assets are imported into Dagster

### Multiple Selections and Partitions

One of ODP's key features is the ability to configure different DBT model selections with different partitions, all through configuration:

```yaml title="workflow_config.yaml"
assets:
  - asset_key: hourly_metrics
    task_type: dbt
    params:
      selection: "tag:hourly"
      dbt_vars:
        target_date: "{{context.partition_key}}"

  - asset_key: monthly_metrics
    task_type: dbt
    params:
      selection: "tag:monthly"
      dbt_vars:
        target_month: "{{context.partition_key}}"

partitions:
  - assets: ["hourly_metrics"]
    params:
      start: "2024-01-01"
      schedule_type: HOURLY

  - assets: ["monthly_metrics"]
    params:
      start: "2024-01-01"
      schedule_type: MONTHLY

jobs:
  - job_id: metrics_job
    asset_selection: ["hourly_metrics", "monthly_metrics"]
```

This configuration:

- Creates separate assets for hourly and monthly metrics
- Each selection has its own partition definition
- Both can be run in the same job
- Variables are passed to the appropriate models
- All managed through configuration without Python code

## DBT Variables

When defining a DBT asset, the `dbt_vars` parameter is passed directly to DBT's `--vars` command-line argument after variable substitution. For example:

```yaml title="workflow_config.yaml"
assets:
  - asset_key: transform_data
    task_type: dbt
    params:
      selection: "tag:transform"
      dbt_vars:
        start_date: "{{context.partition_window_start}}"
        env: "{{resource.env.name}}"
```

These variables are only passed to the models in this specific selection. If you have multiple DBT assets, each can have its own variables.

## External Sources

ODP enhances Dagster's DBT integration by allowing you to create external Dagster assets directly from your DBT `sources.yml` file. This is particularly useful when:

- Your source data isn't managed by Dagster but you want to see its lineage in the UI
- You want source tables to appear in specific asset groups
- You need to document sources with descriptions that appear in Dagster

To use this feature, add Dagster metadata to your DBT sources:

```yaml title="models/sources.yml"
version: 2
sources:
  - name: raw_data
    tables:
      - name: events
        meta:
          dagster:
            group: raw_data          # Asset group name
            external: true           # Enable external asset creation
            description: "Raw event data"
            asset_key: ["events"]    # Asset key in Dagster
```

When `external: true` is set, ODP automatically creates an external dagster asset representing this source

## Materialization Metadata

ODP adds the full table name (including schema) to each DBT model's materialization metadata under the `destination_table_id` field. This is particularly useful for non-DBT assets that depend on DBT models:

```yaml title="workflow_config.yaml"
assets:
  - asset_key: export_metrics
    task_type: gcs_table_export
    depends_on: ["monthly_metrics"]  # A DBT model
    params:
      # Access the DBT model's full table name
      source_table: "{{monthly_metrics.destination_table_id}}"
```

!!!note "DBT Dependencies"
    Dependencies between DBT models are automatically handled by Dagster's DBT integration using the models' refs and sources. You don't need to use materialization metadata for DBT-to-DBT dependencies.

## Best Practices

1. **Model Organization**
    - Use DBT tags to organize models that need similar:
        - Partitioning schedules
        - Variable sets
        - Run frequencies

2. **Partitioning**
    - Group models with similar partition requirements
    - Consider dependencies and jobs when setting up partitions

3. **Variable Management**
    - Only pass variables that change between runs
    - Use consistent variable names across related models
    - Leverage ODP's variable substitution for dynamic values

By using ODP's configuration-driven approach to DBT integration, you can effectively manage complex transformation pipelines with multiple partition schedules and variable sets, all without writing Python code.
