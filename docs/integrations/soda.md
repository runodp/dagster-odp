# Soda Integration

dagster-odp allows you to create Dagster asset checks powered by Soda Core through configuration. This integration translates Soda check configurations into Dagster asset checks without requiring Python code.

!!!info "Prerequisites"
    This guide assumes familiarity with Soda Core concepts and syntax. If you're new to Soda, we recommend reading the [Soda Core documentation](https://docs.soda.io/soda-core/overview.html) first.

## Understanding ODP's Soda Integration

ODP's Soda integration:

1. **Configuration-Driven**: Define checks in YAML files using Soda's syntax
2. **Asset Check Integration**: Runs checks as Dagster asset checks
3. **Pipeline Control**: Can block downstream assets if checks fail
4. **Severity Mapping**: Maps Soda outcomes to Dagster severity levels

## Resource Configuration

Configure the Soda resource in your Dagster configuration:

```yaml title="dagster_config.yaml"
resources:
  - resource_kind: soda
    params:
      project_dir: soda_checks     # Root directory for Soda files
      checks_dir: scans           # Optional: subdirectory for check files
```

### Parameters

- `project_dir`: Base directory containing all Soda-related files. This directory must contain Soda's `configuration.yml` file. Can be placed anywhere in your project structure.

- `checks_dir`: Optional subdirectory within `project_dir` containing Soda check files. If not specified, ODP looks for check files directly in `project_dir`.

### Directory Structure

A typical setup might look like this:

```
your_project/
└── odp_config/
    ├── soda/                  # project_dir (can be placed anywhere)
    │   ├── configuration.yml  # Soda data source configuration
    │   └── scans/            # checks_dir (optional)
    │       ├── orders.yml    # Check files
    │       └── customers.yml
    ├── dagster_config.yaml
    └── workflows/
        └── pipeline.yaml     # References Soda checks
```

!!!note "Directory Location"
    While this example shows the Soda directory inside `odp_config`, you can place it anywhere in your project. Just make sure the `project_dir` parameter in your resource configuration points to the correct location.

## Required Soda Files

### configuration.yml

Standard Soda configuration file defining data sources. This is a pure Soda configuration file and follows [Soda's configuration format](https://github.com/sodadata/soda-core/blob/main/docs/configuration.md):

```yaml title="configuration.yml"
data_source warehouse:
  type: duckdb
  connection_parameters:
    database: path/to/db.db

data_source cloud_dw:
  type: bigquery
  connection_parameters:
    project_id: my-project
    dataset: my_dataset
```

### Check Files

Standard Soda check files containing the actual data quality checks. These follow [Soda's check syntax](https://docs.soda.io/soda-cl/soda-cl-overview.html):

```yaml title="orders.yml"
checks for orders:
  # All checks in this file run as part of one asset check
  - row_count > 0
  - duplicate_count(order_id) = 0
  - avg(amount) between 0 and 10000
```

## Asset Check Configuration

Define which Soda checks to run as Dagster asset checks in your workflow files:

```yaml title="workflow.yaml"
soda_checks:
  - asset_key: raw_orders          # Asset to check
    check_file_path: orders.yml    # Relative to checks_dir or project_dir
    blocking: true                 # Optional: block downstream on failure
    description: "Order validation" # Optional: describe the check
    data_source: warehouse        # Must match name in configuration.yml
```

1. All checks in a check file run together as a single Dagster asset check
2. You cannot run individual checks from a file separately
3. Each asset check configuration creates one Dagster asset check

### Required Parameters

- `asset_key`: The Dagster asset these checks apply to
- `check_file_path`: Path to the Soda check file (relative to `checks_dir` or `project_dir`)
- `data_source`: Name of the data source from Soda's `configuration.yml`

### Optional Parameters

- `blocking`: If true, prevents downstream asset materialization when checks fail (default: true)
- `description`: Human-readable description shown in the Dagster UI

## Asset Check Results

ODP maps Soda check outcomes to Dagster asset check severities:

| Soda Outcome | Dagster Severity | Effect                                                         |
|--------------|------------------|----------------------------------------------------------------|
| FAIL         | ERROR           | Fails asset check, blocks downstream assets if `blocking: true` |
| WARN         | WARN            | Records warning, allows pipeline to continue                    |
| PASS         | None            | Check passes successfully                                       |

When a check file contains multiple checks:

- If ANY check has outcome `FAIL`, the asset check severity is `ERROR`
- If no checks `FAIL` but AT LEAST ONE has `WARN`, severity is `WARN`
- ALL checks must `PASS` for the asset check to have no severity level

This mapping lets you:

1. Use Soda's built-in warning thresholds
2. Control pipeline execution based on data quality
3. Track data quality results in Dagster's UI

!!!tip "Best Practices"
    - Using one Soda check file per dagster asset is recommended
    - Use `FAIL` for critical data quality issues and `WARN` for potential problems that need investigation
    - Consider impact on downstream assets when setting `blocking`

By using ODP's Soda integration, you can implement Dagster asset checks using Soda's powerful check syntax while keeping your pipeline configuration in one place.
