# dagster-odp (open data platform)

dagster-odp simplifies data pipeline development by enabling teams to build Dagster pipelines through configuration rather than code. It reduces the learning curve for Dagster while promoting standardization and faster development of data workflows.

## Key Features

- **Configuration-Driven Development**: Build data pipelines using YAML/JSON instead of Python code

- **Pre-built Tasks**:
  - **Google Cloud Operations**: Transfer and export data between GCS and BigQuery, with support for GCS file downloads.
  - **DuckDB Operations**: Load files into DuckDB, execute SQL queries, and export table contents to files.
  - **Utility Operations**: Execute shell commands with configurable environments and working directories.

- **Extensible Framework**: Create custom tasks, sensors, and resources that can be used directly in configuration files

- **Enhanced Modern Data Stack Integration**:
  - **DLT+**: Extended integration with automatic asset creation and granular object handling
  - **DBT+**: Simplified variable management and external source configuration
  - **Soda**: Configuration-driven data quality checks

- **Enhanced Asset Management**:
  - Standardized materialization metadata
  - Simplified dependency management
  - External source handling

- **Flexible Automation**: Configuration-based jobs, schedules, sensors, and partitioning


## Quick Example

Here's a simple pipeline that downloads data and loads it into DuckDB:

```yaml
# odp_config/workflows/pipeline.yaml
assets:
  - asset_key: raw_data
    task_type: url_file_download
    params:
      source_url: https://example.com/data.parquet
      destination_file_path: ./data/raw.parquet

  - asset_key: analyzed_data
    task_type: file_to_duckdb
    depends_on: [raw_data]
    params:
      source_file_uri: "{{raw_data.destination_file_path}}"
      destination_table_id: analyzed_table
```

## Installation

```bash
pip install dagster-odp
```

## Getting Started

1. Create a new project using the Dagster CLI:
```bash
dagster project scaffold --name my-odp-project
cd my-odp-project
```

2. Create the ODP configuration directories:
```bash
mkdir -p odp_config/workflows
```

3. Update your definitions.py:
```python
from dagster_odp import build_definitions
defs = build_definitions("odp_config")
```

4. Start building pipelines in your workflows directory using YAML/JSON configuration.

Check out our [Quickstart Guide](https://runodp.github.io/dagster-odp/getting-started/quickstart/) for a complete walkthrough.

## Who Should Use dagster-odp?

- **Data Teams** seeking to standardize pipeline creation
- **Data Analysts/Scientists** who want to create pipelines without extensive coding
- **Data Engineers** looking to reduce boilerplate code and maintenance overhead
- **Organizations** adopting Dagster who want to accelerate development

## Documentation

[Comprehensive documentation](https://runodp.github.io/dagster-odp/) is available, including:

- [Tutorials](https://runodp.github.io/dagster-odp/tutorials/tutorials/)
- [Concepts Guide](https://runodp.github.io/dagster-odp/concepts/concepts/)
- [Integration Guides](https://runodp.github.io/dagster-odp/integrations/integrations/)
- [Reference Documentation](https://runodp.github.io/dagster-odp/reference/reference/)

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) for details on how to submit pull requests, report issues, and contribute to the project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
