# Reference Overview

This section provides detailed technical reference documentation for dagster-odp's configuration system. The reference covers all available configuration options and their usage.

## Configuration Files

### Dagster Config Reference

The `dagster_config` file is the central configuration file for resources in your ODP project. It contains:

- Resource definitions and parameters
- Integration configurations (DLT, DBT, Soda)
- External service connections
- Database configurations

[Learn more about Dagster Config →](dagster_config.md)

### Workflow Configuration Reference

Workflow files define your data pipelines through configuration. They specify:

- Assets and their configurations
- Jobs and triggers
- Partitioning settings
- Data quality checks
- Variable substitution

[Learn more about Workflow Config →](workflow_config.md)
