# Integrations

Dagster ODP provides enhanced, configuration-driven integrations with popular data tools that complement Dagster's capabilities. These integrations simplify complex pipeline development through standardized configuration while preserving the power and flexibility of the underlying tools.

## Core Integrations

### DLT (Data Load Tool)

**Purpose**: Data ingestion from various sources 


**Key Features**:

- Granular asset creation (one asset per data object)
- Automatic source asset generation
- Configuration-driven pipeline creation
- Automated secrets management

[Learn more about DLT integration →](dlt.md)

### DBT (Data Build Tool)

**Purpose**: Data transformation and modeling  


**Key Features**:

- Configuration-driven Dagster DLT resource creation
- Variable management through configuration
- External source asset creation
- Enhanced materialization metadata

[Learn more about DBT integration →](dbt.md)

### Soda Core

**Purpose**: Data quality monitoring  


**Key Features**:

- Configuration-driven Dagster asset checks
- Severity level mapping
- Pipeline control through blocking checks

[Learn more about Soda integration →](soda.md)

By leveraging these integrations effectively, you can build robust data pipelines that combine the best aspects of each tool while maintaining simplicity through configuration-driven development.
