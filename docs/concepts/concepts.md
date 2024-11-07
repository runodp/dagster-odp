# Concepts Overview

This section provides in-depth explanations of Dagster ODP's core concepts and components. Understanding these concepts will help you build data pipelines using ODP's configuration-driven approach.

## Core Components

### Tasks and Assets

The foundation of ODP pipelines:

- Tasks are reusable Python components that produce assets
- Assets are objects in persistent storage (tables, files, etc.)
- Configuration-driven asset creation using YAML/JSON
- Built-in tasks for common operations
- Extensible through custom tasks

[Learn more about Tasks and Assets →](tasks_and_assets.md)

### Resources

Shared components across your pipeline:

- Connect to external services (databases, APIs)
- Configure once, use everywhere
- Pre-built resources for common tools
- Custom resource creation
- Integration with Dagster's resource system

[Learn more about Resources →](resources.md)

### Automation

Control when and how your pipelines run:

- Jobs define what assets to materialize
- Schedules for time-based execution
- Sensors for event-driven pipelines
- Support for both cron and partition-based scheduling
- Custom sensor creation

[Learn more about Automation →](automation.md)

### Configuration

ODP's configuration system ties everything together:

- Define pipelines using YAML/JSON files
- Centralized resource configuration
- Dynamic variable substitution
- Time-based partitioning
- Comprehensive validation

[Learn more about Configuration →](configuration.md)

### Generating Definitions

Bridge between configuration and Dagster:

- Generate Dagster definitions from ODP configuration
- Combine ODP with existing Dagster code

[Learn more about Generating Definitions →](definitions.md)

## Deep Dives

Each concept page provides an in-depth exploration of its topic, including detailed examples, best practices, and technical details. For integration with external tools like DLT, DBT, and Soda, see the [Integrations](../integrations/integrations.md) section.

These concepts work together to enable ODP's configuration-driven approach, allowing you to build maintainable, scalable data pipelines while leveraging the full power of Dagster's asset-based paradigm.