# Concepts Overview

This section provides in-depth explanations of Dagster ODP's core concepts and components. Understanding these concepts will help you build pipelines using ODP's configuration-driven approach. To understand how ODP works, you'll need to know about:

### Core Components

The fundamental elements of ODP pipelines:

- **Tasks and Assets**: Tasks are reusable operations that produce assets (like database tables or files). [Learn more ->](tasks_and_assets.md)
- **Resources**: Connections to external services and tools. [Learn more ->](resources.md)
- **Automation**: Sensors, schedules, and jobs that control pipeline execution. [Learn more ->](automation.md)

### Configuration

The configuration system that ties everything together:

- **Workflow Files**: Define your pipelines and their components
- **Resource Configuration**: Set up dagster resources
- **Variables**: Dynamic value substitution in your configurations
- **Validation**: Validate your config parameters with Pydantic
- **Partitions**: Create time-based dagster partitions

[Learn more about Configuration →](configuration.md)
