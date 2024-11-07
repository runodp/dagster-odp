# Generating Definitions

Dagster ODP simplifies the process of creating Dagster definitions from configuration files through its `build_definitions` function. This page explains how to generate and combine ODP definitions with your existing Dagster code.

## Building ODP Definitions

### Basic Usage

The simplest way to use ODP is to build definitions directly from your configuration:

```python
from dagster_odp import build_definitions

defs = build_definitions("odp_config")
```

This will:

1. Load configuration from the `odp_config` directory
2. Create Dagster definitions for all configured components:
    - Assets from task configurations
    - Resources from resource configurations
    - Jobs from job configurations
    - Schedules and sensors from trigger configurations
    - Asset checks from Soda check configurations

### Configuration Path Options

You can specify the configuration path in two ways:

1. **As a parameter**:
```python
defs = build_definitions("/path/to/odp_config")
```

2. **Through environment variable**:
```python
# Set environment variable
export ODP_CONFIG_PATH=/path/to/odp_config

# No path parameter needed
defs = build_definitions()
```

If no path is provided and the environment variable isn't set, an empty Dagster Definitions object will be returned.

## Combining with Existing Definitions

Often, you'll want to use ODP alongside existing Dagster code. Dagster's `Definitions.merge()` method allows you to combine ODP definitions with your existing definitions:

```python
from dagster import Definitions
from dagster_odp import build_definitions

# Build ODP definitions
odp_defs = build_definitions("odp_config")

# Your existing Dagster definitions
dagster_defs = Definitions(
    assets=[existing_asset_1, existing_asset_2],
    resources={
        "custom_resource": CustomResource()
    },
    jobs=[existing_job],
    sensors=[existing_sensor]
)

# Merge definitions
defs = Definitions.merge(odp_defs, dagster_defs)
```

### Merge Behavior

When merging definitions, all components (assets, resources, jobs, sensors, and schedules) from both definition objects are combined into a single Definitions object.

!!!warning "Name Conflicts"
    The merge will fail if there are any naming conflicts between the two definition objects. This includes:

    - Assets with the same key
    - Resources with the same name
    - Jobs with the same ID
    - Sensors with the same name
    - Schedules with the same name
    
    Ensure all components have unique names across both ODP configuration and your Dagster code.

!!!tip "Best Practice"
    Consider organizing your project with clear separation between ODP and Dagster components:
    ```
    my_project/
    ├── odp_config/          # ODP configuration
    │   ├── dagster_config.yaml
    │   └── workflows/
    ├── odp_components/      # Custom ODP tasks, sensors, resources
    │   ├── tasks/
    │   ├── jobs/
    │   ├── resources/
    └── dagster_components/  # Direct Dagster code
        ├── assets/         # Software-defined assets
        ├── jobs/          
        └── resources/     
    ```
    This structure makes it clear which components are configuration-driven (ODP) versus code-driven (direct Dagster), making maintenance and debugging easier.