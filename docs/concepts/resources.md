# Resources

Resources in Dagster ODP provide a configuration-driven way to share code across tasks and sensors. While Dagster has its own resources system, ODP enhances it with simplified configuration management.

## Understanding Resources 

### What is a Resource?

A resource in Dagster represents code that needs to be shared across different components in your data pipeline like assets and sensors. Common use cases include:

- External service connections (APIs, databases)
- Shared business logic
- Configuration management
- Utility functions and helpers

Resources in ODP are:

1. **Singleton per Code Location**: Only one configuration of each resource type is allowed per code location
2. **Configurable via YAML/JSON**: Defined in the `dagster_config` file
3. **Dependency Injected**: Automatically provided to tasks and sensors that require them
4. **Validated**: Use Pydantic for configuration validation

## Configuring Resources

Resources are defined in your `dagster_config.yaml` (or `.json`) file. Here's a basic example:

```yaml
resources:
  - resource_kind: duckdb
    params:
      database_path: data/analysis.db

  - resource_kind: bigquery
    params:
      project: my-gcp-project
      location: us-east1

  - resource_kind: dbt
    params:
      project_dir: dbt_project     
      profile: my_profile         
      target: dev               
      load_all_models: true    # ODP-specific parameter
```

### Resource Configuration Rules

1. Each resource type can only be defined once
2. The resource must be defined if any task requires it
3. Parameters are validated against the resource's Pydantic model
4. Resource configurations can be referenced in asset parameters using `{{resource.resource_kind.param_name}}`

## Pre-built Resources

ODP provides several pre-built resources and integrates with Dagster's resource system:

### Google Cloud Resources

ODP registers Dagster's Google Cloud resources so they can be used in the resource configuration:

#### Google Cloud Storage
```yaml
resource_kind: gcs
params:
  project: my-project      # Passed to Dagster's GCSResource
```

#### BigQuery
```yaml
resource_kind: bigquery
params:
  project: my-project      # Passed to Dagster's BigQueryResource
  location: us-east1      
```

### DuckDB Resource

Manages connections to DuckDB databases.

```yaml
resource_kind: duckdb
params:
  database_path: path/to/database.db  # Required
```

The DuckDB resource provides:

- Connection management
- GCS filesystem integration
- Automatic connection cleanup

### Integration Resources

#### DLT Resource
Manages DLT pipeline execution and metadata tracking. This is ODP's custom DLT integration, distinct from Dagster's embedded DLT integration, offering several advantages:

```yaml
resource_kind: dlt
params:
  project_dir: dlt_project    # Required: Path to DLT project
```

Key features:
- Automatic creation of source assets with proper group names
- Creates separate assets for each DLT object, enabling granular dependencies
- Handles DLT secrets management via environment variables
- Standardized materialization metadata:

#### DBT Resource

Enhances Dagster's DBT integration with additional features. [Learn more →](integrations/dbt.md)

```yaml
resource_kind: dbt
params:
  project_dir: dbt_project    # Required: Path to DBT project
  profile: my_profile         # Required: Profile name from profiles.yml
  target: dev                 # Required: Target name from profiles.yml
  load_all_models: true       # Optional: Auto-import all models (default: true)
```
ODP-specific enhancements:

- Automatic dagster DBT resource definition
- External source asset creation
- Materialization metadata for downstream assets
- Variable management
- Partition support

#### Soda Resource
Configures Soda for data quality checks. [Learn more →](integrations/soda.md)

```yaml
resource_kind: soda
params:
  project_dir: soda_project     # Required: Path to Soda project
  checks_dir: scans             # Optional: Directory containing check files
```
Features:

- Integrated with Dagster asset checks
- Result severity mapping
- Blocking check support

## Creating Custom Resources

Custom resources can be created by sub-classing `ConfigurableResource`. Here's an example:

```python
from dagster import ConfigurableResource, EnvVar
from typing import Optional
from pydantic import Field

@odp_resource("custom_api")
class CustomAPIResource(ConfigurableResource):
    """A resource for connecting to a custom API."""
    
    base_url: str
    timeout: Optional[int] = Field(default=30, gt=0)
    
    def make_request(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make an API request."""
        # Read API key from environment variable
        api_key = os.environ.get("CUSTOM_API_KEY")
        if not api_key:
            raise ValueError("CUSTOM_API_KEY environment variable not set")
            
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(
            f"{self.base_url}/{endpoint}", 
            headers=headers,
            timeout=self.timeout,
            **kwargs
        )
        response.raise_for_status()
        return response.json()
```

Use the custom resource in your configuration:

```yaml
resources:
  - resource_kind: custom_api
    params:
      base_url: https://api.example.com/v1
      timeout: 60
```

When ODP processes this configuration:

- Parameters are validated against the class attributes' types
- Required fields (those without defaults) must be provided
- Optional fields use their default values if not specified

### Using Resources in Tasks

Resources are automatically injected into tasks that require them. Here's how to use resources in a task:

```python
@odp_task(
    "custom_api_task",
    required_resources=["custom_api"]
)
class CustomAPITask(BaseTask):
    endpoint: str
    
    def run(self) -> Dict[str, Any]:
        # Access the resource through self._resources
        api = self._resources["custom_api"]
        
        # Use the resource
        result = api.make_request(self.endpoint)
        
        return {"data": result}
```

## Best Practices

- Move common code used across tasks or sensors into resources
- Create resources for shared business logic that might be used in multiple pipelines
- Use resources to standardize how your pipeline interacts with external systems
- Keep resources focused on a single domain or service
- Keep resource configurations in version control

Resources are a fundamental part of Dagster ODP's configuration-driven approach. By properly configuring and using resources, you can create maintainable and secure data pipelines that effectively interact with external systems.
