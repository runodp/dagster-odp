# Overview of dagster-odp

dagster-odp (open data platform) extends Dagster's capabilities by providing a configuration-driven approach to building data pipelines. It's designed to streamline pipeline development and management for teams already using or familiar with Dagster.

## What is dagster-odp?

dagster-odp acts as an abstraction layer, translating user-defined configurations into Dagster primitives such as assets, resources, sensors, schedules, partitions, and asset checks. This approach offers several advantages:

1. **Configuration-driven pipeline creation**: Data analysts and scientists can create pipelines using pre-defined or custom tasks through YAML configuration, reducing the need for direct Python coding.
2. **Declarative pipeline definitions**: ODP separates pipeline logic from task implementation, facilitating easier maintenance and iteration of data workflows.
3. **Task reusability**: Tasks can be defined once and reused across multiple pipelines, promoting code efficiency and standardization.
4. **Integration with popular tools**: ODP provides built-in support for popular data tools, including DLT for data ingestion and Soda for data quality checks.

## Key Integrations

ODP provides enhanced integrations with popular data tools:

- **DLT**: Granular asset creation and automatic dependency management for data ingestion
- **DBT**: Configuration-driven transformation with automatic asset creation and variable management
- **Soda**: Data quality monitoring through configuration-based asset checks

## Building pipelines with ODP

ODP uses two main types of configuration files to define and manage pipelines:

### 1. Resource Configuration

The `dagster_config.yaml` file defines Dagster resources that interface with external services and tools.

Example resource configuration:
```yaml
resources:
  - resource_kind: bigquery
    params:
      project: my-project
      location: us-east1
```

### 2. Workflow Configuration

Files in the `workflows` directory define your pipeline components using assets, jobs, and automation. These files can be in YAML or JSON format and specify what your pipeline does and how it runs.

Example workflow configuration:
```yaml
assets:
  - asset_key: raw_data
    task_type: gcs_file_to_bq
    params:
      source_file_uri: gs://my-bucket/raw_data.csv
      destination_table_id: my_project.my_dataset.raw_data

jobs:
  - job_id: daily_etl
    asset_selection: [raw_data]
    triggers:
      - trigger_id: daily_run
        trigger_type: schedule
        params:
          schedule_kind: cron
          schedule_params:
            cron_schedule: "0 1 * * *"
```

This workflow configuration creates a simple ETL pipeline that:

- Loads data from GCS to BigQuery
- Runs automatically every day at 1 AM

All of this is achieved without writing any Python code. You can split workflow configurations across multiple files for better organization, and they can reference resources defined in your `dagster_config.yaml`.

### 2. Leverage Pre-built Components

ODP comes with a variety of pre-built components, with more on the way, to accelerate pipeline development:

**Pre-defined tasks**

- GCS to BigQuery data transfer
- BigQuery to GCS export
- DuckDB operations
- Shell command execution

**Integrated libraries**

- DLT (Data Load Tool) for data ingestion from various sources
- Soda for data quality checks

### 3. Create Custom Tasks

While ODP provides many pre-built components, it also allows data engineers to create custom tasks for specific needs. Here's how you would write a custom task to anonymize data:

```python
from dagster_odp.tasks.manager import BaseTask, odp_task
from google.cloud import bigquery

@odp_task("anonymize_pii", required_resources=["bigquery"])
class AnonymizePIITask(BaseTask):
    source_table: str
    destination_table: str
    columns_to_hash: list[str]

    def run(self) -> Dict:
        # Anonymization logic
        # Access the Bigquery resource using self._resources["bigquery"]
        pass
```

Use the custom task in your pipeline configuration:

```yaml
assets:
  - asset_key: anonymized_user_data
    task_type: anonymize_pii
    params:
      source_table: my_project.raw_data.users
      destination_table: my_project.processed_data.anonymized_users
      columns_to_hash: ["email", "phone_number", "social_security_number"]
```

### 4. Use Dynamic Configuration

ODP supports various configuration variables and features:

- Time-based partitioning for incremental processing
- Variable substitution using context, resource, and parent asset information
- Date formatting helpers for working with different date formats
- Sensor context passing for event-driven pipelines

## Why Use dagster-odp?

### 1. Empower Non-Engineers
By using a configuration-based approach, ODP allows data analysts and scientists to create and modify pipelines without deep engineering knowledge.

### 2. Standardize Pipeline Structure
ODP encourages a standardized approach to pipeline development within an organization. This leads to more consistent, maintainable, and understandable data workflows.

### 3. Accelerate Development
With pre-built components and integrations, ODP significantly reduces the time and effort required to create new data pipelines. Teams can focus on the "what" (pipeline business logic) rather than the "how" (task implementation details).

### 4. Improve Maintainability
The declarative nature of ODP configurations makes it easier to understand, modify, and version control pipelines. This improves long-term maintainability of data workflows.

### 5. Scale with Flexibility
As your data needs grow, ODP allows you to easily extend functionality through custom tasks while maintaining the simplicity of configuration-based pipeline creation for all team members.

## Next Steps

Now that you're familiar with dagster-odp's concepts and benefits, you're ready to start building your first pipeline. Check out our:

- [Quickstart Guide](./quickstart.md) to get started
- [Tutorials](../tutorials/tutorials.md) for more advanced real-world use cases
- [Concepts](../concepts/concepts.md) section for a deeper understanding of ODP's components
- [Integrations](../integrations/integrations.md) documentation to learn about tool-specific features
