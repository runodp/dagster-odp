# Automation

Dagster ODP provides a configuration-driven way to automate your data pipelines through jobs, schedules, and sensors. This page explains how to use these automation features effectively.

## Jobs

Jobs in ODP are defined through configuration and specify which assets to materialize. They can be triggered manually, scheduled, or run in response to events through sensors.

### Defining Jobs

Jobs are defined in workflow files using YAML/JSON. Here's a basic example:

```yaml
jobs:
  - job_id: daily_weather_analysis
    description: "Process weather data daily"
    asset_selection:
      - raw_weather_data*   # Selects asset and all downstream assets
```

Key components:

- `job_id`: Unique identifier for the job
- `description`: Optional human-readable description
- `asset_selection`: Which assets to materialize (supports [Dagster's asset selection syntax](https://docs.dagster.io/concepts/assets/asset-selection-syntax))
- `triggers`: Optional list of schedules or sensors that trigger the job

## Schedules

ODP supports two types of schedules:

1. **Cron-based**: Run jobs at fixed intervals using cron expressions
2. **Partition-based**: Run jobs based on asset partition definitions

### Cron Schedules

Use cron schedules for time-based execution:

```yaml
jobs:
  - job_id: hourly_weather_check
    triggers:
      - trigger_id: hourly_schedule
        trigger_type: schedule
        description: "Run every hour"
        params:
          schedule_kind: cron
          schedule_params:
            cron_schedule: "@hourly"  # Run once per hour
```

The `cron_schedule` parameter supports:

- Standard cron expressions (e.g., `0 * * * *`)
- Predefined shortcuts (`@hourly`, `@daily`, `@weekly`, `@monthly`)

### Partition Schedules

For jobs that work with partitioned assets, instead of separately defining partitions and schedules, you can use partition-based schedules that automatically align with your asset partitions:

```yaml
partitions:
  - assets: ["monthly_data"]
    params:
      start: "2022-01-01"
      schedule_type: MONTHLY

jobs:
  - job_id: monthly_processing
    triggers:
      - trigger_id: monthly_schedule
        trigger_type: schedule
        params:
          schedule_kind: partition  # Automatically uses partition configuration
    asset_selection:
      - monthly_data*
```

This approach:

- Automatically determines run timing based on partition configuration
- Maintains consistency between partition definitions and scheduling
- Simplifies configuration by removing the need for separate cron expressions

Refer to the [Configuration](configuration.md) page to learn how to configure partitions.

## Sensors

Sensors allow you to trigger jobs in response to external events. ODP provides both pre-built sensors and the ability to create custom sensors.

### Pre-built Sensors

#### GCS Sensor
Monitors Google Cloud Storage buckets for new objects and triggers runs for each new object found:

```yaml
jobs:
  - job_id: process_new_files
    triggers:
      - trigger_id: gcs_file_monitor
        trigger_type: sensor
        description: "Monitor GCS bucket for new files"
        params:
          sensor_kind: gcs_sensor
          sensor_params:
            bucket_name: my-bucket
            path_prefix_filter: data/  # Optional: only watch specific prefix
```

**Required Resources**: The GCS sensor requires the `gcs` resource to be configured. For more information about configuring resources, see the [Resources](resources.md) documentation.

The sensor:

- Tracks processed files using a cursor to avoid duplicate processing
- Can filter objects based on prefix
- Provides file URIs to downstream assets through sensor context
- Automatically deduplicates runs using the object key as the run key

!!!note
    The GCS sensor automatically manages its cursor to track processed files. You don't need to handle cursor management when using this pre-built sensor.

### Creating Custom Sensors

Custom sensors inherit from `BaseSensor` and use the `@odp_sensor` decorator. `BaseSensor` inherits from Pydantic's `BaseModel`, helping ODP validate all sensor parameters using Pydantic's type system.

Each sensor has access to:

- `self._context`: The `SensorEvaluationContext` from Dagster
- `self._cursor`: The current cursor value, used to track sensor state
- `self._resources`: Dictionary of configured required resources

```python
from datetime import datetime
from typing import Any, Generator
from dagster import RunRequest, SkipReason

@odp_sensor(
    sensor_type="api_status_sensor",
    required_resources=["custom_api"]
)
class APIStatusSensor(BaseSensor):
    """Monitor an API endpoint for status changes."""
    
    endpoint: str
    check_interval: int = 300  # seconds
    
    def run(self) -> Generator[RunRequest | SkipReason, Any, None]:
        api = self._resources["custom_api"]
        previous_status = self._cursor
        
        status = api.get_status(self.endpoint)
        
        if status != previous_status:
            # Status changed, trigger a run
            self._context.update_cursor(status)
            
            # Values in sensor_context_config will be available to all tasks
            # in the triggered job through {{sensor.field}} syntax
            yield RunRequest(
                run_key=f"status_change_{status}",
                run_config={
                    "resources": {
                        "sensor_context": {
                            "config": {
                                "sensor_context_config": {
                                    "new_status": status,
                                    "previous_status": previous_status,
                                    "change_time": datetime.now().isoformat()
                                }
                            }
                        }
                    }
                }
            )
        else:
            yield SkipReason(f"Status unchanged: {status}")
                
```

Use the custom sensor in your workflow configuration:

```yaml
jobs:
  - job_id: status_handler
    triggers:
      - trigger_id: api_monitor
        trigger_type: sensor
        params:
          sensor_kind: api_status_sensor
          sensor_params:
            endpoint: /status
            check_interval: 600  # 10 minutes

assets:
  - asset_key: process_status_change
    task_type: status_processor
    params:
      # Any field defined in sensor_context_config above can be
      # accessed by any asset in the job using this syntax
      current_status: "{{sensor.new_status}}"
      previous_status: "{{sensor.previous_status}}"
      change_timestamp: "{{sensor.change_time}}"
```

!!!tip
    Values placed in `sensor_context_config` are automatically made available to all assets in the triggered job through the `{{sensor.field}}` syntax. This provides a convenient way to pass sensor-detected information to your assets without having to modify the job configuration.

!!!info "Important"
    After creating a custom sensor, you must import it in your `__init__.py` or `definitions.py` file for Python to execute the decorator and register the sensor with ODP. Simply defining the class in a separate file isn't enough - it needs to be imported where Dagster loads your code.

Through jobs, schedules, and sensors, ODP provides a configuration-driven way to automate your Dagster pipelines while keeping automation logic organized and maintainable in your workflow files.