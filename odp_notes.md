# ODP

## What is ODP
* ODP allows individuals on data teams to build data pipelines using JSON/YAML config instead of code.
* It is a python library built on top of the dagster orchestrator.
* The library translates user defined config to dagster primitives such as assets, resources, sensors, schedules, partitions and asset checks.
* Supports ELT pipelines through Dagster's DBT integration. ETL pipelines are not supported yet (there isn't a pre-built spark integration yet)
## Advantages
### Pipelines without code
* Use the predefined tasks or custom tasks to build no-code pipelines
* Allows data scientists or data analysts to build pipelines using tasks defined by data engineers
### Declarative pipelines
* Decouple task logic from DAG/pipeline logic
* Iterate on task logic without touching pipelines that use those tasks
* Separate CI/CD and review processes for tasks and pipelines
### Task re-usability
* Define tasks once and re-use them across pipelines
## Why use Dagster ODP
### Pre-defined task libraries
* Common GCP tasks
* Common DuckDB tasks
* Run shell commands
### Pre-built integrations
* DLT integration allows users to ingest data from pre-defined sources without code
* Define Data Quality checks in yaml and incorporate them in dagster asset checks without code
### QOL features
* Custom DLT integration automatically creates source assets and creates a separate asset per persistant storage object, which the official DLT integration doesn't
* Variables in config exposes the current run id, partition key, date etc
* Automatic materialization metadata passing to child assets which can be referred to in the config as variables.
### Validation
* Pydantic validation for all config files
* Auto validation of parameters defined in custom tasks
## Dagster construct support
#### Supported
1. Assets
2. Asset materialization metadata
3. Resources
4. Sensors
5. Schedules
6. Jobs
7. DBT integration (with partition support)
8. Only time partitions (Other types of partitions are not supported)
9. Asset checks through Soda
#### Not supported
1. Static, dynamic and multi-dimensioned partitions
2. Config schema
3. Automation conditions (although imo it's not only super easy to define jobs through the config, it's also more descriptive and clear - which usually means less bugs)
## Concepts
### Tasks
A task is an ODP component that represents an action and produces dagster assets. A dagster asset represents an object in persistent storage, like a table in a db or a file in object storage. For instance, a task called s3_file_to_gcs could copy s3 objects to GCS and each instance of the task would create a dagster asset that represents the GCS file.

Tasks take params as input which defines the dagster asset created. In the example above, the s3_file_to_gcs could have two params, a source s3 path and a destination path, and each set of inputs would create a different dagster asset. Hence, when workflows or pipelines are defined in the config, a task can be reused with a different asset_key that uniquely identifies the asset.

ODP allows users to create their own custom tasks.
### Assets
Assets in ODP are equivalent to dagster assets. The difference between dagster‚Äôs and ODP‚Äôs asset definition process is that in dagster, you create an asset directly in code, whereas in odp you define a task that creates assets dynamically. Making users create tasks instead of assets is how ODP makes all tasks reusable by default across pipelines, one of the core advantages of odp vs. other orchestration frameworks.
### Resources
Resources in dagster represent code that helps connect or integrate with an external resource, like API call logic that makes the call, a database connection, or a cloud client.

Resources in ODP create those dagster resources and can take params from the yaml config. Unlike tasks, ODP only allows a resource to be defined once per code location. For instance, a dbt resource could take the dbt directory as input, and only one dbt resource definition would be allowed. Similarly, a postgres resource could set up the postgres connection, which means the particular dagster code location can only have one postgres connection.

In dagster, resources necessary for an asset are dependency injected into asset definitions, so multiple asset definitions can reuse resource logic. ODP implements a similar approach, but with task definitions instead of asset definitions.

ODP lets users create their own custom resources.
### Sensors
Just like odp resources, odp sensors create dagster sensors, which allow you to instigate runs based on some external state change. An odp sensor can be defined once and used across workflows in job definitions. ODP lets users create their own custom sensors.
### Schedules
Schedules in ODP are part of a job definition in the config and trigger time-based jobs. Since schedules take a cron as a param, there isn‚Äôt really a requirement for users to create their own schedule definitions
### Jobs
Jobs defined in the ODP config are translated to dagster jobs and run the defined asset selection.
### Partitions
Partitions in ODP take a list of assets and assign dagster partitions to them. ODP only supports time based partitions at the moment.
### Soda checks
Soda checks in ODP creates a soda scan as a dagster asset check for a particular asset.
### Odp config directory
This directory contains the config files that are read by the odp library. ODP supports both json and yaml files, and both can be used interchangeably. The directory has a `dagster_config` file, that contains the config for the resources, and a `workflows` directory, which has all of the workflow files. The names `dagster_config` and `workflows` need to be as mentioned for odp to recognize them and cannot be modified.
### dagster_config file
This is a json or yaml file that needs to be created inside the odp config directory. This file contains the config for the resources. If the tasks defined in workflows require certain resources, those resources and their params need to be defined in this file. ODP requires dagster_config to be a single file (either json or yaml)
### workflow directory

The workflow files should be added to this directory. ODP allows for multiple workflow files, each of which need not be self contained. This means, for example, that a job defined in one file could have an asset selection of assets defined in another file. A mix of json and yaml files is also allowed. This allows users to logically group their jobs, assets, partitions and asset checks as necessary.
# Reference

## DLT

The ODP DLT integration is a custom integration and is not related to the Dagster embedded elt integration. It supports the following:
* Automatic creation of source assets with the proper group name, with the name of the source inferred from the asset key
* Creation of assets that correspond to the objects created, so child assets that depend on a single object can be created. Dagster embedded elt only creates a single dagster asset despite dlt creating multiple objects, which makes it hard to create child assets that depend on one particular created dlt object.

DLT has the concept of resources that **differs from dagster resources**. A DLT resource is a logical split of a DLT source, and each source contains multiple resources. For instance, for a DLT API source, each resource refers to a different endpoint. If the source is a database, each resource refers to a different table in that database.

As mentioned above, the ODP DLT integration automatically creates source assets, and the DLT assets are downstream from this source asset. This source asset name as well as the resource are inferred from the `asset_key` of the DLT tasks in the workflow yaml, so it's important to name the asset properly. This is how the name inference works:

Let's say the `asset_key` is called `chess_com/players_games`. The string after the last `/`, `players_games` in this case, is the DLT resource name. This means that each DLT asset defined in the config corresponds to one DLT resource. If multiple resources from a DLT source need to be ingested, a DLT asset needs to be created in the config for each DLT resource.

The string before the last `/` is the name of the source assets. If the `asset_key` has more than two components, all of the components except the last component become the source asset name, and the last component is inferred as the resource name.

The dagster DLT asset names have the name of the source asset with the name of each DLT object concatenated to it. Note that each DLT resource can generate multiple objects. 

Here is an example that puts it all together:
We have an `asset_key` for a dlt task called `abc/github_reactions_to_gcs/pull_requests`.

This dlt task creates the following objects (could be tables if the destination is a database):
1. `pull_requests`
2. `pull_requests__comments`
3. `pull_requests__reactions`
4. `pull_requests__comments__reactions`

This is DLT's way of dealing with nested data in the API responses.

Now, here are the constructs that dagster ODP creates from the asset key:
1. **Source asset**: `abc/github_reactions_to_gcs`
2. **DLT resource**: `pull_requests`. This will cause DLT to only call this particular endpoint.
3. **Dagster assets**:
   1. `abc/github_reactions_to_gcs/pull_requests`
   2. `abc/github_reactions_to_gcs/pull_requests__comments`
   3. `abc/github_reactions_to_gcs/pull_requests__reactions`
   4. `abc/github_reactions_to_gcs/pull_requests__comments__reactions`

With this approach, a child asset that depends on say, `abc/github_reactions_to_gcs/pull_requests__reactions` can be created in dagster.

### Materialization

* When the destination is `bigquery` ,  `destination_table_id`  is added to the metadata in the format `dataset_name.table_name`
* When the destination is `duckdb`, `destination_table_id`  is added to the metadata in the format `schema_name.table_name`
* When the destination is `filesystem`, the `destination_file_uri`  is added to the metadata.

These metadata fields be used by child assets.

### Secrets

The `secrets.toml` file in the `.dlt` directory are flattened and copied to environment variables so the DLT integration can use them. Note that ODP does not copy over the parameters in the `config.toml` file. Instead, those params can be passed into ODP via the workflow config.

### Resource reference

```yaml "dagster_config.yaml"
resources:
  - resource_kind: dlt
    params:
      project_dir: chess_dlt
```

The dlt resource accepts one parameter, the `project_dir` with DLT code relative to the directory where dagster is being run.

### Workflow task reference

``` yaml "Example task"
assets:
  - asset_key: chess_com/chess_data/players_games
    task_type: dlt
    description: chess.com players games
    group_name: monthly_player_games
    params:
      source_module: chess.source
      schema_file_path: schemas/export/chess.schema.yaml
      source_params:
        players: ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"]
        start_month: "{{#date}}{{context.partition_key}}|%Y/%m{{/date}}"
        end_month: "{{#date}}{{context.partition_key}}|%Y/%m{{/date}}"
      destination: filesystem
      destination_params:
        bucket_url: data/
      pipeline_params:
        dataset_name: chess_games
      run_params:
        write_disposition: replace
```

* **source_module**: When the DLT project is initialized with `dlt init` , a file containing the actual code interacting with the source is created. The file consists of a `source` method and `resource` methods. The methods are usually located in the `source_name/__init__.py` file, but this could differ based on the source type. The `source_module` parameter is the python module path to the `source` method from the DLT directory mentioned in the `dlt_path` resource parameter. In this example the folder structure is `chess_dlt/chess/__init__.py`, hence the python path is `chess.source`

* **schema_file_path**: The path to the DLT generated schema file. ODP creates the dagster assets based on the objects in this file. To generate the schema file, add the `export_schema_path` parameter to the pipeline method in the pipeline file generated by `dlt init`, and run the dlt pipeline directly.

* **source_params**: These are the parameters for the DLT source method that are directly passed in.

* **destination**: One of the DLT destinations.

* **destination_params**:  These are the parameters for the DLT destination method that are directly passed in.

* **pipeline_params**: These params are directly passed into DLT's pipeline method.

* **run_params**: These params are directly passed into DLT's run method.

  

## Workflow config

### Variables

ODP supports certain variables that can be used anywhere inside params with the mustache syntax/double curly braces, which will be replaced by the actual value during runtime.

#### Context

* **context.partition_key**: The partition key of the current run of the asset.
* **context.partition_window_start**: The time object at the start of the partition window of the asset.
* **context.partition_window_end**: The time object at the end of the partition window of the asset.
* **context.partition_key**: The partition key of the current run of the asset.
* **context.run_id**: the run id of the current run.

#### Resource

All of the resource parameters defined in `dagster_config.yaml` are accessible. This can be useful if some value from a resource definition needs to be passed into an asset param. For the example resource config below:

```yaml
resources:
  - resource_kind: dlt
    params:
      project_dir: chess_dlt
```

 `{{resource.dlt.project_dir}}` in the workflow config will be replaced by `chess_dlt` in runtime.

#### Sensor

The sensor context passed out from a dagster sensor can also be accessed through variables. This is useful if you want to, say, pass the name of a file detected by a sensor to an asset param. For example, for the following sensor run config:

```python
run_config = {
    "resources": {
        "sensor_context": {
            "config": {
                "sensor_context_config": {
                    "file_uri": f"gs://{self.bucket_name}/{gcs_key}",
                }
            }
        }
    }
}
```

the `file_uri` value can be accessed through `sensor.file_uri`. Note that the same key naming convention must be followed (`resources.sensor_context.config.sensor_context_config`)

#### Parent

The materialization metadata of a parent can be accessed through variables in a child asset. This is useful as a communication mechanism between assets in the config. Here is an example:
```yaml
assets:
  - asset_key: weather_daily_gcs
    task_type: api_to_gcs
    description: Ingest weather data into GCS
  - asset_key: weather_bq_table
  	task_type: gcs_file_to_bq
    params:
      source_file_uri: {{weather_daily_gcs.destination_file_uri}}
      destination_table_id: weather.weather_data
```

Given that the `weather_daily_gcs` asset has a key called`destination_file_uri` in it's materialization metadata, `weather_bq_table` can access the value of that key, making it easy to pass dynamic metadata between assets.

Note: if the parent asset has prefixes, for instance, if the parent asset is named `weather_data/weather_daily_gcs`, when referring to this asset in variables, replace the `/` with `_`. So in the above example, the variable would be `{{weather_data_weather_daily_gcs.destination_file_uri}}`

#### Date transformation

Variables that contain DateTime values can be formatted to a different datetime format. This is useful if a parameter requires the date in a specific format. The syntax for a date transformation is:

`{{#date}}{{context.partition_key}}|%Y/%m{{/date}}`

where the date string is before the `|`, the expected format is after the `|`, and they are wrapped in `{{#date}}{{/date}}`

### Partitions

ODP supports the definition of partitioned dagster assets in the config. An example of a partition definition is:

```yaml
partitions:
  - assets: ["asset_1"]
    params:
      start: "2022-01-01"
      schedule_type: MONTHLY
```

Where the `assets` are the list of assets with that partition, and the `params` are dagster's [TimeWindowPartitionsDefinition](https://docs.dagster.io/_apidocs/partitions#dagster.TimeWindowPartitionsDefinition) parameters.

Each asset can only have a single partition. The params are shared among all the assets in the list, removing the need to repeat the params for each asset. There can be multiple asset lists with different param definitions, as long as the assets are distinct in each list.

### Overall syntax

```yaml
assets:
	- asset_key: asset_1
		task_type: task_1
		params:
			param_1: value_1
			param_2: value_2
	- asset_key: asset_2
		task_type: task_2
		params:
			param_1: value_1
			param_2: value_2
			
partitions:
  - assets: ["asset_1"]
  	params:
			param_1: value_1
			
soda_checks:
	- asset_key: asset_2
		check_file_path: check.yml
		data_source: source_1
```

There are three top level items that a workflow file can contain: `assets`, `partitions` and `soda_checks`

## Asset checks with Soda

ODP supports asset checks with a Soda integration. The checks are defined in a soda check yml file, which is run for an asset check. Here is how you can setup an asset check with Soda and ODP:

### Resource definition

First, add the soda resource to `dagster_config.yaml`

```yaml "dagster_config.yaml"
resources:
  - resource_kind: soda
    params:
      project_dir: odp_config/soda
      checks_dir: scans
```

The `project_dir` contains Soda's config file: `configuration.yml` and the `checks_dir`

The `checks_dir` is the directory name that contains all the soda check yml files. This directory would exist within the `project_dir`. This parameter is optional, if not defined, ODP would assume the check files exist within the `project_dir`

All of the checks in a check file would be run as a single asset check. This is because Soda does not expose a method to run a single soda check independently, you can only run one or multiple check files.

### Workflow definition

An example of a Soda asset check definition in the workflow config is:

```yaml
soda_checks:
  - asset_key: bq_country_code_data
    check_file_path: country_codes.yml
    blocking: true
    description: 'Validate country codes'
    data_source: bq_global_weather
```

* `asset_key` is the asset for which the test is to be run.
* `check_file_path` is the path to the check file inside the `checks_dir` defined in the dagster config. In this example, the check file is located directly within the `checks_dir`
* `blocking`: if blocking is true, downstream assets are not executed if a check fails.
* `data_source` is the Soda data source that has the required connection parameters and is defined in `configuration.yml`

`blocking` and `description` are optional.

### Outcomes

Soda allows users to define two seperate outcomes for a test: `WARN` and `FAIL`. If one or more than one checks result in the `FAIL` outcome, the asset check has a dagster severity of `ERROR`.  If there are not any `fail` results, but one or more checks have a `WARN` outcome, the asset check has a dagster severity of `WARN`
