
1. Download and run dlt init
2. Add `export_schema_path="schemas/export"`, comment out `# load_players_online_status()` and run the pipeline
  * This creates the schema yaml file which is used by the ODP DLT integration to create the required assets
  * The ODP DLT integration is a custom integration and is not related to the Dagster embedded elt integration. It supports the following:
    * Automatic creation of source assets with the proper group name, with the name of the source inferred from the asset key
    TODO: check if the name can be inferred from the DLT resource
    * Creation of assets that correspond to the tables created, so child assets that depend on a single table can be created
  TODO: If the initial manual run of the dlt pipeline has additional dlt resources that are not used in the dagster pipeline, are those assets still created in dagster? If so, filter out the table names in the dlt output schema using the resource name.