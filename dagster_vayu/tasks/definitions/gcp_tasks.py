from typing import Any, Dict

from google.cloud import bigquery as bq

from ..manager import BaseTask, vayu_task
from ..utils import replace_bq_job_params


@vayu_task(
    task_type="gcs_file_to_bq",
    required_resources=["bigquery"],
    compute_kind="bigquery",
    storage_kind="bigquery",
)
class GCSFileToBQ(BaseTask):
    """
    A task that loads data from a Google Cloud Storage (GCS) file to a BigQuery table.

    Attributes:
        source_file_uri (str): The URI of the source file in GCS.
        destination_table_id (str): The ID of the destination BigQuery table.
        job_config_params (Dict[str, Any]): Additional parameters for the load job.
    """

    source_file_uri: str
    destination_table_id: str
    job_config_params: Dict[str, Any] = {}

    def run(self) -> Dict:
        """
        Loads data from a GCS file to a BigQuery table.

        Returns:
            Dict: Metadata about the loaded data.

        Raises:
            ValueError: If the required 'bigquery' resource is not provided.
        """

        bq_client = self._resources["bigquery"]

        with bq_client as client:
            job_config = bq.LoadJobConfig(
                **replace_bq_job_params(self.job_config_params)
            )

            load_job = client.load_table_from_uri(
                self.source_file_uri, self.destination_table_id, job_config=job_config
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.

            destination_table = client.get_table(
                self.destination_table_id
            )  # Make an API request.
            print(f"Loaded {destination_table.num_rows} rows.")
            metadata = {
                "source_file_uri": self.source_file_uri,
                "destination_table_id": self.destination_table_id,
                "row_count": destination_table.num_rows,
            }
        return metadata


@vayu_task(
    task_type="bq_table_to_gcs",
    required_resources=["bigquery"],
    compute_kind="bigquery",
    storage_kind="googlecloud",
)
class BQTableToGCS(BaseTask):
    """
    A task that exports data from a BigQuery table to a Google Cloud Storage (GCS) file.

    Attributes:
        source_table_id (str): The ID of the source BigQuery table.
        destination_file_uri (str): The URI of the destination file in GCS.
        job_config_params (Dict[str, Any]): Additional parameters for the extract job.
    """

    source_table_id: str
    destination_file_uri: str
    job_config_params: Dict[str, Any] = {}

    def run(self) -> Dict:
        """
        Exports a BigQuery table to Google Cloud Storage (GCS).

        Returns:
            Dict: Metadata about the exported data.

        Raises:
            ValueError: If the required 'bigquery' resource is not provided.
        """

        bq_client = self._resources["bigquery"]

        with bq_client as client:

            job_config = bq.ExtractJobConfig(**self.job_config_params)

            extract_job = client.extract_table(
                self.source_table_id,
                self.destination_file_uri,
                job_config=job_config,
            )

            extract_job.result()

            print("Table exported to GCS successfully.")
            source_table = client.get_table(self.source_table_id)
            metadata = {
                "source_table_id": self.source_table_id,
                "destination_file_uri": extract_job.destination_uris[0].rsplit("/", 1)[
                    0
                ],
                "row_count": source_table.num_rows,  # type: ignore
            }

        return metadata
