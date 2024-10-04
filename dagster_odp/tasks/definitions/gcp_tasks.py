import os
import pathlib
from typing import Any, Dict

from google.cloud import bigquery as bq
from google.cloud.storage import Client as GCSClient

from ..manager import BaseTask, odp_task
from ..utils import replace_bq_job_params


@odp_task(
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


@odp_task(
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


@odp_task(
    task_type="gcs_file_download",
    required_resources=["gcs"],
    compute_kind="googlecloud",
    storage_kind="filesystem",
)
class GCSFileDownload(BaseTask):
    """
    A task that downloads files from a Google Cloud Storage path to a local filesystem.

    Attributes:
        source_file_uri (str): The URI of the source path in GCS
            (e.g., 'gs://bucket-name/path/to/files/').
        destination_file_path (str): The local directory where files should be saved.
    """

    source_file_uri: str
    destination_file_path: str

    def run(self) -> Dict:
        """
        Downloads files from GCS to a local filesystem using the batch context manager.

        Returns:
            Dict: Metadata about the downloaded files.

        Raises:
            ValueError: If the GCS URI is invalid or if
                the destination path is not a valid directory path.
        """
        self._validate_paths()

        gcs_client: GCSClient = self._resources["gcs"]
        bucket_name, prefix = self._parse_gcs_uri()
        bucket = gcs_client.bucket(bucket_name)
        blobs_to_download = list(bucket.list_blobs(prefix=prefix))

        if not blobs_to_download:
            print(f"No files found in {self.source_file_uri}")
            return {"file_count": 0, "total_size_bytes": 0}

        os.makedirs(self.destination_file_path, exist_ok=True)
        total_size = self._download_blobs(blobs_to_download, prefix)

        return {
            "source_file_uri": self.source_file_uri,
            "destination_file_path": self.destination_file_path,
            "file_count": len(blobs_to_download),
            "total_size_bytes": total_size,
        }

    def _validate_paths(self) -> None:
        if not self.source_file_uri.startswith("gs://"):
            raise ValueError("Invalid GCS URI. Must start with 'gs://'")

        # Check if the destination path contains a file name
        if pathlib.Path(self.destination_file_path).suffix:
            raise ValueError(
                f"Destination path {self.destination_file_path} appears to contain "
                f"a file name. Please provide only a directory path."
            )

    def _parse_gcs_uri(self) -> tuple:
        parts = self.source_file_uri.split("/")
        bucket_name = parts[2]
        prefix = "/".join(parts[3:])
        return bucket_name, prefix

    def _download_blobs(self, blobs: list, prefix: str) -> int:
        total_size = 0
        for blob in blobs:
            local_path = os.path.join(
                self.destination_file_path,
                blob.name.replace(prefix, "").lstrip("/"),
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)
            total_size += blob.size
        return total_size
