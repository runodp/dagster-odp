from typing import Any, Generator, Optional

from dagster import RunRequest, SensorEvaluationContext, SkipReason
from dagster_gcp.gcs.sensor import get_gcs_keys

from ..manager.base_sensor import BaseSensor
from ..manager.sensor_registry import vayu_sensor


@vayu_sensor("gcs_sensor")
class GCSSensor(BaseSensor):
    """
    A sensor that monitors a Google Cloud Storage bucket for new objects.

    This sensor checks for new objects in the specified GCS bucket and yields
    RunRequests for each new object found. It can optionally filter objects
    based on a prefix.

    Attributes:
        bucket_name (str): The name of the GCS bucket to monitor.
        path_prefix_filter (Optional[str]): If provided, only objects with this
            prefix will trigger runs.

    Methods:
        run: Executes the sensor logic to check for new objects and yield
            RunRequests.
    """

    bucket_name: str
    path_prefix_filter: Optional[str] = None

    def run(
        self, context: SensorEvaluationContext
    ) -> Generator[RunRequest, Any, SkipReason | None]:
        client = context.resources.gcs
        since_key = context.cursor or None
        new_gcs_objects = get_gcs_keys(
            self.bucket_name, since_key=since_key, gcs_session=client
        )
        if not new_gcs_objects:
            return SkipReason(f"No new objects in bucket '{self.bucket_name}'")

        filtered_gcs_objects = [
            obj
            for obj in new_gcs_objects
            if self.path_prefix_filter is None
            or obj.startswith(self.path_prefix_filter)
        ]

        for gcs_key in filtered_gcs_objects:
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

            yield RunRequest(run_key=gcs_key, run_config=run_config)

        context.update_cursor(new_gcs_objects[-1])
        return None
