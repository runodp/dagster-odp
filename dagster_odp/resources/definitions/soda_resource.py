import os
from typing import Any, Dict, List, Optional

from dagster import ConfigurableResource
from soda.scan import Scan

from ..resource_registry import odp_resource


@odp_resource("soda")
class SodaResource(ConfigurableResource):
    """
    A resource for executing Soda scans.

    This resource manages the configuration and execution of Soda scans,
    including handling of configuration and check files.

    Attributes:
        project_dir (str): The directory containing the Soda project files.
        checks_dir (Optional[str]): The subdirectory containing check files,
            if different from project_dir.
    """

    project_dir: str
    checks_dir: Optional[str] = None

    def _get_config_file_path(self) -> str:
        config_file_path = os.path.join(self.project_dir, "configuration.yml")
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Configuration file not found: {config_file_path}")
        return config_file_path

    def _get_check_file_path(self, check_file_path: str) -> str:
        if self.checks_dir:
            full_check_file_path = os.path.join(
                self.project_dir, self.checks_dir, check_file_path
            )
        else:
            full_check_file_path = os.path.join(self.project_dir, check_file_path)

        if not os.path.exists(full_check_file_path):
            raise FileNotFoundError(f"Check file not found: {full_check_file_path}")
        return full_check_file_path

    def _setup_scan(self, data_source: str, check_file_path: str) -> Scan:
        scan = Scan()
        scan.set_data_source_name(data_source)
        scan.add_configuration_yaml_file(file_path=self._get_config_file_path())
        scan.add_sodacl_yaml_file(self._get_check_file_path(check_file_path))
        return scan

    def _extract_scan_results(self, scan: Scan) -> List[Dict[str, Any]]:
        scan_results = []
        for check in scan._checks:  # pylint: disable=W0212
            scan_results.append(
                {
                    "name": check.name,
                    "outcome": check.outcome.name if check.outcome else None,
                    "check": check.check_cfg.source_line,
                    "result": check.get_log_diagnostic_dict(),
                }
            )

        if not scan_results:
            raise ValueError("No soda scan results found, check your configuration")

        return scan_results

    def run(self, data_source: str, check_file_path: str) -> List[Dict[str, Any]]:
        """
        Execute a Soda scan for the specified data source and check file.

        Args:
            data_source (str): The name of the data source to scan.
            check_file_path (str): The path to the check file, relative to
                project_dir or checks_dir.

        Returns:
            List[Dict[str, Any]]: Results of the scan.

        Raises:
            FileNotFoundError: If the configuration file or check file is not found.
        """
        scan = self._setup_scan(data_source, check_file_path)
        scan.execute()
        return self._extract_scan_results(scan)
