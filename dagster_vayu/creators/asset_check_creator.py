import os
from typing import Any, Dict, List

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    asset_check,
)

from dagster_vayu.config_manager.builders.workflow_builder import WorkflowBuilder


def _get_check_result(check_results: List[Dict[str, Any]]) -> AssetCheckResult:
    outcomes = [r["outcome"] for r in check_results]
    has_fail = "FAIL" in outcomes
    has_warn = "WARN" in outcomes

    return AssetCheckResult(
        passed=not (has_fail or has_warn),
        severity=AssetCheckSeverity.ERROR if has_fail else AssetCheckSeverity.WARN,
        metadata={c["check"]: c["outcome"] for c in check_results},
    )


def _get_asset_check_def(check_params: Dict[str, Any]) -> AssetChecksDefinition:
    @asset_check(
        name=os.path.basename(check_params["check_file_path"]).split(".")[0],
        asset=check_params["asset_key"],
        required_resource_keys={"soda"},
        compute_kind="soda",
        blocking=check_params["blocking"],
        metadata=check_params,
    )
    def _check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        check_results = context.resources.soda.run(
            data_source=check_params["data_source"],
            check_file_path=check_params["check_file_path"],
        )
        return _get_check_result(check_results)

    return _check


def get_asset_checks(wb: WorkflowBuilder) -> List[AssetChecksDefinition]:
    """
    Builds the asset checks based on the soda check specs in the workflow config files.

    Args:
        wb (WorkflowBuilder): An instance of the WorkflowBuilder class.

    Returns:
        List[AssetChecksDefinition]: List of asset check definitions.
    """
    return [
        _get_asset_check_def(check_specs.model_dump()) for check_specs in wb.soda_checks
    ]
