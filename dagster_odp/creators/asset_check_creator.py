import os
from typing import Any, Dict, List

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetKey,
    asset_check,
)

from dagster_odp.config_manager.builders.workflow_builder import WorkflowBuilder


def _get_check_result(check_results: List[Dict[str, Any]]) -> AssetCheckResult:
    outcomes = [r["outcome"] for r in check_results]
    has_fail = "FAIL" in outcomes
    has_warn = "WARN" in outcomes
    has_none = any(outcome is None for outcome in outcomes)

    if has_none:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={
                c["check"]: (
                    "ERROR: No outcome" if c["outcome"] is None else c["outcome"]
                )
                for c in check_results
            },
        )

    return AssetCheckResult(
        passed=not (has_fail or has_warn),
        severity=AssetCheckSeverity.ERROR if has_fail else AssetCheckSeverity.WARN,
        metadata={c["check"]: c["outcome"] for c in check_results},
    )


def _get_asset_check_def(check_params: Dict[str, Any]) -> AssetChecksDefinition:
    @asset_check(
        name=os.path.basename(check_params["check_file_path"]).split(".")[0],
        asset=AssetKey(check_params["asset_key"].split("/")),
        description=check_params["description"],
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
