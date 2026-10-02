"""Data-quality check accumulation and summary extraction."""

from typing import Any, Dict, List, Optional

from ._utils import emit_tracking_log


class DQAccumulator:
    """Collect check outcomes and produce the aggregate metrics summary."""

    def __init__(self, log_obj=None):
        self.logger = log_obj
        self.results: List[Dict[str, Any]] = []

    def check(self, condition: bool, name: str, required: bool = False) -> bool:
        passed = bool(condition)
        self.results.append({"name": name, "passed": passed, "required": required})
        emit_tracking_log(
            level="INFO" if passed else ("ERROR" if required else "WARNING"),
            event="DQ_CHECK_PASSED" if passed else "DQ_CHECK_FAILED",
            log_obj=self.logger,
            check=name,
            required=required,
        )
        return passed

    def summary(self) -> Optional[Dict[str, Any]]:
        if not self.results:
            return None
        total = len(self.results)
        passed = sum(1 for result in self.results if result["passed"])
        failed_required = [
            result["name"] for result in self.results
            if not result["passed"] and result["required"]
        ]
        return {
            "dq_checks_total": total,
            "dq_checks_passed": passed,
            "dq_checks_failed": total - passed,
            "dq_required_checks_failed": len(failed_required),
            "failed_required_checks": failed_required,
        }
