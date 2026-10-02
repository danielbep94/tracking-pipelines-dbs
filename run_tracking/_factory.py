"""Public RunTracker factory."""

from typing import Dict, Optional

from ._tracker import RunTracker


def run_tracking(
    spark=None,
    snowflake_options: Optional[Dict[str, str]] = None,
    pipeline_name: str = "UNNAMED_PIPELINE",
    environment: str = "PROD",
    table_name: str = "PRD_MDP.MDP_STG.PIPELINE_RUNS",
    dbutils=None,
    **kwargs,
) -> RunTracker:
    """Create a RunTracker configured for one pipeline execution."""
    return RunTracker(
        spark=spark,
        snowflake_options=snowflake_options,
        pipeline_name=pipeline_name,
        environment=environment,
        table_name=table_name,
        dbutils=dbutils,
        **kwargs,
    )
