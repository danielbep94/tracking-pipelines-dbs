"""Snowflake query tags for connector read and write sessions."""

import json
from typing import Dict, Optional

from ._utils import emit_tracking_log


# ---------------------------------------------------------------------------
# Snowflake query tagging
# ---------------------------------------------------------------------------

def build_query_tag_statement(
    run_id: str,
    pipeline_name: str,
    pipeline_version: Optional[str] = None,
) -> str:
    """
    Build an "ALTER SESSION SET QUERY_TAG = ..." statement for auditing in
    Snowflake QUERY_HISTORY.

    This statement must be attached to an actual read or write operation
    using the "preactions" (or "postactions") option of the Snowflake Spark
    connector. The connector's DataFrameReader "query" option only supports
    SELECT statements, so ALTER SESSION cannot be executed as a standalone
    read.

    Example
    -------
        tag_sql = build_query_tag_statement(
            run_id=tracker.context.run_id,
            pipeline_name="FACT_SALES_OTC",
            pipeline_version="1.2.0",
        )

        (
            df.write.format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .option("dbtable", "MDP_STG.MY_TABLE")
            .option("preactions", tag_sql)
            .mode("append")
            .save()
        )
    """
    tag_payload = json.dumps(
        {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "pipeline_version": pipeline_version or "unversioned",
        },
        separators=(",", ":"),
    )

    escaped_tag_payload = tag_payload.replace("'", "''")

    return f"ALTER SESSION SET QUERY_TAG = '{escaped_tag_payload}'"


def tag_snowflake_session(
    spark=None,
    snowflake_options: Optional[Dict[str, str]] = None,
    run_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    pipeline_version: Optional[str] = None,
) -> str:
    """
    Deprecated.

    Previous versions of this function executed a standalone read using
    ``spark.read...option("query", "ALTER SESSION ...")``. Snowflake's Spark
    connector only supports SELECT statements through that path, so the
    previous implementation silently failed or raised at runtime.

    This function no longer executes anything. It returns the QUERY_TAG SQL
    statement so callers can attach it to their own read/write operation via
    the "preactions" option. See build_query_tag_statement() for the
    non-deprecated equivalent.
    """
    emit_tracking_log(
        level="WARNING",
        event="SNOWFLAKE_TAG_DEPRECATED",
        run_id=run_id,
        pipeline_name=pipeline_name,
        reason=(
            "tag_snowflake_session() no longer executes a standalone ALTER "
            "SESSION statement, because Snowflake's Spark connector only "
            "supports SELECT through the read 'query' option. Use "
            "build_query_tag_statement() with the 'preactions' write "
            "option instead."
        ),
    )

    return build_query_tag_statement(
        run_id=run_id,
        pipeline_name=pipeline_name,
        pipeline_version=pipeline_version,
    )
