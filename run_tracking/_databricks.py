"""Safe access to Databricks widgets, context tags, and runtime metadata."""

import json
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def get_widget_value(
    dbutils,
    name: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Safely read a Databricks notebook widget."""
    if dbutils is None:
        return default

    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default

    normalized = str(value).strip()

    return normalized if normalized else default


def get_databricks_context_tag(
    dbutils,
    tag_name: str,
) -> Optional[str]:
    """Safely extract a Databricks runtime context tag."""
    if dbutils is None:
        return None

    try:
        context_json = (
            dbutils.notebook.entry_point
            .getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )

        context_data = json.loads(context_json)
        tags = context_data.get("tags", {})

        value = tags.get(tag_name)

        if value is None:
            return None

        normalized = str(value).strip()

        return normalized if normalized else None

    except Exception:
        return None


def _collect_runtime_metadata(
    spark=None,
    dbutils=None,
) -> Dict[str, Any]:
    """Collect optional Databricks and Spark runtime metadata."""
    metadata: Dict[str, Any] = {}

    if spark is not None:
        try:
            metadata["runtime_spark_version"] = spark.version
        except Exception:
            pass

        try:
            metadata["runtime_cluster_id"] = spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterId",
                None,
            )
        except Exception:
            pass

    if dbutils is not None:
        try:
            context_json = (
                dbutils.notebook.entry_point
                .getDbutils()
                .notebook()
                .getContext()
                .toJson()
            )

            context_data = json.loads(context_json)
            tags = context_data.get("tags", {})
            extra_context = context_data.get("extraContext", {})

            metadata_mapping = (
                ("notebookPath", "runtime_notebook_path"),
                ("gitCommit", "runtime_git_sha"),
                ("browserHostName", "runtime_host"),
            )

            for source_key, target_key in metadata_mapping:
                value = tags.get(source_key) or extra_context.get(source_key)

                if value:
                    metadata[target_key] = str(value).strip()

        except Exception:
            pass

    return {
        key: value
        for key, value in metadata.items()
        if value is not None
    }
