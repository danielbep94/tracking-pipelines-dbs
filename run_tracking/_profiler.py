"""Spark DataFrame profiling metrics."""

from typing import Any, Dict, List, Optional

from ._utils import emit_tracking_log, sanitize_error_message


class DataFrameProfiler:
    """Calculate lightweight metrics from Spark DataFrames."""

    def __init__(self, log_obj=None):
        self.logger = log_obj

    def profile(
        self,
        df,
        *,
        prefix: str = "",
        count: bool = True,
        distinct_columns: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
        null_columns: Optional[List[str]] = None,
        sum_columns: Optional[List[str]] = None,
        min_columns: Optional[List[str]] = None,
        max_columns: Optional[List[str]] = None,
        avg_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Compute lightweight profiling metrics on a Spark DataFrame and return
        them as a flat dictionary. Call add_metrics() to merge the result
        into BUSINESS_METRICS_JSON.

        Duplicate-related metrics (only computed when key_columns is given):

        * {prefix}duplicate_key_group_count -- number of DISTINCT key
          combinations that appear more than once (i.e. duplicated groups).
        * {prefix}duplicate_row_count -- number of EXCESS rows beyond the
          first occurrence of each duplicated key (e.g. 5 rows sharing one
          key produce duplicate_key_group_count=1, duplicate_row_count=4).

        Null-rate metrics are computed correctly even when count=False: an
        internal row count is still calculated whenever null_columns is
        supplied, so {col}_null_rate is never silently forced to None just
        because the row_count metric itself was not requested.
        """
        result: Dict[str, Any] = {}

        try:
            from pyspark.sql import functions as F

            need_internal_row_count = count or bool(null_columns)

            aggregate_expressions = []

            if need_internal_row_count:
                aggregate_expressions.append(
                    F.count("*").alias("__row_count")
                )

            for column_name in distinct_columns or []:
                aggregate_expressions.append(
                    F.countDistinct(column_name).alias(
                        f"__distinct__{column_name}"
                    )
                )

            for column_name in null_columns or []:
                aggregate_expressions.append(
                    F.sum(F.col(column_name).isNull().cast("int")).alias(
                        f"__null__{column_name}"
                    )
                )

            for column_name in sum_columns or []:
                aggregate_expressions.append(
                    F.sum(column_name).alias(f"__sum__{column_name}")
                )

            for column_name in min_columns or []:
                aggregate_expressions.append(
                    F.min(column_name).alias(f"__min__{column_name}")
                )

            for column_name in max_columns or []:
                aggregate_expressions.append(
                    F.max(column_name).alias(f"__max__{column_name}")
                )

            for column_name in avg_columns or []:
                aggregate_expressions.append(
                    F.avg(column_name).alias(f"__avg__{column_name}")
                )

            internal_row_count = 0

            if aggregate_expressions:
                row = df.agg(*aggregate_expressions).first()

                if row:
                    values = row.asDict()

                    internal_row_count = values.get("__row_count") or 0

                    if count:
                        result[f"{prefix}row_count"] = values.get(
                            "__row_count"
                        )

                    for column_name in distinct_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_distinct_count"
                        ] = values.get(f"__distinct__{column_name}")

                    for column_name in null_columns or []:
                        null_count = values.get(f"__null__{column_name}")

                        result[
                            f"{prefix}{column_name.lower()}_null_count"
                        ] = null_count

                        null_rate_key = (
                            f"{prefix}{column_name.lower()}_null_rate"
                        )

                        if internal_row_count and null_count is not None:
                            result[null_rate_key] = round(
                                null_count / internal_row_count,
                                6,
                            )
                        else:
                            result[null_rate_key] = None

                    for column_name in sum_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_sum"
                        ] = values.get(f"__sum__{column_name}")

                    for column_name in min_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_min"
                        ] = values.get(f"__min__{column_name}")

                    for column_name in max_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_max"
                        ] = values.get(f"__max__{column_name}")

                    for column_name in avg_columns or []:
                        average_value = values.get(f"__avg__{column_name}")

                        result[
                            f"{prefix}{column_name.lower()}_avg"
                        ] = (
                            round(float(average_value), 6)
                            if average_value is not None
                            else None
                        )

            if key_columns:
                duplicate_groups_df = (
                    df.groupBy(*key_columns)
                    .count()
                    .filter(F.col("count") > 1)
                )

                duplicate_key_group_count = duplicate_groups_df.count()

                excess_row_count_row = duplicate_groups_df.agg(
                    F.sum(F.col("count") - F.lit(1)).alias("__excess_rows")
                ).first()

                duplicate_row_count = 0

                if (
                    excess_row_count_row is not None
                    and excess_row_count_row["__excess_rows"] is not None
                ):
                    duplicate_row_count = int(
                        excess_row_count_row["__excess_rows"]
                    )

                result[
                    f"{prefix}duplicate_key_group_count"
                ] = duplicate_key_group_count

                result[
                    f"{prefix}duplicate_row_count"
                ] = duplicate_row_count

        except Exception as profile_error:
            emit_tracking_log(
                level="WARNING",
                event="PROFILE_FAILED",
                log_obj=self.logger,
                error_type=type(profile_error).__name__,
                error_message=sanitize_error_message(profile_error),
            )

        return result
