# Pipeline Run Tracking (`run_tracking`)

A lightweight, production-ready Python module designed to log append-only execution telemetry into Snowflake (`PRD_MDP.MDP_STG.PIPELINE_RUNS`) from Databricks jobs.

---

## Production Fault-Tolerance Guarantee

**Telemetry failures will NEVER stop or crash your production job.**

If Snowflake is temporarily unreachable, Key Vault secrets fail, or tracking writes encounter an error, `run_tracking` catches and logs the telemetry warning safely without interrupting your pipeline execution.

---

## Repository Structure

```
run-tracking/
├── run_tracking.py          # Standalone Python module (zero project dependencies)
├── README.md                # Documentation & integration steps
├── schema/
│   └── pipeline_run_history.sql  # Snowflake DDL for PIPELINE_RUNS table and views
└── tests/
    └── test_run_tracking.py # Unit test suite (20 tests)
```

---

## Quick Start (Notebook Integration Steps)

### Step 1: Initialize at the Top of Notebook (Cell 1)

```python
%python
from run_tracking import run_tracking, get_snowflake_options

sf_options = get_snowflake_options(
    dbutils,
    secret_scope="DAN-AM-P-KVT800-R-MDP-DB",
    user_key="snowflake-user-dph-reader",
    private_key_secret="Snowflake-Private-Key-DPH-READER",
)

tracker = run_tracking(
    spark=spark,
    snowflake_options=sf_options,
    pipeline_name="FACT_PREVENTA_IVY_OTC",
    target_name="PRD_MDP.MDP_STG.FACT_PREVENTA_IVY_OTC",
    dbutils=dbutils,
)
```

### Step 2: (Optional) Feed Row Counts

Row count metrics default to `NULL`. **Assign only if business explicitly requires row-level tracking.**

```python
%python
tracker.source_rows = df_input.count()
tracker.transformed_rows = df_transformed.count()
```

### Step 3: Complete Tracking (Last Cell)

```python
%python
tracker.finish()
```

---

## BUSINESS_METRICS_JSON — Extensibility Mechanism

`BUSINESS_METRICS_JSON` is the single extensibility point for all additional metrics.
All profiling results, reconciliation values, DQ summaries, business KPIs, and runtime
metadata flow into this single JSON column. **No schema changes are needed when adding new metrics.**

Example payload:

```json
{
  "source_row_count": 10000,
  "target_row_count": 10000,
  "kilos_sum": 250000,
  "kilos_null_count": 12,
  "kilos_null_rate": 0.0012,
  "duplicate_count": 0,
  "dq_checks_total": 5,
  "dq_checks_passed": 5,
  "dq_checks_failed": 0,
  "reconciliation_status": "PASS",
  "runtime_spark_version": "3.5.0",
  "runtime_cluster_id": "0101-123456-abc123",
  "runtime_notebook_path": "/Repos/team/FACT_PREVENTA_IVY_OTC"
}
```

---

## Lightweight DataFrame Profiling

`tracker.profile()` computes row counts, null rates, sums, and duplicate counts on a Spark
DataFrame. The result is returned as a plain dictionary — call `tracker.add_metrics()` to store it.

```python
%python
source_profile = tracker.profile(
    df_source,
    prefix="source_",
    sum_columns=["KILOS"],
    key_columns=["ORDER_ID"],   # used to detect duplicates
    null_columns=["KILOS"],
)
tracker.add_metrics(source_profile)

target_profile = tracker.profile(df_target, prefix="target_", sum_columns=["KILOS"])
tracker.add_metrics(target_profile)

tracker.add_metrics({"reconciliation_status": "PASS"})
```

Available `profile()` parameters:

| Parameter | Description |
|---|---|
| `prefix` | Key prefix applied to every metric (e.g. `"source_"`) |
| `count` | Include `row_count` (default `True`) |
| `distinct_columns` | Columns for `{col}_distinct_count` |
| `key_columns` | Columns used to detect duplicates, produces `duplicate_count` |
| `null_columns` | Columns for `{col}_null_count` and `{col}_null_rate` |
| `sum_columns` | Columns for `{col}_sum` |
| `min_columns` | Columns for `{col}_min` |
| `max_columns` | Columns for `{col}_max` |
| `avg_columns` | Columns for `{col}_avg` |

---

## Lightweight DQ Framework

Use `tracker.check()` to register boolean DQ checks. Only aggregate counts are stored in
`BUSINESS_METRICS_JSON` — no per-check detail table is created.

```python
%python
tracker.check(df_target.filter("KILOS < 0").count() == 0,     "no_negative_kilos")
tracker.check(df_target.filter("KILOS IS NULL").count() == 0, "no_null_kilos")
tracker.check(df_target.filter("ORDER_ID IS NULL").count() == 0, "no_null_order_ids")
```

At `tracker.finish()`, the DQ summary is automatically written into `BUSINESS_METRICS_JSON`:

```json
{ "dq_checks_total": 3, "dq_checks_passed": 3, "dq_checks_failed": 0 }
```

`check()` also returns the boolean result so you can branch on it:

```python
if not tracker.check(dup_count == 0, "no_duplicates"):
    raise ValueError("Duplicate rows detected — aborting load.")
```

---

## Snowflake Session Tagging

Tag the active Snowflake session with `RUN_ID`, `PIPELINE_NAME`, and `PIPELINE_VERSION`
so that Snowflake `QUERY_HISTORY` can be correlated back to each execution.

```python
%python
from run_tracking import tag_snowflake_session

tag_snowflake_session(
    spark=spark,
    snowflake_options=sf_options,
    run_id=tracker.context.run_id,
    pipeline_name="FACT_PREVENTA_IVY_OTC",
    pipeline_version="1.2.0",
)
```

This is best-effort: any failure is logged and silently suppressed.

---

## Runtime Metadata (Auto-Collected)

At initialization the tracker automatically collects Databricks/Spark runtime values
and stores them inside `BUSINESS_METRICS_JSON` under a `runtime_` prefix:

| Key | Description |
|---|---|
| `runtime_spark_version` | Spark version string |
| `runtime_cluster_id` | Databricks cluster ID |
| `runtime_notebook_path` | Notebook path from Databricks context |
| `runtime_git_sha` | Git commit SHA (if available) |

To disable auto-collection:

```python
tracker = run_tracking(..., collect_runtime_metadata=False)
```

---

## Table Columns Reference (`PRD_MDP.MDP_STG.PIPELINE_RUNS`)

| Column Name | Type | How It Is Populated |
|---|---|---|
| `RUN_ID` | `VARCHAR` | Auto-generated UUID |
| `PIPELINE_NAME` | `VARCHAR` | `pipeline_name="..."` parameter |
| `PIPELINE_VERSION` | `VARCHAR` | Auto-detected widget or `"workspace-unversioned"` |
| `ENVIRONMENT` | `VARCHAR` | `environment="PROD"` (default) |
| `STATUS` | `VARCHAR` | Auto-set: `"SUCCEEDED"` or `"FAILED"` |
| `DATABRICKS_JOB_ID` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `DATABRICKS_JOB_RUN_ID` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `DATABRICKS_TASK_RUN_ID` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `DATABRICKS_TASK_NAME` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `ATTEMPT_NUMBER` | `NUMBER` | Auto-extracted from Databricks widget |
| `TRIGGER_TYPE` | `VARCHAR` | Auto-extracted from widget (default `"one_time"`) |
| `SOURCE_NAME` | `VARCHAR` | `source_name="..."` parameter |
| `SOURCE_FILE` | `VARCHAR` | `source_file="..."` parameter |
| `TARGET_NAME` | `VARCHAR` | `target_name="..."` parameter |
| `PERIOD_START_DATE` | `DATE` | `period_start_date=date(...)` parameter |
| `PERIOD_END_DATE` | `DATE` | `period_end_date=date(...)` parameter |
| `STARTED_AT_UTC` | `TIMESTAMP_TZ` | Auto-captured UTC timestamp at start |
| `COMPLETED_AT_UTC` | `TIMESTAMP_TZ` | Auto-captured UTC timestamp at finish |
| `DURATION_SECONDS` | `NUMBER` | Auto-calculated duration in seconds |
| `SOURCE_ROWS` | `NUMBER` | Optional — `tracker.source_rows = count` |
| `STAGING_ROWS` | `NUMBER` | Optional — `tracker.staging_rows = count` |
| `TRANSFORMED_ROWS` | `NUMBER` | Optional — `tracker.transformed_rows = count` |
| `TARGET_ROWS_BEFORE` | `NUMBER` | Optional — `tracker.target_rows_before = count` |
| `TARGET_ROWS_AFTER` | `NUMBER` | Optional — `tracker.target_rows_after = count` |
| `TARGET_ROW_DELTA` | `NUMBER` | Auto-calculated `(target_rows_after - target_rows_before)` |
| `BUSINESS_METRICS_JSON` | `VARCHAR` | Profiling, DQ summary, runtime metadata, custom KPIs |
| `ERROR_TYPE` | `VARCHAR` | Auto-populated on error (`type(err).__name__`) |
| `ERROR_MESSAGE` | `VARCHAR` | Auto-populated on error (sanitized & masked) |
| `CREATED_AT_UTC` | `TIMESTAMP_TZ` | Auto-captured insert timestamp |

---

## Unit Tests

```bash
PYTHONPATH=. pytest -v
```

---

## Future Evolution: Advanced Observability Model

The current architecture is deliberately simple and covers the majority of operational ETL needs.

### Current Model

```
1 RUN_ID  =  1 ROW in PIPELINE_RUNS
All metrics stored in BUSINESS_METRICS_JSON
```

**Best for:** ETL monitoring, audit trails, reconciliation, operational support, small and medium data platforms.

---

### Advanced Model (Future — adopt only when business demands it)

If requirements evolve toward metric-level trend analysis or enterprise observability dashboards, the next step would be a separate metric facts table:

```
RUN_ID | COLUMN_NAME | METRIC_NAME  | METRIC_VALUE
------------------------------------------------------
abc123 | KILOS       | SUM          | 250000
abc123 | KILOS       | AVG          | 25
abc123 | KILOS       | NULL_COUNT   | 12
abc123 | ORDER_ID    | DISTINCT     | 10000
abc123 | -           | DQ_FAILED    | 1
```

**Advantages:**
- Trend analysis per metric and column over time
- Enterprise observability and data quality scorecards
- Historical metric reporting and anomaly detection

**Disadvantages:**
- Additional Snowflake tables and maintenance
- More storage and architectural complexity
- Harder to onboard quickly across many pipelines

### Recommendation

> **Stay with the current single-row architecture until there is a clear and validated business requirement for metric-level historical analysis.**
>
> `BUSINESS_METRICS_JSON` provides sufficient flexibility for all current use cases with zero schema migration cost.
