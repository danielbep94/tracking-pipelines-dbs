# Pipeline Run Tracking — `run_tracking`

> **Owner:** Data & Analytics — MDP Platform  
> **Table:** `PRD_MDP.MDP_STG.PIPELINE_RUNS`  
> **Repo:** `danielbep94/tracking-pipelines-dbs`

A standalone, zero-dependency Python module that logs one execution record per Databricks job run into Snowflake. Designed for operational monitoring, audit trails, and lightweight data quality tracking.

---

## Repository Contents

```
run-tracking/
├── run_tracking.py               # Module — copy this into your Databricks Repo
├── README.md                     # This document
├── schema/
│   └── pipeline_run_history.sql  # Snowflake DDL (run once by platform team)
└── tests/
    └── test_run_tracking.py      # Unit tests
```

---

## Key Design Rules

- **One job run = one row** in `PIPELINE_RUNS`. Never more.
- **Row counts are optional.** They default to `NULL`. Only populate them if business explicitly requires it for your job.
- **All extra metrics go into `BUSINESS_METRICS_JSON`.** Profiling results, DQ summaries, reconciliation values, and KPIs all land in that single JSON column. No schema changes needed when you add new metrics.
- **Telemetry never crashes your job.** All tracking failures are caught and logged silently.

---

## How to Integrate — Step by Step

### Prerequisites

1. Confirm the `PIPELINE_RUNS` table exists in Snowflake (ask the platform team to run `schema/pipeline_run_history.sql` if not).
2. Add this repository to your Databricks Workspace via **Repos** so that `run_tracking.py` is available on the cluster path.

---

### Step 1 — Import and Initialize (first cell of your notebook)

```python
# Cell 1
from run_tracking import run_tracking, get_snowflake_options

sf_options = get_snowflake_options(
    dbutils,
    secret_scope="DAN-AM-P-KVT800-R-MDP-DB",   # your Key Vault scope
    user_key="snowflake-user-dph-reader",
    private_key_secret="Snowflake-Private-Key-DPH-READER",
)

tracker = run_tracking(
    spark=spark,
    snowflake_options=sf_options,
    pipeline_name="FACT_PREVENTA_IVY_OTC",          # unique job name — use UPPER_SNAKE_CASE
    target_name="PRD_MDP.MDP_STG.FACT_PREVENTA_IVY_OTC",  # (optional) target table
    dbutils=dbutils,
)
```

> The tracker starts automatically. Runtime metadata (Spark version, cluster ID,
> notebook path) is collected and stored in `BUSINESS_METRICS_JSON`.

---

### Step 2 — Run your pipeline logic (your existing notebook cells)

Write your transformation logic as you normally would. The tracker runs in the background.

```python
# Cell 2+ — your existing pipeline code
df_source = spark.read.format("snowflake").options(**sf_options).option("dbtable", "SOURCE_TABLE").load()
df_target  = df_source.filter("ACTIVE = 1").withColumn(...)

df_target.write.format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "PRD_MDP.MDP_STG.FACT_PREVENTA_IVY_OTC") \
    .mode("overwrite").save()
```

---

### Step 3 — (Optional) Add custom metrics

Use any combination of the options below, only if your job requires them.

#### Option A — Manual key/value metrics

```python
tracker.set_metrics(
    reconciliation_status="PASS",
    source_row_count=10000,
    target_row_count=10000,
)
```

#### Option B — Lightweight DataFrame profiling

```python
# Returns a dict — you must call add_metrics() to store it
source_profile = tracker.profile(
    df_source,
    prefix="source_",
    sum_columns=["KILOS"],
    null_columns=["KILOS"],
    key_columns=["ORDER_ID"],   # used to detect duplicates
)
tracker.add_metrics(source_profile)
```

Supported `profile()` options:

| Parameter | What it produces |
|---|---|
| `count=True` | `{prefix}row_count` |
| `sum_columns=["COL"]` | `{prefix}col_sum` |
| `min_columns=["COL"]` | `{prefix}col_min` |
| `max_columns=["COL"]` | `{prefix}col_max` |
| `avg_columns=["COL"]` | `{prefix}col_avg` |
| `null_columns=["COL"]` | `{prefix}col_null_count`, `{prefix}col_null_rate` |
| `distinct_columns=["COL"]` | `{prefix}col_distinct_count` |
| `key_columns=["COL"]` | `{prefix}duplicate_count` |

#### Option C — Lightweight DQ checks

```python
tracker.check(df_target.filter("KILOS < 0").count() == 0,     "no_negative_kilos")
tracker.check(df_target.filter("KILOS IS NULL").count() == 0, "no_null_kilos")
tracker.check(df_target.filter("ORDER_ID IS NULL").count() == 0, "no_null_order_ids")
```

At `finish()` time, the DQ summary is automatically written into `BUSINESS_METRICS_JSON`:

```json
{ "dq_checks_total": 3, "dq_checks_passed": 3, "dq_checks_failed": 0 }
```

`check()` returns the boolean value, so you can also use it to halt execution:

```python
if not tracker.check(dup_count == 0, "no_duplicates"):
    raise ValueError("Duplicate rows found — aborting.")
```

#### Option D — Row counts (only if business explicitly requires it)

```python
tracker.source_rows      = df_source.count()
tracker.transformed_rows = df_target.count()
```

> **Warning:** `.count()` triggers a full Spark scan. Only add this if it is a documented business requirement for the job.

---

### Step 4 — Finish tracking (last cell of your notebook)

```python
# Last cell
tracker.finish()
```

That's it. One row is written to `PIPELINE_RUNS` in Snowflake.

---

## What Gets Logged Automatically

You do not need to configure any of this — it is captured at `tracker = run_tracking(...)`:

| Field | Value |
|---|---|
| `RUN_ID` | Unique UUID for this execution |
| `PIPELINE_NAME` | Your `pipeline_name` parameter |
| `PIPELINE_VERSION` | From Databricks widget or `"workspace-unversioned"` |
| `STATUS` | `SUCCEEDED` on finish, `FAILED` on any unhandled exception |
| `STARTED_AT_UTC` | UTC timestamp at tracker initialization |
| `COMPLETED_AT_UTC` | UTC timestamp at `tracker.finish()` |
| `DURATION_SECONDS` | Elapsed time in seconds |
| `DATABRICKS_JOB_ID` | Auto-extracted from Databricks runtime |
| `DATABRICKS_JOB_RUN_ID` | Auto-extracted from Databricks runtime |
| `DATABRICKS_TASK_NAME` | Auto-extracted from Databricks runtime |
| `ERROR_TYPE` | Exception class name on failure |
| `ERROR_MESSAGE` | Sanitized error message on failure |
| `BUSINESS_METRICS_JSON` | Runtime metadata + any custom/profiling/DQ metrics |

---

## BUSINESS_METRICS_JSON — Full Example Payload

```json
{
  "runtime_spark_version": "3.5.0",
  "runtime_cluster_id": "0101-123456-abc123",
  "runtime_notebook_path": "/Repos/team/FACT_PREVENTA_IVY_OTC",
  "source_row_count": 10000,
  "source_kilos_sum": 250000,
  "source_kilos_null_count": 12,
  "source_kilos_null_rate": 0.0012,
  "source_duplicate_count": 0,
  "dq_checks_total": 3,
  "dq_checks_passed": 3,
  "dq_checks_failed": 0,
  "reconciliation_status": "PASS"
}
```

---

## Querying the Results in Snowflake

```sql
-- Last 10 runs for a specific pipeline
SELECT
    RUN_ID,
    STATUS,
    STARTED_AT_UTC,
    DURATION_SECONDS,
    PARSE_JSON(BUSINESS_METRICS_JSON):dq_checks_failed::INT  AS dq_failed,
    PARSE_JSON(BUSINESS_METRICS_JSON):reconciliation_status::VARCHAR AS recon_status,
    ERROR_MESSAGE
FROM PRD_MDP.MDP_STG.PIPELINE_RUNS
WHERE PIPELINE_NAME = 'FACT_PREVENTA_IVY_OTC'
ORDER BY STARTED_AT_UTC DESC
LIMIT 10;
```

---

## (Optional) Tag Snowflake Sessions for Auditing

If your team uses Snowflake `QUERY_HISTORY` for cost or audit tracking, you can tag the session so that all Snowflake queries from your job can be identified by `RUN_ID`:

```python
from run_tracking import tag_snowflake_session

tag_snowflake_session(
    spark=spark,
    snowflake_options=sf_options,
    run_id=tracker.context.run_id,
    pipeline_name="FACT_PREVENTA_IVY_OTC",
)
```

---

## Available Parameters for `run_tracking()`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `spark` | SparkSession | required | Active Spark session |
| `snowflake_options` | dict | required | Output of `get_snowflake_options()` |
| `pipeline_name` | str | `"UNNAMED_PIPELINE"` | Unique name for this job (UPPER_SNAKE_CASE) |
| `dbutils` | dbutils | required | Databricks dbutils object |
| `environment` | str | `"PROD"` | `"PROD"` or `"DEV"` |
| `table_name` | str | `PRD_MDP.MDP_STG.PIPELINE_RUNS` | Target tracking table |
| `target_name` | str | `None` | Target table written by this pipeline |
| `source_name` | str | `None` | Source table read by this pipeline |
| `source_file` | str | `None` | Source file path (for file-based pipelines) |
| `period_start_date` | date | `None` | Business period start |
| `period_end_date` | date | `None` | Business period end |
| `collect_runtime_metadata` | bool | `True` | Auto-collect Spark/cluster info into JSON |
| `pipeline_version` | str | `None` | Override pipeline version string |

---

## Running Unit Tests

```bash
# From the run-tracking/ directory
PYTHONPATH=. pytest -v
```

Expected: **20 passed**.

---

## Future Evolution: Advanced Observability

The current model is the right choice for most pipelines:

```
1 RUN_ID  =  1 ROW  +  all metrics in BUSINESS_METRICS_JSON
```

If a future business requirement demands metric-level trend analysis (e.g. tracking `KILOS_SUM` per run over 12 months), the architecture would evolve to a separate metric facts table:

```
RUN_ID | COLUMN_NAME | METRIC_NAME | METRIC_VALUE
--------------------------------------------------
abc123 | KILOS       | SUM         | 250000
abc123 | KILOS       | NULL_COUNT  | 12
abc123 | ORDER_ID    | DISTINCT    | 10000
```

**Do not implement this until there is a validated business requirement.** The current JSON model is simpler, cheaper, and covers all current needs.
