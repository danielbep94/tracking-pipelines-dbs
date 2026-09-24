# Pipeline Run Tracking (`run_tracking`)

A lightweight, production-ready Python module designed to log append-only execution telemetry into Snowflake (`PRD_MDP.MDP_STG.PIPELINE_RUNS`) from Databricks jobs.

---

## 🛡️ Production Fault-Tolerance Guarantee

**Telemetry failures will NEVER stop or crash your production job.**

If Snowflake is temporarily unreachable, Key Vault secrets fail, or tracking writes encounter an error, `run_tracking` catches and logs the telemetry warning safely without interrupting your pipeline execution.

---

## 📁 Repository Structure

```
run-tracking/
├── run_tracking.py          # Standalone Python module (Zero project dependencies)
├── README.md                # Documentation & integration steps
├── schema/
│   └── pipeline_run_history.sql  # Snowflake DDL for PIPELINE_RUNS table and views
└── tests/
    └── test_run_tracking.py # Unit test suite
```

---

## 🚀 Quick Start (Notebook Integration Steps)

### Step 1: Initialize at the Top of Notebook (Cell 1)

Add this setup block at the top of your notebook:

```python
%python
from run_tracking import run_tracking, get_snowflake_options

# Get Snowflake options using your exact Key Vault secrets
sf_options = get_snowflake_options(
    dbutils,
    secret_scope="DAN-AM-P-KVT800-R-MDP-DB",
    user_key="snowflake-user-dph-reader",
    private_key_secret="Snowflake-Private-Key-DPH-READER",
)

# Start run tracking
tracker = run_tracking(
    spark=spark,
    snowflake_options=sf_options,
    pipeline_name="FACT_PREVENTA_IVY_OTC",
    target_name="PRD_MDP.MDP_STG.FACT_PREVENTA_IVY_OTC",
    dbutils=dbutils,
)
```

---

### Step 2: Feed Row Counts (Inside Main Notebook Logic)

Assign row counts to `tracker` as your DataFrames process:

```python
%python
# 1. Capture source input row count
df_input = spark.read.format("snowflake")...
tracker.source_rows = df_input.count()

# 2. Capture transformed output row count
df_transformed = df_input.filter(...)
tracker.transformed_rows = df_transformed.count()

# 3. (Optional) Track target table count before write
tracker.target_rows_before = 5000

# 4. Write data to Snowflake
df_transformed.write.format("net.snowflake.spark.snowflake")...

# 5. (Optional) Set target rows after write
tracker.target_rows_after = tracker.target_rows_before + tracker.transformed_rows
```

---

### Step 3: Complete Tracking at Bottom of Notebook (Last Cell)

Call `tracker.finish()` at the very end of your notebook:

```python
%python
# Record pipeline completion
tracker.finish()
```

---

## 📊 Table Columns Reference (`PRD_MDP.MDP_STG.PIPELINE_RUNS`)

| Column Name | Type | How It Is Populated |
|---|---|---|
| `RUN_ID` | `VARCHAR` | Auto-generated UUID |
| `PIPELINE_NAME` | `VARCHAR` | `pipeline_name="FACT_PREVENTA_IVY_OTC"` |
| `PIPELINE_VERSION` | `VARCHAR` | Auto-detected widget or default `"workspace-unversioned"` |
| `ENVIRONMENT` | `VARCHAR` | `environment="PROD"` (default) |
| `STATUS` | `VARCHAR` | Auto-set: `"SUCCEEDED"` on completion, `"FAILED"` on error |
| `DATABRICKS_JOB_ID` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `DATABRICKS_JOB_RUN_ID` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `DATABRICKS_TASK_RUN_ID` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `DATABRICKS_TASK_NAME` | `VARCHAR` | Auto-extracted from Databricks runtime context |
| `ATTEMPT_NUMBER` | `NUMBER` | Auto-extracted from Databricks widget (if present) |
| `TRIGGER_TYPE` | `VARCHAR` | Auto-extracted from Databricks widget (default `"one_time"`) |
| `SOURCE_NAME` | `VARCHAR` | `source_name="..."` parameter |
| `SOURCE_FILE` | `VARCHAR` | `source_file="..."` parameter |
| `TARGET_NAME` | `VARCHAR` | `target_name="..."` parameter |
| `PERIOD_START_DATE` | `DATE` | `period_start_date=date(...)` parameter |
| `PERIOD_END_DATE` | `DATE` | `period_end_date=date(...)` parameter |
| `STARTED_AT_UTC` | `TIMESTAMP_TZ` | Auto-captured UTC timestamp at start |
| `COMPLETED_AT_UTC` | `TIMESTAMP_TZ` | Auto-captured UTC timestamp at finish |
| `DURATION_SECONDS` | `NUMBER` | Auto-calculated duration in seconds |
| `SOURCE_ROWS` | `NUMBER` | Set via `tracker.source_rows = count` |
| `STAGING_ROWS` | `NUMBER` | Set via `tracker.staging_rows = count` |
| `TRANSFORMED_ROWS` | `NUMBER` | Set via `tracker.transformed_rows = count` |
| `TARGET_ROWS_BEFORE` | `NUMBER` | Set via `tracker.target_rows_before = count` |
| `TARGET_ROWS_AFTER` | `NUMBER` | Set via `tracker.target_rows_after = count` |
| `TARGET_ROW_DELTA` | `NUMBER` | Auto-calculated `(target_rows_after - target_rows_before)` |
| `BUSINESS_METRICS_JSON` | `VARCHAR` | Auto-serialized JSON set via `tracker.set_metrics(...)` |
| `ERROR_TYPE` | `VARCHAR` | Auto-populated on error (`type(err).__name__`) |
| `ERROR_MESSAGE` | `VARCHAR` | Auto-populated on error (sanitized & masked) |
| `CREATED_AT_UTC` | `TIMESTAMP_TZ` | Auto-captured insert timestamp |

---

## 🧪 Unit Tests

Run local tests:
```bash
python3 -m unittest discover -s tests
```
