# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Pipeline: CEO Asistencia — PRODUCTION
# MAGIC
# MAGIC **Process:** `CEO_ASISTENCIA`
# MAGIC **Mode:** Production. No widgets, no dry-run, no manual period input.
# MAGIC
# MAGIC Workflow: `incoming/` -> validate filenames/period -> stage to Hive/Delta ->
# MAGIC transform and idempotent target loads (existing orchestrator) ->
# MAGIC independent Delta row-count validation -> promote files to `current/`.
# MAGIC
# MAGIC If the run fails for any reason, every file inside `incoming/` is deleted
# MAGIC so the corrected source can be uploaded again with its canonical filename.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

import re
import logging
import unicodedata

PROCESS_NAME   = "CEO_ASISTENCIA"

BASE_DBFS_PATH = "dbfs:/FileStore/tables/rh_danone"
INCOMING_PATH  = f"{BASE_DBFS_PATH}/incoming/"
CURRENT_PATH   = f"{BASE_DBFS_PATH}/current/"

TARGET_CATALOG = "hive_metastore"
TARGET_SCHEMA  = "RH_DANONE"
DELTA_TARGET_TABLE = "hive_metastore.default.fact_ceo_asistencia"

EXPECTED_FILE_COUNT = 1
EXPECTED_PREFIXES   = ["CEO_ASISTENCIA"]

FILENAME_REGEX = re.compile(
    r"^(?P<prefix>[a-zA-Z0-9_]+)_(?P<month>0[1-9]|1[0-2])_(?P<year>\d{4})\.csv$"
)
DUPLICATE_SUFFIX_REGEX = re.compile(r"(-\d+|\(\d+\)|_copy)\.csv$", re.IGNORECASE)

STAGING_TO_BUSINESS_COLUMN_MAP = {}
YEAR_COLUMN = "ANIO"
MONTH_COLUMN = "NUM_MES"

LOG_PREFIX = f"[{PROCESS_NAME}]"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(PROCESS_NAME)

# The Databricks Py4J bridge can emit one INFO message per JVM command and may
# also log a recovered connection reset as INFO. These messages obscured the
# actual pipeline result in the supplied execution log.
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)


def log(msg, level="info"):
    getattr(logger, level)(f"{LOG_PREFIX} {msg}")


class PipelineError(Exception):
    """Controlled production pipeline failure."""


# COMMAND ----------

def list_incoming_csv_files():
    """
    Return the CSV files directly inside incoming/, or None when there are none.

    An empty folder is an operational no-op, not a failure: the job exits green
    without touching any table. Non-CSV files are reported so they cannot sit
    unnoticed in the folder forever.
    """
    dbutils.fs.mkdirs(BASE_DBFS_PATH)
    dbutils.fs.mkdirs(INCOMING_PATH)
    dbutils.fs.mkdirs(CURRENT_PATH)

    try:
        listing = dbutils.fs.ls(INCOMING_PATH)
    except Exception as e:
        raise PipelineError(f"Cannot access INCOMING_PATH {INCOMING_PATH}: {e}")

    items = [f for f in listing if not f.name.endswith("/")]
    csv_files = [f for f in items if f.name.lower().endswith(".csv")]
    other_files = [f.name for f in items if not f.name.lower().endswith(".csv")]

    if other_files:
        log(f"Non-CSV file(s) present in incoming: {sorted(other_files)}",
            level="warning")

    if len(csv_files) == 0:
        return None

    return csv_files


def validate_filenames_and_period(csv_files):
    if len(csv_files) != EXPECTED_FILE_COUNT:
        raise PipelineError(
            f"Expected exactly {EXPECTED_FILE_COUNT} file(s) in incoming, "
            f"found {len(csv_files)}: {[f.name for f in csv_files]}"
        )

    parsed = []
    for f in csv_files:
        if DUPLICATE_SUFFIX_REGEX.search(f.name):
            raise PipelineError(f"Rejected duplicate-upload filename: {f.name}")

        m = FILENAME_REGEX.match(f.name)
        if not m:
            raise PipelineError(
                f"Filename '{f.name}' does not match required pattern "
                "<prefix>_<mm>_<yyyy>.csv"
            )

        parsed.append({
            "path": f.path,
            "name": f.name,
            "prefix": m.group("prefix").upper(),
            "month": int(m.group("month")),
            "year": int(m.group("year")),
        })

    periods = {(p["year"], p["month"]) for p in parsed}
    if len(periods) != 1:
        raise PipelineError(
            f"All files must share the same year/month, found: {periods}"
        )
    year, month = periods.pop()

    prefixes_found = [p["prefix"] for p in parsed]
    if len(set(prefixes_found)) != len(prefixes_found):
        raise PipelineError(f"Duplicate prefixes detected in batch: {prefixes_found}")

    missing = set(EXPECTED_PREFIXES) - set(prefixes_found)
    unexpected = set(prefixes_found) - set(EXPECTED_PREFIXES)
    if missing or unexpected:
        raise PipelineError(
            f"Prefix mismatch. Missing={missing} Unexpected={unexpected}"
        )

    log(f"Validated batch | year={year} month={month:02d} prefixes={prefixes_found}")
    return year, month, parsed


# COMMAND ----------

def normalize_column_name(name: str) -> str:
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", ascii_only)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned.upper()


def stage_csv_to_hive(path, prefix, year, month):
    log(f"Staging {path} (prefix={prefix})")

    df = spark.read.option("header", True).option("inferSchema", True).csv(path)

    row_count = df.count()
    if row_count == 0:
        raise PipelineError(f"File {path} has zero data rows - rejecting.")

    normalized_cols = [normalize_column_name(c) for c in df.columns]
    if len(set(normalized_cols)) != len(normalized_cols):
        dupes = sorted({c for c in normalized_cols if normalized_cols.count(c) > 1})
        raise PipelineError(
            f"Column normalization produced duplicate columns for {path}: {dupes}"
        )

    df_staged = df.toDF(*normalized_cols)

    if STAGING_TO_BUSINESS_COLUMN_MAP:
        rename_map = {
            k: v for k, v in STAGING_TO_BUSINESS_COLUMN_MAP.items()
            if k in df_staged.columns
        }
        for staging_name, business_name in rename_map.items():
            df_staged = df_staged.withColumnRenamed(staging_name, business_name)

    # src.extract reads this exact period table. The previous `stg_...` name
    # created a table that run_pipeline never consumed.
    table_name = (
        f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{prefix.lower()}_{month:02d}_{year}"
    )

    (df_staged.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name))

    saved_count = spark.table(table_name).count()
    if saved_count != row_count:
        raise PipelineError(
            f"Row count mismatch staging {table_name}: "
            f"source={row_count} saved={saved_count}"
        )

    log(
        f"Staged OK | file={path} | table={table_name} | "
        f"rows={row_count} | cols={len(normalized_cols)}"
    )
    return table_name, row_count


# COMMAND ----------

def validate_delta_period(expected_rows, year, month):
    """Validate the period written by src.main.run_pipeline.

    CEO Asistencia stores the load period in ANIO and NUM_MES. It does not
    contain TIME_ID.
    """
    from pyspark.sql import functions as F

    if not spark.catalog.tableExists(DELTA_TARGET_TABLE):
        raise PipelineError(f"Delta target does not exist: {DELTA_TARGET_TABLE}")

    target_df = spark.table(DELTA_TARGET_TABLE)
    required_period_columns = {YEAR_COLUMN, MONTH_COLUMN}
    missing_period_columns = required_period_columns - set(target_df.columns)
    if missing_period_columns:
        raise PipelineError(
            f"Delta target {DELTA_TARGET_TABLE} is missing period columns "
            f"{sorted(missing_period_columns)}. "
            f"Available columns: {target_df.columns}"
        )

    period_value = f"{year}{month:02d}"
    target_count = (
        target_df
        .filter(
            (F.col(YEAR_COLUMN).cast("int") == year)
            & (F.col(MONTH_COLUMN).cast("int") == month)
        )
        .count()
    )

    status = "PASS" if target_count == expected_rows else "FAIL"
    log(
        f"[VALIDATION] Delta period row_count {status} | "
        f"period={period_value} source={expected_rows} target={target_count}"
    )
    if target_count != expected_rows:
        raise PipelineError(
            f"Delta row count mismatch for {period_value}: "
            f"source={expected_rows}, target={target_count}"
        )


# COMMAND ----------

def promote_file(incoming_path, filename):
    target_path = f"{CURRENT_PATH}{filename}"
    try:
        dbutils.fs.ls(target_path)
        log(f"Existing canonical file found, removing before promotion: {target_path}")
        dbutils.fs.rm(target_path)
    except Exception:
        pass

    dbutils.fs.mv(incoming_path, target_path)
    log(f"Promoted {filename} -> {target_path}")


def clear_incoming_after_failure():
    """
    Delete every file directly inside incoming/ after any failed run.

    Leaving the rejected file in place would make the corrected re-upload land
    as FILE-1.csv, which DUPLICATE_SUFFIX_REGEX then rejects — so the next run
    would fail for the wrong reason. Wiping incoming/ keeps the retry clean.

    All output is logged at ERROR level so the full record of what was removed
    survives even when the job's log level is raised to ERROR — that list is the
    audit trail for the failed run.

    Safety guarantees:
    - incoming/ itself is never recursively removed.
    - current/ is never read, modified, or deleted.
    - each file is deleted individually.
    - cleanup errors are logged but never raised.
    - incoming/ is recreated defensively if it is missing.
    """
    removed_count = 0
    failed_items = []

    try:
        dbutils.fs.mkdirs(INCOMING_PATH)
    except Exception as cleanup_error:
        log(
            f"Cleanup could not access or create {INCOMING_PATH}: "
            f"{type(cleanup_error).__name__}: {cleanup_error}",
            level="error",
        )
        log(
            f"MANUAL ACTION REQUIRED: clear all files from {INCOMING_PATH} "
            "before the next upload.",
            level="error",
        )
        return

    try:
        incoming_items = dbutils.fs.ls(INCOMING_PATH)
    except Exception as cleanup_error:
        log(
            f"Cleanup could not list {INCOMING_PATH}: "
            f"{type(cleanup_error).__name__}: {cleanup_error}",
            level="error",
        )
        log(
            f"MANUAL ACTION REQUIRED: clear all files from {INCOMING_PATH} "
            "before the next upload.",
            level="error",
        )
        return

    files = [f for f in incoming_items if not f.name.endswith("/")]

    if not files:
        log("Cleanup: incoming is already empty; nothing to remove.", level="error")
        log("Cleanup total: 0 file(s) removed.", level="error")
        return

    for file_info in files:
        try:
            deleted = dbutils.fs.rm(file_info.path, recurse=False)

            if deleted is False:
                failed_items.append(file_info.name)
                log(f"Cleanup could not remove: {file_info.name}", level="error")
                continue

            removed_count += 1
            log(f"Cleanup removed: {file_info.name}", level="error")

        except Exception as cleanup_error:
            failed_items.append(file_info.name)
            log(
                f"Cleanup could not remove {file_info.name}: "
                f"{type(cleanup_error).__name__}: {cleanup_error}",
                level="error",
            )

    log(f"Cleanup total: {removed_count} file(s) removed.", level="error")

    if failed_items:
        log(
            f"MANUAL ACTION REQUIRED: clear all files from {INCOMING_PATH} "
            "before the next upload.",
            level="error",
        )


# COMMAND ----------

# dbutils.notebook.exit() works by RAISING an exception, so calling it inside the
# try block below would be caught by "except Exception" and reported as a failure.
# The empty-folder case therefore sets this flag and exits after the try block.
no_files_to_process = False

try:
    log("PHASE 1: setup and discovery")
    csv_files = list_incoming_csv_files()

    if csv_files is None:
        log("NO FILES TO PROCESS - incoming folder is empty. Exiting successfully.")
        no_files_to_process = True

    else:
        log(f"Found {len(csv_files)} csv file(s) in incoming: "
            f"{[f.name for f in csv_files]}")

        log("PHASE 2: filename and period validation")
        year, month, parsed_files = validate_filenames_and_period(csv_files)

        log("PHASE 3: Hive staging and staging row checks")
        staging_results = {}
        for pf in parsed_files:
            table_name, row_count = stage_csv_to_hive(
                pf["path"], pf["prefix"], year, month
            )
            staging_results[pf["prefix"]] = {"table": table_name, "rows": row_count}

        log("PHASE 4: transformations and idempotent target loads")
        from src.main import run_pipeline

        # run_pipeline performs transformation plus Delta and Snowflake writes.
        # Its return value is intentionally ignored; the current implementation
        # returns None after a successful load.
        run_pipeline(spark=spark, year=year, month=month, dry_run=False)

        log("PHASE 5: target validation and file promotion")
        expected_rows = sum(item["rows"] for item in staging_results.values())
        validate_delta_period(expected_rows, year, month)

        for pf in parsed_files:
            promote_file(pf["path"], pf["name"])

        log(f"Pipeline completed successfully | period={year}-{month:02d} | "
            "incoming is now empty.")

except Exception as e:
    # Required log order:
    # 1. Original failure
    # 2. Removed files and total
    # 3. Explicit next step
    log(f"Pipeline FAILED: {type(e).__name__}: {e}", level="error")

    # This routine is designed never to raise or hide the original exception.
    clear_incoming_after_failure()

    log(
        "NEXT STEP: correct the source file and re-upload it with its original "
        "canonical name.",
        level="error",
    )

    # Preserve the original exception and traceback so the Databricks Job fails.
    raise

# Exit AFTER the try block: notebook.exit() raises, and must not be caught above.
if no_files_to_process:
    dbutils.notebook.exit("NO FILES TO PROCESS")


# COMMAND ----------

