# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC ## ETL Pipeline: CEO Asistencia

# COMMAND ----------
# 1. Define Widgets
dbutils.widgets.text("year", "2026", "Year (YYYY)")
dbutils.widgets.text("month", "06", "Month (MM)")
dbutils.widgets.dropdown("dry_run", "Yes", ["Yes", "No"], "Dry Run")

# COMMAND ----------
# 2. Fetch Widget Values
target_year = int(dbutils.widgets.get("year"))
target_month = int(dbutils.widgets.get("month"))
is_dry_run = True if dbutils.widgets.get("dry_run") == "Yes" else False

# COMMAND ----------
# 3. Import and Run Main Orchestrator
from src.main import run_pipeline

run_pipeline(
    spark=spark, 
    year=target_year, 
    month=target_month, 
    dry_run=is_dry_run
)