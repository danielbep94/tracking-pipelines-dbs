"""CEO Asistencia normalization, attendance derivation, and AREA enrichment."""

import re
import unicodedata

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

from conf.settings import FINAL_COLUMNS
from logs.logger import get_logger


logger = get_logger("Transform")

REQUIRED_SOURCE_COLUMNS = {
    "ID",
    "EMPRESA",
    "NUMERO_EMPLEADO",
    "PATERNO",
    "MATERNO",
    "NOMBRE",
    "FECHA",
    "REGISTRO_1",
    "REGISTRO_2",
}


def normalize_column_name(name: str) -> str:
    """Return an uppercase, accent-free, underscore-delimited column name."""
    decomposed = unicodedata.normalize("NFKD", str(name))
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", ascii_only)
    return re.sub(r"_+", "_", cleaned).strip("_").upper()


def _normalize_layout(dataframe):
    normalized = [normalize_column_name(name) for name in dataframe.columns]
    duplicates = sorted(
        {name for name in normalized if normalized.count(name) > 1}
    )
    if duplicates:
        raise ValueError(
            f"Column normalization produced duplicate columns: {duplicates}"
        )
    dataframe = dataframe.toDF(*normalized)
    if "NUM_EMPLEADO" in dataframe.columns and "NUMERO_EMPLEADO" in dataframe.columns:
        raise ValueError(
            "Source contains both NUM_EMPLEADO and NUMERO_EMPLEADO."
        )
    if "NUM_EMPLEADO" in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(
            "NUM_EMPLEADO",
            "NUMERO_EMPLEADO",
        )
    missing = sorted(REQUIRED_SOURCE_COLUMNS - set(dataframe.columns))
    if missing:
        raise ValueError(f"Required source columns are missing: {missing}")
    if "AREA" not in dataframe.columns:
        dataframe = dataframe.withColumn("AREA", F.lit(None).cast(StringType()))
    return dataframe


def clean_and_transform(dataframe, reference_dataframe):
    """Apply the 16-column attendance output contract without dropping rows."""
    logger.info("Starting CEO Asistencia transformations")
    dataframe = _normalize_layout(dataframe)

    dataframe = dataframe.withColumn(
        "FECHA_CLEAN",
        F.coalesce(
            F.to_date(F.col("FECHA"), "dd/MM/yyyy"),
            F.to_date(F.col("FECHA"), "yyyy-MM-dd"),
        ),
    )
    dataframe = (
        dataframe.withColumn("ANIO", F.year(F.col("FECHA_CLEAN")))
        .withColumn("NUM_MES", F.month(F.col("FECHA_CLEAN")))
        .withColumn(
            "NUMERO_EMPLEADO",
            F.lpad(
                F.col("NUMERO_EMPLEADO").cast(IntegerType()).cast("string"),
                8,
                "0",
            ),
        )
        .withColumn("ID", F.col("ID").cast("string"))
        .withColumn("WEEK_NUMBER", F.weekofyear(F.col("FECHA_CLEAN")))
        .withColumn("DAY", F.date_format(F.col("FECHA_CLEAN"), "EEEE"))
    )

    dataframe = (
        dataframe.withColumn("REG1_TS", F.to_timestamp(F.col("REGISTRO_1")))
        .withColumn("REG2_TS", F.to_timestamp(F.col("REGISTRO_2")))
        .withColumn(
            "RESULTADO",
            F.when(
                F.col("REG1_TS").isNotNull()
                & F.col("REG2_TS").isNotNull()
                & (F.hour(F.col("REG1_TS")) >= 6),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "DIFERENCIA_HORAS",
            F.when(
                F.col("RESULTADO") == 1,
                F.round(
                    (
                        F.unix_timestamp("REG2_TS")
                        - F.unix_timestamp("REG1_TS")
                    )
                    / 3600,
                    2,
                ),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
    )

    reference_clean = (
        reference_dataframe.withColumn(
            "NUMERO_EMPLEADO",
            F.lpad(
                F.col("NUMERO_EMPLEADO").cast(IntegerType()).cast("string"),
                8,
                "0",
            ),
        )
        .withColumn("ID", F.col("ID").cast("string"))
        .select(
            "NUMERO_EMPLEADO",
            "ID",
            F.col("AREA").alias("REFERENCE_AREA"),
        )
        .dropDuplicates(["NUMERO_EMPLEADO", "ID"])
    )
    dataframe = dataframe.join(
        reference_clean,
        on=["NUMERO_EMPLEADO", "ID"],
        how="left",
    )
    source_area = F.when(
        F.trim(F.col("AREA").cast("string")) == "",
        F.lit(None),
    ).otherwise(F.col("AREA"))
    dataframe = dataframe.withColumn(
        "AREA",
        F.coalesce(source_area, F.col("REFERENCE_AREA")),
    )

    return (
        dataframe.withColumn("FECHA", F.col("FECHA_CLEAN"))
        .select(*[F.col(column_name) for column_name in FINAL_COLUMNS])
    )
