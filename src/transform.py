# src/transform.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from logs.logger import get_logger
from conf.settings import FINAL_COLUMNS

logger = get_logger(__name__)

def clean_and_transform(df: DataFrame, df_ref: DataFrame, target_year: int, target_month: int) -> DataFrame:
    logger.info("Starting transformations")
    
    # 1. Clean column names
    df = df.select([F.col(c).alias(c.strip().upper().replace(" ", "_")) for c in df.columns])
    if "NUM_EMPLEADO" in df.columns:
        df = df.withColumnRenamed("NUM_EMPLEADO", "NUMERO_EMPLEADO")

    # 2. Date parsing (Handles multiple formats using coalesce)
    df = df.withColumn(
        "FECHA_CLEAN",
        F.coalesce(
            F.to_date(F.col("FECHA"), "dd/MM/yyyy"),
            F.to_date(F.col("FECHA"), "yyyy-MM-dd")
        )
    )

    # 3. Filter early to target period
    df = df.withColumn("ANIO", F.year(F.col("FECHA_CLEAN"))) \
           .withColumn("NUM_MES", F.month(F.col("FECHA_CLEAN")))
           
    df = df.filter((F.col("ANIO") == target_year) & (F.col("NUM_MES") == target_month))

    # 4. Standardize strings and formats
    df = df.withColumn("NUMERO_EMPLEADO", F.lpad(F.col("NUMERO_EMPLEADO").cast(IntegerType()).cast("string"), 8, "0")) \
           .withColumn("ID", F.col("ID").cast("string")) \
           .withColumn("WEEK_NUMBER", F.weekofyear(F.col("FECHA_CLEAN"))) \
           .withColumn("DAY", F.date_format(F.col("FECHA_CLEAN"), "EEEE")) 

    # 5. Handle Times (Assuming REGISTRO_1 and REGISTRO_2 are datetime strings)
    df = df.withColumn("REG1_TS", F.to_timestamp(F.col("REGISTRO_1"))) \
           .withColumn("REG2_TS", F.to_timestamp(F.col("REGISTRO_2")))

    # 6. Calculate RESULTADO and DIFERENCIA_HORAS
    df = df.withColumn(
        "RESULTADO",
        F.when(
            F.col("REG1_TS").isNotNull() & 
            F.col("REG2_TS").isNotNull() & 
            (F.hour(F.col("REG1_TS")) >= 6), 
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "DIFERENCIA_HORAS",
        F.when(
            F.col("RESULTADO") == 1,
            F.round((F.unix_timestamp("REG2_TS") - F.unix_timestamp("REG1_TS")) / 3600, 2)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )

    # 7. Enrich AREA from Snowflake reference
    df_ref_clean = df_ref.withColumn("NUMERO_EMPLEADO", F.lpad(F.col("NUMERO_EMPLEADO").cast(IntegerType()).cast("string"), 8, "0")) \
                         .withColumn("ID", F.col("ID").cast("string")) \
                         .dropDuplicates(["NUMERO_EMPLEADO", "ID"])

    df = df.alias("a").join(
        df_ref_clean.alias("b"),
        on=["NUMERO_EMPLEADO", "ID"],
        how="left"
    )
    
    if "AREA" in df.columns and "b.AREA" in df.columns:
        df = df.withColumn("FINAL_AREA", F.coalesce(F.col("a.AREA"), F.col("b.AREA")))
    else:
        df = df.withColumn("FINAL_AREA", F.col("b.AREA"))
    
    df = df.withColumn("AREA", F.col("FINAL_AREA"))

    # 8. Final select and cast
    df_final = df.withColumn("FECHA", F.col("FECHA_CLEAN")) \
                 .select(*[F.col(c) for c in FINAL_COLUMNS])

    return df_final