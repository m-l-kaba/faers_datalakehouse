# Silver layer transformation for outcomes data
import os
import sys

sys.path.append("../")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job
from utils.logger import setup_logger
from utils.silver_transformations import (
    add_silver_metadata,
    read_latest_partition,
)

config = load_config()
target_catalog = os.getenv("target_catalog")
logger = setup_logger("silver_outcomes")


def transform_outcomes(spark) -> DataFrame:
    """
    Transform bronze outcomes data to silver layer with:
    - Outcome code standardization
    - Severity ranking
    - Only processes latest partition
    """
    logger.info("Starting outcomes silver transformation")

    df_bronze = read_latest_partition(spark, "outcomes")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze outcomes partition"
    )

    df_silver = df_bronze.withColumn(
        "outcome_description",
        F.when(F.col("outc_cod") == "DE", "Death")
        .when(F.col("outc_cod") == "LT", "Life-threatening")
        .when(F.col("outc_cod") == "HO", "Hospitalization - initial or prolonged")
        .when(F.col("outc_cod") == "DS", "Disability")
        .when(F.col("outc_cod") == "CA", "Congenital anomaly")
        .when(
            F.col("outc_cod") == "RI",
            "Required intervention to prevent permanent impairment/damage",
        )
        .when(F.col("outc_cod") == "OT", "Other serious (important medical events)")
        .otherwise("Unknown"),
    )

    df_silver = df_silver.withColumn(
        "outcome_severity_rank",
        F.when(F.col("outc_cod") == "DE", 7)  # Death - most severe
        .when(F.col("outc_cod") == "LT", 6)  # Life-threatening
        .when(F.col("outc_cod") == "CA", 5)  # Congenital anomaly
        .when(F.col("outc_cod") == "DS", 4)  # Disability
        .when(F.col("outc_cod") == "HO", 3)  # Hospitalization
        .when(F.col("outc_cod") == "RI", 2)  # Required intervention
        .when(F.col("outc_cod") == "OT", 1)  # Other serious
        .otherwise(0),
    )

    df_silver = df_silver.withColumn(
        "outcome_category",
        F.when(F.col("outc_cod") == "DE", "Fatal")
        .when(F.col("outc_cod").isin("LT", "CA", "DS"), "Serious Non-Fatal")
        .when(F.col("outc_cod").isin("HO", "RI"), "Medically Significant")
        .when(F.col("outc_cod") == "OT", "Other Serious")
        .otherwise("Unknown"),
    )

    df_silver = df_silver.withColumn(
        "expedited_reporting_required",
        F.when(
            F.col("outc_cod").isin("DE", "LT", "HO", "DS", "CA", "RI"), True
        ).otherwise(False),
    )

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(f"Completed outcomes transformation with {df_silver.count()} records")
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "silver")

    df_outcomes_silver = transform_outcomes(spark)

    logger.info("Writing outcomes silver data")

    df_outcomes_silver.write.mode("overwrite").saveAsTable("outcomes_silver")

    spark.sql("OPTIMIZE outcomes_silver")

    logger.info("Outcomes silver transformation complete")
