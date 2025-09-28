# Silver layer transformation for reports data
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
logger = setup_logger("silver_reports")


def transform_reports(spark) -> DataFrame:
    """
    Transform bronze reports data to silver layer with:
    - Reporter source standardization
    - Reporter type categorization
    - Only processes latest partition
    """
    logger.info("Starting reports silver transformation")

    df_bronze = read_latest_partition(spark, "reports")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze reports partition"
    )

    df_silver = df_bronze.withColumn(
        "reporter_source_description",
        F.when(F.col("rpsr_cod") == "HP", "Healthcare Professional")
        .when(F.col("rpsr_cod") == "CSM", "Consumer/Patient")
        .when(F.col("rpsr_cod") == "LW", "Lawyer")
        .when(F.col("rpsr_cod") == "OTH", "Other")
        .when(F.col("rpsr_cod") == "UNK", "Unknown")
        .otherwise("Unspecified"),
    )

    df_silver = df_silver.withColumn(
        "reporter_category",
        F.when(F.col("rpsr_cod") == "HP", "Professional")
        .when(F.col("rpsr_cod") == "CSM", "Consumer")
        .when(F.col("rpsr_cod").isin("LW", "OTH"), "Other Professional")
        .otherwise("Unknown"),
    )

    # Add reporter reliability score (HP reports generally considered more reliable)
    df_silver = df_silver.withColumn(
        "reporter_reliability_score",
        F.when(
            F.col("rpsr_cod") == "HP", 5
        )  # Healthcare professionals - highest reliability
        .when(F.col("rpsr_cod") == "LW", 4)  # Lawyers - high reliability
        .when(F.col("rpsr_cod") == "CSM", 3)  # Consumers - moderate reliability
        .when(F.col("rpsr_cod") == "OTH", 2)  # Other - low reliability
        .otherwise(1),  # Unknown - lowest reliability
    )

    df_silver = df_silver.withColumn(
        "regulatory_priority",
        F.when(F.col("rpsr_cod") == "HP", "High")
        .when(F.col("rpsr_cod") == "LW", "Medium")
        .otherwise("Standard"),
    )

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(f"Completed reports transformation with {df_silver.count()} records")
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog)

    df_reports_silver = transform_reports(spark)

    logger.info("Writing reports silver data")

    df_reports_silver.write.mode("overwrite").saveAsTable("silver.reports")

    spark.sql("OPTIMIZE silver.reports")

    logger.info("Reports silver transformation complete")
