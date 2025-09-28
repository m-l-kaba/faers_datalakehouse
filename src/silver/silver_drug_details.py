# Silver layer transformation for drug details data
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
    standardize_date_fields,
    standardize_numeric_fields,
    add_silver_metadata,
    read_latest_partition,
)

config = load_config()
target_catalog = os.getenv("target_catalog")
logger = setup_logger("silver_drug_details")


def transform_drug_details(spark) -> DataFrame:
    """
    Transform bronze drug details data to silver layer with:
    - Date standardization and validation
    - Numeric field conversion
    - Role code standardization
    - Only processes latest partition
    """
    logger.info("Starting drug details silver transformation")

    df_bronze = read_latest_partition(spark, "drug_details")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze drug_details partition"
    )

    date_fields = ["exp_dt"]
    df_silver = standardize_date_fields(df_bronze, date_fields)
    logger.info("Standardized date fields")

    numeric_fields = ["drug_seq", "val_vbm", "dose_amt", "nda_num"]
    df_silver = standardize_numeric_fields(df_silver, numeric_fields)
    logger.info("Standardized numeric fields")

    df_silver = df_silver.withColumn(
        "role_description",
        F.when(F.col("role_cod") == "PS", "Primary Suspect")
        .when(F.col("role_cod") == "SS", "Secondary Suspect")
        .when(F.col("role_cod") == "C", "Concomitant")
        .when(F.col("role_cod") == "I", "Interacting")
        .otherwise("Unknown"),
    )

    df_silver = df_silver.withColumn(
        "route_standardized",
        F.when(F.upper(F.col("route")).contains("ORAL"), "Oral")
        .when(F.upper(F.col("route")).contains("INTRAVENOUS"), "Intravenous")
        .when(F.upper(F.col("route")).contains("INTRAMUSCULAR"), "Intramuscular")
        .when(F.upper(F.col("route")).contains("SUBCUTANEOUS"), "Subcutaneous")
        .when(F.upper(F.col("route")).contains("TOPICAL"), "Topical")
        .when(F.upper(F.col("route")).contains("INHALATION"), "Inhalation")
        .when(F.upper(F.col("route")).contains("UNKNOWN"), "Unknown")
        .otherwise(F.initcap(F.col("route"))),
    )

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(
        f"Completed drug details transformation with {df_silver.count()} records"
    )
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog)

    df_drug_details_silver = transform_drug_details(spark)

    logger.info("Writing drug details silver data")

    df_drug_details_silver.write.mode("overwrite").saveAsTable("silver.drug_details")

    spark.sql("OPTIMIZE silver.drug_details")

    logger.info("Drug details silver transformation complete")
