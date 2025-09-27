# Silver layer transformation for demographics data
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
    calculate_age_groups,
    add_silver_metadata,
    read_latest_partition,
)

config = load_config()
target_catalog = os.getenv("target_catalog")
logger = setup_logger("silver_demographics")


def transform_demographics(spark) -> DataFrame:
    """
    Transform bronze demographics data to silver layer with:
    - Date standardization and validation
    - Numeric field conversion
    - Age group calculation
    - Only processes latest partition
    """
    logger.info("Starting demographics silver transformation")

    # Read only the latest partition
    df_bronze = read_latest_partition(spark, "demographics")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze demographics partition"
    )

    date_fields = ["event_dt", "init_fda_dt", "mfr_dt", "fda_dt", "rept_dt"]
    df_silver = standardize_date_fields(df_bronze, date_fields)
    logger.info("Standardized date fields")

    numeric_fields = ["age", "wt"]
    df_silver = standardize_numeric_fields(df_silver, numeric_fields)
    logger.info("Standardized numeric fields")

    df_silver = calculate_age_groups(df_silver, "age")
    logger.info("Calculated age groups")

    df_silver = df_silver.withColumn(
        "age_category",
        F.when(F.col("age_cod") == "YR", "Years")
        .when(F.col("age_cod") == "MON", "Months")
        .when(F.col("age_cod") == "WK", "Weeks")
        .when(F.col("age_cod") == "DY", "Days")
        .when(F.col("age_cod") == "HR", "Hours")
        .otherwise("Unknown"),
    ).drop("age_cod")

    df_silver = df_silver.withColumn(
        "weight_category",
        F.when(F.col("wt_cod") == "KG", "Kilograms")
        .when(F.col("wt_cod") == "LB", "Pounds")
        .otherwise("Unknown"),
    ).drop("wt_cod")

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(
        f"Completed demographics transformation with {df_silver.count()} records"
    )
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "silver")

    df_demographics_silver = transform_demographics(spark)

    logger.info("Writing demographics silver data")

    df_demographics_silver.write.mode("overwrite").saveAsTable("demographics")

    spark.sql("OPTIMIZE demographics_silver")

    logger.info("Demographics silver transformation complete")
