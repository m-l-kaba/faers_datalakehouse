# Silver layer transformation for therapy dates data
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
logger = setup_logger("silver_therapy_dates")


def transform_therapy_dates(spark) -> DataFrame:
    """
    Transform bronze therapy dates data to silver layer with:
    - Date parsing and standardization
    - Duration calculation
    - Therapy status classification
    - Only processes latest partition
    """
    logger.info("Starting therapy dates silver transformation")

    df_bronze = read_latest_partition(spark, "therapy_dates")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze therapy_dates partition"
    )

    date_fields = ["start_dt", "end_dt"]
    df_silver = standardize_date_fields(df_bronze, date_fields)
    logger.info("Standardized date fields")

    numeric_fields = ["dsg_drug_seq", "dur"]
    df_silver = standardize_numeric_fields(df_silver, numeric_fields)
    logger.info("Standardized numeric fields")

    df_silver = df_silver.withColumn(
        "duration_description",
        F.when(F.col("dur_cod") == "YR", "Years")
        .when(F.col("dur_cod") == "MON", "Months")
        .when(F.col("dur_cod") == "WK", "Weeks")
        .when(F.col("dur_cod") == "DY", "Days")
        .when(F.col("dur_cod") == "HR", "Hours")
        .when(F.col("dur_cod") == "MIN", "Minutes")
        .otherwise("Unknown"),
    )

    df_silver = df_silver.withColumn(
        "therapy_duration_days",
        F.when(
            (F.col("start_dt").isNotNull()) & (F.col("end_dt_parsed").isNotNull()),
            F.datediff(F.col("end_dt_parsed"), F.col("start_dt")),
        ).otherwise(None),
    )

    df_silver = df_silver.withColumn(
        "reported_duration_days",
        F.when(
            (F.col("dur_numeric").isNotNull()) & (F.col("dur_cod") == "YR"),
            F.col("dur_numeric") * 365,
        )
        .when(
            (F.col("dur_numeric").isNotNull()) & (F.col("dur_cod") == "MON"),
            F.col("dur_numeric") * 30,
        )
        .when(
            (F.col("dur_numeric").isNotNull()) & (F.col("dur_cod") == "WK"),
            F.col("dur_numeric") * 7,
        )
        .when(
            (F.col("dur_numeric").isNotNull()) & (F.col("dur_cod") == "DY"),
            F.col("dur_numeric"),
        )
        .when(
            (F.col("dur_numeric").isNotNull()) & (F.col("dur_cod") == "HR"),
            F.col("dur_numeric") / 24,
        )
        .otherwise(None),
    )

    df_silver = df_silver.withColumn(
        "therapy_status",
        F.when(
            (F.col("start_dt").isNotNull()) & (F.col("end_dt_parsed").isNotNull()),
            "Completed",
        )
        .when(
            (F.col("start_dt").isNotNull()) & (F.col("end_dt_parsed").isNull()),
            "Ongoing",
        )
        .when(F.col("start_dt").isNull(), "Unknown Start")
        .otherwise("Unknown"),
    )

    df_silver = df_silver.withColumn(
        "duration_category",
        F.when(F.col("therapy_duration_days") <= 7, "Short-term (≤1 week)")
        .when(
            (F.col("therapy_duration_days") > 7)
            & (F.col("therapy_duration_days") <= 30),
            "Medium-term (1-4 weeks)",
        )
        .when(
            (F.col("therapy_duration_days") > 30)
            & (F.col("therapy_duration_days") <= 90),
            "Long-term (1-3 months)",
        )
        .when(F.col("therapy_duration_days") > 90, "Extended (>3 months)")
        .otherwise("Unknown Duration"),
    )

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(
        f"Completed therapy dates transformation with {df_silver.count()} records"
    )
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog)

    df_therapy_dates_silver = transform_therapy_dates(spark)

    logger.info("Writing therapy dates silver data")

    df_therapy_dates_silver.write.mode("overwrite").saveAsTable("silver.therapy_dates")

    spark.sql("OPTIMIZE silver.therapy_dates")

    logger.info("Therapy dates silver transformation complete")
