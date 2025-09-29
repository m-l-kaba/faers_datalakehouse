# Therapy dimension for FAERS gold layer with SCD Type 2
import os
import sys

sys.path.append("../../")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job
from utils.logger import setup_logger
from utils.scd_type2 import apply_scd_type2_merge

config = load_config("../../config.yml")
target_catalog = os.getenv("target_catalog")
logger = setup_logger("dim_therapy")


def create_therapy_dimension(spark) -> DataFrame:
    """
    Create therapy dimension from silver therapy dates data.

    This dimension tracks changes in therapy duration and treatment patterns using SCD Type 2:
    - Therapy identity (primary_id, dsg_drug_seq) as business keys
    - Slowly changing attributes: duration, therapy status, treatment patterns
    - Derived attributes: therapy classifications, compliance indicators

    Returns:
        DataFrame with therapy dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating therapy dimension from silver therapy durations")

    # Read current silver therapy durations data
    df_therapy = spark.table("silver.therapy_dates")
    logger.info(f"Read {df_therapy.count()} records from silver therapy durations")

    # Create therapy dimension with business keys and tracked attributes
    dim_therapy = df_therapy.select(
        # Business keys - uniquely identify a therapy in a report
        F.col("primary_id").alias("therapy_report_id"),
        F.col("dsg_drug_seq").alias("drug_sequence_key"),
        # Core therapy timing information (slowly changing)
        F.col("start_dt").alias("therapy_start_date"),
        F.col("end_dt").alias("therapy_end_date"),
        F.col("therapy_duration_days").alias("therapy_duration_days"),
        F.col("reported_duration_days").alias("reported_duration_days"),
        F.col("therapy_status").alias("therapy_status"),
        F.col("duration_category").alias("duration_category"),
        # Duration coding information
        F.col("dur").alias("duration_value"),
        F.col("dur_cod").alias("duration_code"),
        F.col("duration_description").alias("duration_unit_description"),
        # Metadata for lineage
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    # Add therapy duration classifications
    dim_therapy = (
        dim_therapy.withColumn(
            "is_short_term_therapy",
            F.when(F.col("therapy_duration_days") <= 30, True).otherwise(False),
        )
        .withColumn(
            "is_long_term_therapy",
            F.when(F.col("therapy_duration_days") > 90, True).otherwise(False),
        )
        .withColumn(
            "is_chronic_therapy",
            F.when(F.col("therapy_duration_days") > 180, True).otherwise(False),
        )
        .withColumn(
            "is_completed_therapy",
            F.when(F.col("therapy_status") == "Completed", True).otherwise(False),
        )
        .withColumn(
            "is_ongoing_therapy",
            F.when(F.col("therapy_status") == "Ongoing", True).otherwise(False),
        )
    )

    # Add duration validation and quality indicators
    dim_therapy = (
        dim_therapy.withColumn(
            "has_complete_dates",
            F.when(
                (F.col("therapy_start_date").isNotNull())
                & (F.col("therapy_end_date").isNotNull()),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "has_duration_data",
            F.when(F.col("duration_value").isNotNull(), True).otherwise(False),
        )
        .withColumn(
            "duration_data_quality",
            F.when(F.col("has_complete_dates") & F.col("has_duration_data"), "High")
            .when(F.col("has_complete_dates") | F.col("has_duration_data"), "Medium")
            .otherwise("Low"),
        )
    )

    # Calculate therapy timing metrics
    current_date_col = F.current_date()
    dim_therapy = dim_therapy.withColumn(
        "days_since_start",
        F.when(
            F.col("therapy_start_date").isNotNull(),
            F.datediff(current_date_col, F.col("therapy_start_date")),
        ).otherwise(None),
    ).withColumn(
        "days_since_end",
        F.when(
            F.col("therapy_end_date").isNotNull(),
            F.datediff(current_date_col, F.col("therapy_end_date")),
        ).otherwise(None),
    )

    # Add therapy period classifications
    dim_therapy = dim_therapy.withColumn(
        "therapy_period",
        F.when(F.col("therapy_duration_days") <= 1, "Single Dose")
        .when(F.col("therapy_duration_days").between(2, 7), "Weekly Treatment")
        .when(F.col("therapy_duration_days").between(8, 30), "Monthly Treatment")
        .when(F.col("therapy_duration_days").between(31, 90), "Quarterly Treatment")
        .when(F.col("therapy_duration_days").between(91, 180), "Semi-Annual Treatment")
        .when(F.col("therapy_duration_days") > 180, "Long-term Treatment")
        .otherwise("Unknown Duration"),
    )

    # Add compliance and adherence indicators
    dim_therapy = dim_therapy.withColumn(
        "potential_compliance_issue",
        F.when(
            (F.col("therapy_status") == "Unknown Start")
            | (F.col("duration_data_quality") == "Low"),
            True,
        ).otherwise(False),
    ).withColumn(
        "therapy_interruption_risk",
        F.when(
            (F.col("is_ongoing_therapy")) & (F.col("days_since_start") > 365), True
        ).otherwise(False),
    )

    # Add therapeutic monitoring requirements
    dim_therapy = dim_therapy.withColumn(
        "monitoring_frequency",
        F.when(F.col("therapy_period") == "Single Dose", "None Required")
        .when(F.col("therapy_period") == "Weekly Treatment", "Weekly")
        .when(F.col("therapy_period") == "Monthly Treatment", "Bi-weekly")
        .when(F.col("therapy_period") == "Quarterly Treatment", "Monthly")
        .when(F.col("therapy_period").contains("Long-term"), "Quarterly")
        .otherwise("As Needed"),
    )

    # Add duration unit standardization
    dim_therapy = dim_therapy.withColumn(
        "duration_in_hours",
        F.when(F.col("duration_code") == "HR", F.col("duration_value"))
        .when(F.col("duration_code") == "DY", F.col("duration_value") * 24)
        .when(F.col("duration_code") == "WK", F.col("duration_value") * 24 * 7)
        .when(F.col("duration_code") == "MON", F.col("duration_value") * 24 * 30)
        .when(F.col("duration_code") == "YR", F.col("duration_value") * 24 * 365)
        .otherwise(None),
    ).withColumn(
        "duration_in_weeks",
        F.when(
            F.col("duration_in_hours").isNotNull(),
            F.col("duration_in_hours") / (24 * 7),
        ).otherwise(None),
    )

    # Add therapy safety indicators
    dim_therapy = dim_therapy.withColumn(
        "extended_therapy_flag",
        F.when(F.col("duration_in_weeks") > 52, True).otherwise(False),  # > 1 year
    ).withColumn(
        "intensive_therapy_flag",
        F.when(F.col("therapy_period") == "Single Dose", False)
        .when(F.col("therapy_period") == "Weekly Treatment", True)
        .otherwise(False),
    )

    # Add business value indicators
    dim_therapy = dim_therapy.withColumn(
        "therapy_value_category",
        F.when(F.col("is_chronic_therapy"), "High Value - Chronic Care")
        .when(F.col("is_long_term_therapy"), "Medium Value - Extended Care")
        .when(F.col("is_short_term_therapy"), "Standard Value - Acute Care")
        .otherwise("Unknown Value"),
    )

    logger.info(
        f"Created therapy dimension with {dim_therapy.count()} unique therapy records"
    )
    return dim_therapy


def process_therapy_dimension(spark) -> None:
    """
    Process the therapy dimension using SCD Type 2 logic.
    """
    logger.info("Starting therapy dimension SCD Type 2 processing")

    # Create the dimension data
    new_therapy_data = create_therapy_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["therapy_report_id", "drug_sequence_key"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "therapy_start_date",
        "therapy_end_date",
        "therapy_duration_days",
        "reported_duration_days",
        "therapy_status",
        "duration_category",
        "duration_value",
        "duration_code",
        "duration_unit_description",
        "is_short_term_therapy",
        "is_long_term_therapy",
        "is_chronic_therapy",
        "is_completed_therapy",
        "is_ongoing_therapy",
        "has_complete_dates",
        "has_duration_data",
        "duration_data_quality",
        "therapy_period",
        "potential_compliance_issue",
        "therapy_interruption_risk",
        "monitoring_frequency",
        "duration_in_hours",
        "duration_in_weeks",
        "extended_therapy_flag",
        "intensive_therapy_flag",
        "therapy_value_category",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_therapy",
        new_data=new_therapy_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="therapy_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_therapy")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_therapy COMPUTE STATISTICS")

    logger.info("Therapy dimension SCD Type 2 processing complete")


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the therapy dimension
    process_therapy_dimension(spark)

    logger.info("Therapy dimension processing complete")
