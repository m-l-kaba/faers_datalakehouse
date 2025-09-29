# Report dimension for FAERS gold layer with SCD Type 2
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
logger = setup_logger("dim_report")


def create_report_dimension(spark) -> DataFrame:
    """
    Create report dimension from silver reports data.

    This dimension tracks changes in report metadata using SCD Type 2:
    - Report identity (primary_id, caseid) as business keys
    - Slowly changing attributes: reporter info, regulatory classifications
    - Derived attributes: priority scores, categorizations

    Returns:
        DataFrame with report dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating report dimension from silver reports")

    # Read current silver reports data
    df_reports = spark.table("silver.reports")
    logger.info(f"Read {df_reports.count()} records from silver reports")

    # Create report dimension with business keys and tracked attributes
    dim_report = df_reports.select(
        # Business keys - uniquely identify a report
        F.col("primary_id").alias("report_primary_id"),
        F.col("caseid").alias("report_case_id"),
        # Reporter information (slowly changing)
        F.col("rpsr_cod").alias("reporter_source_code"),
        F.col("reporter_source_description"),
        F.col("reporter_category"),
        F.col("reporter_reliability_score"),
        F.col("regulatory_priority"),
        # Metadata for lineage
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    # Add enhanced reporter classifications
    dim_report = (
        dim_report.withColumn(
            "is_healthcare_professional",
            F.when(F.col("reporter_source_code") == "HP", True).otherwise(False),
        )
        .withColumn(
            "is_consumer_report",
            F.when(F.col("reporter_source_code") == "CSM", True).otherwise(False),
        )
        .withColumn(
            "is_legal_report",
            F.when(F.col("reporter_source_code") == "LW", True).otherwise(False),
        )
    )

    # Add regulatory follow-up indicators
    dim_report = dim_report.withColumn(
        "requires_followup",
        F.when(F.col("regulatory_priority") == "High", True).otherwise(False),
    ).withColumn(
        "regulatory_review_level",
        F.when(F.col("reporter_reliability_score") >= 4, "Priority")
        .when(F.col("reporter_reliability_score") == 3, "Standard")
        .otherwise("Low Priority"),
    )

    # Add report quality score based on reporter type
    dim_report = dim_report.withColumn(
        "report_quality_score",
        F.when(F.col("is_healthcare_professional"), 10)
        .when(F.col("is_legal_report"), 8)
        .when(F.col("is_consumer_report"), 6)
        .otherwise(4),
    )

    # Add report source categorization for analytics
    dim_report = dim_report.withColumn(
        "report_source_tier",
        F.when(F.col("reporter_reliability_score") >= 4, "Tier 1 - High Reliability")
        .when(F.col("reporter_reliability_score") == 3, "Tier 2 - Moderate Reliability")
        .otherwise("Tier 3 - Low Reliability"),
    )

    logger.info(
        f"Created report dimension with {dim_report.count()} unique report records"
    )
    return dim_report


def process_report_dimension(spark) -> None:
    """
    Process the report dimension using SCD Type 2 logic.
    """
    logger.info("Starting report dimension SCD Type 2 processing")

    # Create the dimension data
    new_report_data = create_report_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["report_primary_id", "report_case_id"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "reporter_source_code",
        "reporter_source_description",
        "reporter_category",
        "reporter_reliability_score",
        "regulatory_priority",
        "is_healthcare_professional",
        "is_consumer_report",
        "is_legal_report",
        "requires_followup",
        "regulatory_review_level",
        "report_quality_score",
        "report_source_tier",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_report",
        new_data=new_report_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="report_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_report")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_report COMPUTE STATISTICS")

    logger.info("Report dimension SCD Type 2 processing complete")


def get_current_reports(spark) -> DataFrame:
    """
    Get current report dimension records for analysis.
    """
    from utils.scd_type2 import get_current_records

    return get_current_records(spark, "gold.dim_report")


def get_report_history(spark, report_primary_id: str, report_case_id: str) -> DataFrame:
    """
    Get the complete change history for a specific report.
    """
    from utils.scd_type2 import get_change_history

    return get_change_history(
        spark,
        "gold.dim_report",
        ["report_primary_id", "report_case_id"],
        [report_primary_id, report_case_id],
    )


def analyze_report_patterns(spark) -> None:
    """
    Analyze patterns in the report dimension data.
    """
    logger.info("=== Report Dimension Analysis ===")
    current_reports = get_current_reports(spark)

    # Basic statistics
    total_reports = current_reports.count()
    logger.info(f"Total current report records: {total_reports}")

    # Reporter source distribution
    source_dist = (
        current_reports.groupBy("reporter_source_description")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Reporter source distribution:")
    source_dist.show()

    # Reporter category distribution
    category_dist = (
        current_reports.groupBy("reporter_category")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Reporter category distribution:")
    category_dist.show()

    # Regulatory priority distribution
    priority_dist = (
        current_reports.groupBy("regulatory_priority")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Regulatory priority distribution:")
    priority_dist.show()

    # Report quality metrics
    quality_metrics = current_reports.select(
        F.avg("reporter_reliability_score").alias("avg_reliability_score"),
        F.avg("report_quality_score").alias("avg_quality_score"),
        F.avg(F.col("is_healthcare_professional").cast("int")).alias(
            "pct_healthcare_professional"
        ),
        F.avg(F.col("requires_followup").cast("int")).alias("pct_requires_followup"),
    )
    logger.info("Report quality metrics:")
    quality_metrics.show()

    # Source tier distribution
    tier_dist = (
        current_reports.groupBy("report_source_tier")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Report source tier distribution:")
    tier_dist.show()


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the report dimension
    process_report_dimension(spark)

    logger.info("Report dimension processing complete")
