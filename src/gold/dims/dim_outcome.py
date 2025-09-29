# Outcome dimension for FAERS gold layer with SCD Type 2
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
logger = setup_logger("dim_outcome")


def create_outcome_dimension(spark) -> DataFrame:
    """
    Create outcome dimension from silver outcomes data.

    This dimension tracks changes in clinical outcomes using SCD Type 2:
    - Outcome identity (primary_id, outc_cod) as business keys
    - Slowly changing attributes: outcome severity, category classifications
    - Derived attributes: regulatory significance, safety indicators

    Returns:
        DataFrame with outcome dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating outcome dimension from silver outcomes")

    # Read current silver outcomes data
    df_outcomes = spark.table("silver.outcomes")
    logger.info(f"Read {df_outcomes.count()} records from silver outcomes")

    # Create outcome dimension with business keys and tracked attributes
    dim_outcome = df_outcomes.select(
        # Business keys - uniquely identify an outcome in a report
        F.col("primary_id").alias("outcome_report_id"),
        F.col("outc_cod").alias("outcome_code"),
        # Core outcome information (slowly changing)
        F.col("outcome_description").alias("outcome_description"),
        F.col("outcome_severity_rank").alias("outcome_severity_rank"),
        F.col("outcome_category").alias("outcome_category"),
        # Metadata for lineage
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    # Add critical outcome classifications
    dim_outcome = (
        dim_outcome.withColumn(
            "is_fatal_outcome",
            F.when(F.col("outcome_code") == "DE", True).otherwise(False),
        )
        .withColumn(
            "is_life_threatening",
            F.when(F.col("outcome_code") == "LT", True).otherwise(False),
        )
        .withColumn(
            "requires_hospitalization",
            F.when(F.col("outcome_code") == "HO", True).otherwise(False),
        )
        .withColumn(
            "causes_disability",
            F.when(F.col("outcome_code") == "DS", True).otherwise(False),
        )
        .withColumn(
            "is_congenital_anomaly",
            F.when(F.col("outcome_code") == "CA", True).otherwise(False),
        )
        .withColumn(
            "requires_intervention",
            F.when(F.col("outcome_code") == "RI", True).otherwise(False),
        )
    )

    # Add regulatory reporting classifications
    dim_outcome = (
        dim_outcome.withColumn(
            "expedited_reporting_required",
            F.when(
                F.col("outcome_code").isin("DE", "LT", "HO", "DS", "CA"), True
            ).otherwise(False),
        )
        .withColumn(
            "serious_adverse_event",
            F.when(F.col("outcome_category") != "Unknown", True).otherwise(False),
        )
        .withColumn(
            "regulatory_priority",
            F.when(F.col("is_fatal_outcome"), "Critical - Immediate Report")
            .when(F.col("is_life_threatening"), "High - 15 Day Report")
            .when(F.col("outcome_code").isin("HO", "DS", "CA"), "High - 15 Day Report")
            .when(F.col("outcome_code") == "RI", "Medium - Standard Report")
            .when(F.col("outcome_code") == "OT", "Medium - Standard Report")
            .otherwise("Low - Routine Report"),
        )
    )

    # Add safety signal indicators
    dim_outcome = (
        dim_outcome.withColumn(
            "safety_signal_potential",
            F.when(F.col("outcome_severity_rank") >= 6, "High")
            .when(F.col("outcome_severity_rank").between(4, 5), "Medium")
            .when(F.col("outcome_severity_rank").between(2, 3), "Low")
            .otherwise("Minimal"),
        )
        .withColumn(
            "requires_safety_review",
            F.when(F.col("outcome_severity_rank") >= 5, True).otherwise(False),
        )
        .withColumn(
            "potential_causality_assessment",
            F.when(F.col("is_fatal_outcome"), "Mandatory Assessment")
            .when(F.col("is_life_threatening"), "Priority Assessment")
            .when(F.col("serious_adverse_event"), "Standard Assessment")
            .otherwise("Optional Assessment"),
        )
    )

    # Add clinical impact classifications
    dim_outcome = dim_outcome.withColumn(
        "clinical_impact_level",
        F.when(F.col("outcome_code") == "DE", "Irreversible")
        .when(F.col("outcome_code").isin("DS", "CA"), "Permanent")
        .when(F.col("outcome_code").isin("LT", "HO"), "Potentially Reversible")
        .when(F.col("outcome_code") == "RI", "Preventable")
        .otherwise("Temporary"),
    ).withColumn(
        "healthcare_utilization",
        F.when(F.col("requires_hospitalization"), "Inpatient Care")
        .when(F.col("outcome_code") == "LT", "Emergency Care")
        .when(F.col("requires_intervention"), "Outpatient Intervention")
        .otherwise("Monitoring Only"),
    )

    # Add outcome severity scoring
    dim_outcome = dim_outcome.withColumn(
        "clinical_severity_score",
        F.when(F.col("outcome_severity_rank") == 7, 100)  # Death
        .when(F.col("outcome_severity_rank") == 6, 90)  # Life-threatening
        .when(F.col("outcome_severity_rank") == 5, 80)  # Congenital anomaly
        .when(F.col("outcome_severity_rank") == 4, 70)  # Disability
        .when(F.col("outcome_severity_rank") == 3, 60)  # Hospitalization
        .when(F.col("outcome_severity_rank") == 2, 40)  # Required intervention
        .when(F.col("outcome_severity_rank") == 1, 20)  # Other serious
        .otherwise(0),
    )

    # Add pharmaceutical impact indicators
    dim_outcome = dim_outcome.withColumn(
        "drug_development_impact",
        F.when(F.col("is_fatal_outcome"), "Clinical Trial Stop")
        .when(F.col("is_life_threatening"), "Safety Review Required")
        .when(F.col("outcome_code").isin("DS", "CA"), "Label Update Required")
        .when(F.col("serious_adverse_event"), "Safety Monitoring")
        .otherwise("Routine Pharmacovigilance"),
    ).withColumn(
        "market_impact_potential",
        F.when(F.col("outcome_severity_rank") >= 6, "High Impact")
        .when(F.col("outcome_severity_rank").between(4, 5), "Medium Impact")
        .when(F.col("outcome_severity_rank").between(2, 3), "Low Impact")
        .otherwise("Minimal Impact"),
    )

    # Add patient care implications
    dim_outcome = dim_outcome.withColumn(
        "patient_monitoring_level",
        F.when(F.col("is_fatal_outcome"), "Post-mortem Review")
        .when(F.col("is_life_threatening"), "Intensive Monitoring")
        .when(F.col("requires_hospitalization"), "Inpatient Monitoring")
        .when(F.col("causes_disability"), "Long-term Follow-up")
        .when(F.col("requires_intervention"), "Active Monitoring")
        .otherwise("Standard Follow-up"),
    ).withColumn(
        "care_coordination_needed",
        F.when(
            F.col("outcome_code").isin("DE", "LT", "HO", "DS", "CA"), True
        ).otherwise(False),
    )

    # Add outcome predictability indicators
    dim_outcome = dim_outcome.withColumn(
        "outcome_predictability",
        F.when(F.col("outcome_code") == "CA", "Developmental")
        .when(F.col("outcome_code").isin("DE", "LT"), "Acute")
        .when(F.col("outcome_code") == "DS", "Progressive")
        .when(F.col("outcome_code") == "HO", "Episodic")
        .otherwise("Variable"),
    )

    # Add quality metrics
    dim_outcome = dim_outcome.withColumn(
        "outcome_completeness_score",
        F.when(F.col("outcome_description") != "Unknown", 100).otherwise(50),
    ).withColumn(
        "data_reliability",
        F.when(F.col("outcome_code").isin("DE", "LT", "HO"), "High")
        .when(F.col("outcome_code").isin("DS", "CA", "RI"), "Medium")
        .otherwise("Variable"),
    )

    logger.info(
        f"Created outcome dimension with {dim_outcome.count()} unique outcome records"
    )
    return dim_outcome


def process_outcome_dimension(spark) -> None:
    """
    Process the outcome dimension using SCD Type 2 logic.
    """
    logger.info("Starting outcome dimension SCD Type 2 processing")

    # Create the dimension data
    new_outcome_data = create_outcome_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["outcome_report_id", "outcome_code"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "outcome_description",
        "outcome_severity_rank",
        "outcome_category",
        "is_fatal_outcome",
        "is_life_threatening",
        "requires_hospitalization",
        "causes_disability",
        "is_congenital_anomaly",
        "requires_intervention",
        "expedited_reporting_required",
        "serious_adverse_event",
        "regulatory_priority",
        "safety_signal_potential",
        "requires_safety_review",
        "potential_causality_assessment",
        "clinical_impact_level",
        "healthcare_utilization",
        "clinical_severity_score",
        "drug_development_impact",
        "market_impact_potential",
        "patient_monitoring_level",
        "care_coordination_needed",
        "outcome_predictability",
        "outcome_completeness_score",
        "data_reliability",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_outcome",
        new_data=new_outcome_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="outcome_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_outcome")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_outcome COMPUTE STATISTICS")

    logger.info("Outcome dimension SCD Type 2 processing complete")


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the outcome dimension
    process_outcome_dimension(spark)

    logger.info("Outcome dimension processing complete")
