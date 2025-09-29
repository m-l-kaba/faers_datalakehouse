# Reaction dimension for FAERS gold layer with SCD Type 2
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
logger = setup_logger("dim_reaction")


def create_reaction_dimension(spark) -> DataFrame:
    """
    Create reaction dimension from silver reactions data.

    This dimension tracks changes in adverse event reactions using SCD Type 2:
    - Reaction identity (primary_id, pt) as business keys
    - Slowly changing attributes: classifications, severity, drug actions
    - Derived attributes: medical categorizations, priority scores

    Returns:
        DataFrame with reaction dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating reaction dimension from silver reactions")

    # Read current silver reactions data
    df_reactions = spark.table("silver.reactions")
    logger.info(f"Read {df_reactions.count()} records from silver reactions")

    # Create reaction dimension with business keys and tracked attributes
    dim_reaction = df_reactions.select(
        # Business keys - uniquely identify a reaction in a report
        F.col("primary_id").alias("reaction_report_id"),
        F.col("pt").alias("preferred_term"),
        # Core reaction information (slowly changing)
        F.col("pt").alias("reaction_preferred_term"),
        F.col("reaction_category").alias("reaction_category"),
        F.col("severity_classification").alias("severity_classification"),
        F.col("system_organ_class").alias("system_organ_class"),
        # Drug action information (can change)
        F.col("drug_rec_act").alias("drug_action_code"),
        F.col("drug_rec_act_description").alias("drug_action_description"),
        # Derived severity and priority metrics
        F.col("reaction_priority_score").alias("reaction_priority_score"),
        # Metadata for lineage
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    # Add enhanced reaction classifications
    dim_reaction = (
        dim_reaction.withColumn(
            "is_fatal_reaction",
            F.when(
                F.upper(F.col("reaction_preferred_term")).contains("DEATH"), True
            ).otherwise(False),
        )
        .withColumn(
            "is_serious_reaction",
            F.when(
                F.col("severity_classification").isin(
                    "Life-threatening", "Serious", "Hospitalization Required"
                ),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "is_cardiac_event",
            F.when(F.col("reaction_category") == "Cardiovascular", True).otherwise(
                False
            ),
        )
        .withColumn(
            "is_neurological_event",
            F.when(F.col("reaction_category") == "Neurological", True).otherwise(False),
        )
        .withColumn(
            "is_hepatic_event",
            F.when(F.col("reaction_category") == "Hepatic", True).otherwise(False),
        )
    )

    # Add regulatory reporting classifications
    dim_reaction = dim_reaction.withColumn(
        "regulatory_significance",
        F.when(F.col("is_fatal_reaction"), "Expedited Reporting Required")
        .when(F.col("is_serious_reaction"), "Serious Adverse Event")
        .when(F.col("reaction_priority_score") >= 7, "Significant Event")
        .otherwise("Standard Reporting"),
    )

    # Add medical device/drug interaction indicators
    dim_reaction = (
        dim_reaction.withColumn(
            "drug_action_taken",
            F.when(F.col("drug_action_code") == "1", True)  # Drug withdrawn
            .when(F.col("drug_action_code") == "2", True)  # Dose reduced
            .when(F.col("drug_action_code") == "3", True)  # Dose increased
            .otherwise(False),
        )
        .withColumn(
            "drug_withdrawn",
            F.when(F.col("drug_action_code") == "1", True).otherwise(False),
        )
        .withColumn(
            "dose_modified",
            F.when(F.col("drug_action_code").isin("2", "3"), True).otherwise(False),
        )
    )

    # Add reaction term standardization
    dim_reaction = dim_reaction.withColumn(
        "reaction_term_length", F.length(F.col("reaction_preferred_term"))
    ).withColumn(
        "reaction_term_clean",
        F.upper(
            F.trim(F.regexp_replace(F.col("reaction_preferred_term"), r"[^\w\s]", ""))
        ),
    )

    # Add system organ class hierarchy
    dim_reaction = dim_reaction.withColumn(
        "organ_system_group",
        F.when(F.col("system_organ_class").contains("Cardiac"), "Cardiovascular System")
        .when(F.col("system_organ_class").contains("Nervous"), "Nervous System")
        .when(F.col("system_organ_class").contains("Respiratory"), "Respiratory System")
        .when(
            F.col("system_organ_class").contains("Gastrointestinal"), "Digestive System"
        )
        .when(F.col("system_organ_class").contains("Hepatobiliary"), "Hepatic System")
        .when(F.col("system_organ_class").contains("Renal"), "Renal System")
        .when(F.col("system_organ_class").contains("Skin"), "Integumentary System")
        .when(F.col("system_organ_class").contains("Neoplasms"), "Oncology")
        .when(F.col("system_organ_class").contains("Infections"), "Infectious Disease")
        .when(F.col("system_organ_class").contains("Psychiatric"), "Mental Health")
        .otherwise("Other Systems"),
    )

    # Add safety signal indicators
    dim_reaction = dim_reaction.withColumn(
        "potential_safety_signal",
        F.when(
            (F.col("is_serious_reaction")) & (F.col("drug_action_taken")), True
        ).otherwise(False),
    ).withColumn(
        "requires_medical_review",
        F.when(F.col("reaction_priority_score") >= 8, True).otherwise(False),
    )

    logger.info(
        f"Created reaction dimension with {dim_reaction.count()} unique reaction records"
    )
    return dim_reaction


def process_reaction_dimension(spark) -> None:
    """
    Process the reaction dimension using SCD Type 2 logic.
    """
    logger.info("Starting reaction dimension SCD Type 2 processing")

    # Create the dimension data
    new_reaction_data = create_reaction_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["reaction_report_id", "preferred_term"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "reaction_preferred_term",
        "reaction_category",
        "severity_classification",
        "system_organ_class",
        "drug_action_code",
        "drug_action_description",
        "reaction_priority_score",
        "is_fatal_reaction",
        "is_serious_reaction",
        "is_cardiac_event",
        "is_neurological_event",
        "is_hepatic_event",
        "regulatory_significance",
        "drug_action_taken",
        "drug_withdrawn",
        "dose_modified",
        "reaction_term_clean",
        "organ_system_group",
        "potential_safety_signal",
        "requires_medical_review",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_reaction",
        new_data=new_reaction_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="reaction_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_reaction")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_reaction COMPUTE STATISTICS")

    logger.info("Reaction dimension SCD Type 2 processing complete")


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the reaction dimension
    process_reaction_dimension(spark)

    logger.info("Reaction dimension processing complete")
