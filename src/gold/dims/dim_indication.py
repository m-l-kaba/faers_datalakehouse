# Indication dimension for FAERS gold layer with SCD Type 2
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
logger = setup_logger("dim_indication")


def create_indication_dimension(spark) -> DataFrame:
    """
    Create indication dimension from silver indications data.

    This dimension tracks changes in medical indications using SCD Type 2:
    - Indication identity (primary_id, indi_pt) as business keys
    - Slowly changing attributes: therapeutic areas, severity classifications
    - Derived attributes: disease categories, treatment classifications

    Returns:
        DataFrame with indication dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating indication dimension from silver indications")

    # Read current silver indications data
    df_indications = spark.table("silver.indications")
    logger.info(f"Read {df_indications.count()} records from silver indications")

    # Create indication dimension with business keys and tracked attributes
    dim_indication = df_indications.select(
        # Business keys - uniquely identify an indication in a report
        F.col("primary_id").alias("indication_report_id"),
        F.col("indi_pt").alias("indication_preferred_term"),
        # Core indication information (slowly changing)
        F.col("indi_pt").alias("medical_indication"),
        F.col("therapeutic_area").alias("therapeutic_area"),
        F.col("indication_severity").alias("indication_severity"),
        # Metadata for lineage
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    # Add enhanced therapeutic classifications
    dim_indication = (
        dim_indication.withColumn(
            "is_oncology_indication",
            F.when(F.col("therapeutic_area") == "Oncology", True).otherwise(False),
        )
        .withColumn(
            "is_chronic_condition",
            F.when(
                F.upper(F.col("medical_indication")).rlike(
                    ".*CHRONIC.*|.*DIABETES.*|.*HYPERTENSION.*|.*ARTHRITIS.*"
                ),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "is_acute_condition",
            F.when(
                F.upper(F.col("medical_indication")).rlike(
                    ".*ACUTE.*|.*EMERGENCY.*|.*CRISIS.*"
                ),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "is_psychiatric_condition",
            F.when(F.col("therapeutic_area") == "Psychiatry", True).otherwise(False),
        )
        .withColumn(
            "is_rare_disease",
            F.when(
                F.upper(F.col("medical_indication")).rlike(
                    ".*RARE.*|.*ORPHAN.*|.*SYNDROME.*"
                ),
                True,
            ).otherwise(False),
        )
    )

    # Add disease severity scoring
    dim_indication = dim_indication.withColumn(
        "severity_score",
        F.when(F.col("indication_severity") == "Severe", 5)
        .when(F.col("indication_severity") == "Moderate", 3)
        .when(F.col("indication_severity") == "Mild", 1)
        .otherwise(0),
    )

    # Add regulatory and market classifications
    dim_indication = dim_indication.withColumn(
        "regulatory_category",
        F.when(F.col("is_oncology_indication"), "Oncology - Special Review")
        .when(F.col("is_rare_disease"), "Orphan Drug - Priority Review")
        .when(F.col("is_psychiatric_condition"), "CNS - Enhanced Monitoring")
        .when(
            F.col("therapeutic_area") == "Infectious Diseases",
            "Anti-Infective - Resistance Monitoring",
        )
        .otherwise("Standard Review"),
    )

    # Add market size classifications
    dim_indication = dim_indication.withColumn(
        "market_potential",
        F.when(
            F.col("therapeutic_area").isin("Oncology", "Cardiology", "Endocrinology"),
            "Large Market",
        )
        .when(
            F.col("therapeutic_area").isin("Neurology", "Psychiatry", "Rheumatology"),
            "Medium Market",
        )
        .when(F.col("is_rare_disease"), "Niche Market")
        .otherwise("Standard Market"),
    )

    # Add therapeutic area groupings
    dim_indication = dim_indication.withColumn(
        "therapeutic_area_group",
        F.when(
            F.col("therapeutic_area").isin("Oncology", "Neurology"), "Specialty Care"
        )
        .when(
            F.col("therapeutic_area").isin(
                "Cardiology", "Endocrinology", "Pulmonology"
            ),
            "Chronic Care",
        )
        .when(
            F.col("therapeutic_area").isin("Infectious Diseases", "Gastroenterology"),
            "Acute Care",
        )
        .when(
            F.col("therapeutic_area").isin("Psychiatry", "Rheumatology"),
            "Behavioral/Autoimmune",
        )
        .otherwise("General Medicine"),
    )

    # Add indication standardization
    dim_indication = dim_indication.withColumn(
        "indication_term_length", F.length(F.col("medical_indication"))
    ).withColumn(
        "indication_term_clean",
        F.upper(F.trim(F.regexp_replace(F.col("medical_indication"), r"[^\w\s]", ""))),
    )

    # Add treatment complexity indicators
    dim_indication = dim_indication.withColumn(
        "treatment_complexity",
        F.when(F.col("is_oncology_indication"), "High Complexity")
        .when(
            F.col("therapeutic_area").isin("Neurology", "Psychiatry"), "High Complexity"
        )
        .when(F.col("is_chronic_condition"), "Moderate Complexity")
        .when(F.col("is_acute_condition"), "Low Complexity")
        .otherwise("Standard Complexity"),
    )

    # Add patient monitoring requirements
    dim_indication = dim_indication.withColumn(
        "monitoring_requirements",
        F.when(F.col("is_oncology_indication"), "Intensive Monitoring")
        .when(F.col("therapeutic_area") == "Cardiology", "Cardiac Monitoring")
        .when(F.col("therapeutic_area") == "Psychiatry", "Behavioral Monitoring")
        .when(F.col("severity_score") >= 4, "Enhanced Monitoring")
        .otherwise("Standard Monitoring"),
    )

    # Add clinical trial potential
    dim_indication = dim_indication.withColumn(
        "clinical_trial_potential",
        F.when(F.col("is_rare_disease"), "High - Orphan Drug")
        .when(F.col("is_oncology_indication"), "High - Oncology")
        .when(F.col("therapeutic_area").isin("Neurology", "Psychiatry"), "Medium - CNS")
        .otherwise("Standard"),
    )

    logger.info(
        f"Created indication dimension with {dim_indication.count()} unique indication records"
    )
    return dim_indication


def process_indication_dimension(spark) -> None:
    """
    Process the indication dimension using SCD Type 2 logic.
    """
    logger.info("Starting indication dimension SCD Type 2 processing")

    # Create the dimension data
    new_indication_data = create_indication_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["indication_report_id", "indication_preferred_term"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "medical_indication",
        "therapeutic_area",
        "indication_severity",
        "is_oncology_indication",
        "is_chronic_condition",
        "is_acute_condition",
        "is_psychiatric_condition",
        "is_rare_disease",
        "severity_score",
        "regulatory_category",
        "market_potential",
        "therapeutic_area_group",
        "indication_term_clean",
        "treatment_complexity",
        "monitoring_requirements",
        "clinical_trial_potential",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_indication",
        new_data=new_indication_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="indication_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_indication")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_indication COMPUTE STATISTICS")

    logger.info("Indication dimension SCD Type 2 processing complete")


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the indication dimension
    process_indication_dimension(spark)

    logger.info("Indication dimension processing complete")
