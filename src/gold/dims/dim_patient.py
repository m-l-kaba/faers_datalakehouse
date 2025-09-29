# Patient dimension for FAERS gold layer with SCD Type 2
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
logger = setup_logger("dim_patient")


def create_patient_dimension(spark) -> DataFrame:
    """
    Create patient dimension from silver demographics data.

    This dimension tracks changes in patient demographics using SCD Type 2:
    - Patient identity (primary_id, caseid) as business keys
    - Slowly changing attributes: age, sex, weight, location data
    - Derived attributes: age groups, weight categories

    Returns:
        DataFrame with patient dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating patient dimension from silver demographics")

    # Read current silver demographics data
    df_demographics = spark.table("silver.demographics")
    logger.info(f"Read {df_demographics.count()} records from silver demographics")

    # Create patient dimension with business keys and tracked attributes
    dim_patient = df_demographics.select(
        # Business keys - uniquely identify a patient case
        F.col("primary_id").alias("patient_primary_id"),
        F.col("caseid").alias("patient_case_id"),
        # Slowly changing demographic attributes (Type 2)
        F.col("age").alias("patient_age"),
        F.col("age_group").alias("patient_age_group"),
        F.col("age_category").alias("patient_age_category"),
        F.col("sex").alias("patient_sex"),
        F.col("wt").alias("patient_weight"),
        F.col("weight_category").alias("patient_weight_category"),
        # Geographic attributes (can change)
        F.col("reporter_country").alias("reporter_country"),
        F.col("occr_country").alias("occurrence_country"),
        # Date attributes for reference (not tracked for changes)
        F.col("event_dt").alias("event_date"),
        F.col("init_fda_dt").alias("initial_fda_date"),
        F.col("mfr_dt").alias("manufacturer_date"),
        F.col("fda_dt").alias("fda_date"),
        F.col("rept_dt").alias("report_date"),
        # Metadata for lineage
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    # Add derived attributes
    dim_patient = dim_patient.withColumn(
        "patient_age_numeric",
        F.when(F.col("patient_age_category") == "Years", F.col("patient_age"))
        .when(F.col("patient_age_category") == "Months", F.col("patient_age") / 12.0)
        .when(F.col("patient_age_category") == "Weeks", F.col("patient_age") / 52.0)
        .when(F.col("patient_age_category") == "Days", F.col("patient_age") / 365.0)
        .otherwise(None),
    )

    # Add patient profile indicators
    dim_patient = (
        dim_patient.withColumn(
            "is_pediatric",
            F.when(F.col("patient_age_numeric") < 18, True).otherwise(False),
        )
        .withColumn(
            "is_elderly",
            F.when(F.col("patient_age_numeric") >= 65, True).otherwise(False),
        )
        .withColumn(
            "has_weight_data",
            F.when(F.col("patient_weight").isNotNull(), True).otherwise(False),
        )
    )

    # Add geographic region classification
    dim_patient = dim_patient.withColumn(
        "reporter_region",
        F.when(F.col("reporter_country").isin(["US", "CA", "MX"]), "North America")
        .when(
            F.col("reporter_country").isin(
                ["GB", "DE", "FR", "IT", "ES", "NL", "SE", "NO", "DK", "FI"]
            ),
            "Europe",
        )
        .when(
            F.col("reporter_country").isin(["JP", "CN", "IN", "KR", "AU"]),
            "Asia Pacific",
        )
        .when(F.col("reporter_country").isin(["BR", "AR", "CL", "CO"]), "South America")
        .otherwise("Other"),
    )

    logger.info(
        f"Created patient dimension with {dim_patient.count()} unique patient records"
    )
    return dim_patient


def process_patient_dimension(spark) -> None:
    """
    Process the patient dimension using SCD Type 2 logic.
    """
    logger.info("Starting patient dimension SCD Type 2 processing")

    # Create the dimension data
    new_patient_data = create_patient_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["patient_primary_id", "patient_case_id"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "patient_age",
        "patient_age_group",
        "patient_age_category",
        "patient_sex",
        "patient_weight",
        "patient_weight_category",
        "reporter_country",
        "occurrence_country",
        "patient_age_numeric",
        "is_pediatric",
        "is_elderly",
        "has_weight_data",
        "reporter_region",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_patient",
        new_data=new_patient_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="patient_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_patient")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_patient COMPUTE STATISTICS")

    logger.info("Patient dimension SCD Type 2 processing complete")


def get_current_patients(spark) -> DataFrame:
    """
    Get current patient dimension records for analysis.
    """
    from utils.scd_type2 import get_current_records

    return get_current_records(spark, "gold.dim_patient")


def get_patient_history(
    spark, patient_primary_id: str, patient_case_id: str
) -> DataFrame:
    """
    Get the complete change history for a specific patient.
    """
    from utils.scd_type2 import get_change_history

    return get_change_history(
        spark,
        "gold.dim_patient",
        ["patient_primary_id", "patient_case_id"],
        [patient_primary_id, patient_case_id],
    )


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the patient dimension
    process_patient_dimension(spark)

    logger.info("Patient dimension processing complete")
