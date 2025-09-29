# Drug dimension for FAERS gold layer with SCD Type 2
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
logger = setup_logger("dim_drug")


def create_drug_dimension(spark) -> DataFrame:
    """
    Create drug dimension from silver drug details data.

    This dimension tracks changes in drug information using SCD Type 2:
    - Drug identity (primary_id, drug_seq) as business keys
    - Slowly changing attributes: drug names, dosage, role, expiration
    - Derived attributes: drug categories, dosage classifications

    Returns:
        DataFrame with drug dimension data ready for SCD Type 2 processing
    """
    logger.info("Creating drug dimension from silver drug details")

    df_drug_details = spark.table("silver.drug_details")
    logger.info(f"Read {df_drug_details.count()} records from silver drug details")

    dim_drug = df_drug_details.select(
        F.col("primary_id").alias("drug_report_id"),
        F.col("drug_seq").alias("drug_sequence_number"),
        F.col("drugname").alias("drug_name"),
        F.col("prod_ai").alias("active_ingredient"),
        F.col("val_vbm").alias("drug_verbatim"),
        F.col("role_cod").alias("drug_role_code"),
        F.col("role_description").alias("drug_role_description"),
        F.col("dose_vbm").alias("dose_verbatim"),
        F.col("dose_amt").alias("dose_amount"),
        F.col("dose_unit").alias("dose_unit"),
        F.col("dose_form").alias("dose_form"),
        F.col("route").alias("administration_route"),
        F.col("dose_freq").alias("dose_frequency"),
        F.col("nda_num").alias("nda_number"),
        F.col("exp_dt").alias("expiration_date"),
        F.col("silver_processed_ts").alias("source_processed_ts"),
        F.current_timestamp().alias("dim_created_ts"),
    ).distinct()

    dim_drug = dim_drug.withColumn(
        "drug_name_clean",
        F.upper(F.trim(F.regexp_replace(F.col("drug_name"), r"[^\w\s]", ""))),
    )

    dim_drug = dim_drug.withColumn(
        "dose_amount_derived",
        F.when(
            F.col("dose_amount").isNull(),
            F.when(
                F.regexp_extract(F.col("dose_verbatim"), r"(\d+\.?\d*)", 1) != "",
                F.regexp_extract(F.col("dose_verbatim"), r"(\d+\.?\d*)", 1).cast(
                    "double"
                ),
            ).otherwise(None),
        ).otherwise(F.col("dose_amount")),
    ).withColumn(
        "dose_unit_derived",
        F.when(
            F.col("dose_unit").isNull(),
            F.when(
                F.regexp_extract(F.col("dose_verbatim"), r"\d+\.?\d*\s*(\w+)", 1) != "",
                F.upper(
                    F.trim(
                        F.regexp_extract(
                            F.col("dose_verbatim"), r"\d+\.?\d*\s*(\w+)", 1
                        )
                    )
                ),
            ).otherwise(None),
        ).otherwise(F.col("dose_unit")),
    )

    dim_drug = (
        dim_drug.withColumn(
            "is_primary_suspect",
            F.when(F.col("drug_role_code") == "PS", True).otherwise(False),
        )
        .withColumn(
            "is_suspect_drug",
            F.when(F.col("drug_role_code").isin(["PS", "SS"]), True).otherwise(False),
        )
        .withColumn(
            "is_concomitant",
            F.when(F.col("drug_role_code") == "C", True).otherwise(False),
        )
    )

    dim_drug = dim_drug.withColumn(
        "route_standardized",
        F.when(F.upper(F.col("administration_route")).contains("ORAL"), "Oral")
        .when(F.upper(F.col("administration_route")).contains("IV"), "Intravenous")
        .when(F.upper(F.col("administration_route")).contains("IM"), "Intramuscular")
        .when(F.upper(F.col("administration_route")).contains("TOPICAL"), "Topical")
        .when(
            F.upper(F.col("administration_route")).contains("SUBCUTANEOUS"),
            "Subcutaneous",
        )
        .otherwise("Other"),
    )

    dim_drug = dim_drug.withColumn(
        "dose_strength_category",
        F.when(F.col("dose_amount_derived").isNull(), "Unknown")
        .when(F.col("dose_amount_derived") < 1, "Low")
        .when(F.col("dose_amount_derived").between(1, 100), "Medium")
        .when(F.col("dose_amount_derived") > 100, "High")
        .otherwise("Unknown"),
    )

    dim_drug = dim_drug.withColumn(
        "has_complete_dosage_info",
        F.when(
            (F.col("dose_amount_derived").isNotNull())
            & (F.col("dose_unit_derived").isNotNull())
            & (F.col("administration_route").isNotNull()),
            True,
        ).otherwise(False),
    ).withColumn(
        "has_nda_number", F.when(F.col("nda_number").isNotNull(), True).otherwise(False)
    )

    logger.info(f"Created drug dimension with {dim_drug.count()} unique drug records")
    return dim_drug


def process_drug_dimension(spark) -> None:
    """
    Process the drug dimension using SCD Type 2 logic.
    """
    logger.info("Starting drug dimension SCD Type 2 processing")

    # Create the dimension data
    new_drug_data = create_drug_dimension(spark)

    # Define business keys and tracked columns for SCD Type 2
    business_keys = ["drug_report_id", "drug_sequence_number"]

    # Columns to track for changes (will trigger new version when changed)
    tracked_columns = [
        "drug_name",
        "active_ingredient",
        "drug_verbatim",
        "drug_role_code",
        "drug_role_description",
        "dose_verbatim",
        "dose_amount",
        "dose_unit",
        "dose_form",
        "administration_route",
        "dose_frequency",
        "nda_number",
        "expiration_date",
        "drug_name_clean",
        "dose_amount_derived",
        "dose_unit_derived",
        "is_primary_suspect",
        "is_suspect_drug",
        "is_concomitant",
        "route_standardized",
        "dose_strength_category",
        "has_complete_dosage_info",
        "has_nda_number",
    ]

    # Apply SCD Type 2 logic using optimized MERGE
    apply_scd_type2_merge(
        spark=spark,
        target_table="gold.dim_drug",
        new_data=new_drug_data,
        business_keys=business_keys,
        tracked_columns=tracked_columns,
        dim_key_col="drug_dim_key",
    )

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_drug")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_drug COMPUTE STATISTICS")

    logger.info("Drug dimension SCD Type 2 processing complete")


def get_current_drugs(spark) -> DataFrame:
    """
    Get current drug dimension records for analysis.
    """
    from utils.scd_type2 import get_current_records

    return get_current_records(spark, "gold.dim_drug")


def get_drug_history(
    spark, drug_report_id: str, drug_sequence_number: int
) -> DataFrame:
    """
    Get the complete change history for a specific drug.
    """
    from utils.scd_type2 import get_change_history

    return get_change_history(
        spark,
        "gold.dim_drug",
        ["drug_report_id", "drug_sequence_number"],
        [drug_report_id, str(drug_sequence_number)],
    )


def analyze_drug_patterns(spark) -> None:
    """
    Analyze patterns in the drug dimension data.
    """
    logger.info("=== Drug Dimension Analysis ===")
    current_drugs = get_current_drugs(spark)

    # Basic statistics
    total_drugs = current_drugs.count()
    logger.info(f"Total current drug records: {total_drugs}")

    # Drug role distribution
    role_dist = (
        current_drugs.groupBy("drug_role_description")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Drug role distribution:")
    role_dist.show()

    # Route distribution
    route_dist = (
        current_drugs.groupBy("route_standardized")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Administration route distribution:")
    route_dist.show()

    # Dose strength distribution
    dose_dist = (
        current_drugs.groupBy("dose_strength_category")
        .count()
        .orderBy("count", ascending=False)
    )
    logger.info("Dose strength distribution:")
    dose_dist.show()

    # Data quality metrics
    quality_metrics = current_drugs.select(
        F.avg(F.col("has_complete_dosage_info").cast("int")).alias(
            "pct_complete_dosage"
        ),
        F.avg(F.col("has_nda_number").cast("int")).alias("pct_has_nda"),
        F.avg(F.col("is_suspect_drug").cast("int")).alias("pct_suspect_drugs"),
    )
    logger.info("Data quality metrics:")
    quality_metrics.show()


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the drug dimension
    process_drug_dimension(spark)

    logger.info("Drug dimension processing complete")
