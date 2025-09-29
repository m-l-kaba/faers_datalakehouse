# Fact table for FAERS adverse drug events
import os
import sys

sys.path.append("../../")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job
from utils.logger import setup_logger

config = load_config("../../config.yml")
target_catalog = os.getenv("target_catalog")
logger = setup_logger("fact_adverse_events")


def create_fact_adverse_events(spark) -> DataFrame:
    """
    Create the core fact table for FAERS adverse drug events.

    This fact table captures the relationships between:
    - Patients (who experienced the event)
    - Drugs (what was administered)
    - Reactions (what adverse events occurred)
    - Indications (why the drug was given)
    - Therapy durations (how long treatment lasted)
    - Outcomes (what happened as a result)
    - Reports (who reported and when)
    - Dates (when events occurred)

    The grain is: One row per drug-reaction combination per adverse event report.
    This allows analysis of:
    - Which drugs cause which reactions
    - Patient populations most affected
    - Severity and outcomes of adverse events
    - Reporting patterns and data quality
    - Temporal trends in adverse events

    Returns:
        DataFrame with fact table data ready for gold layer storage
    """
    logger.info("Creating fact_adverse_events from silver layer data")

    # Read silver layer tables
    demographics = spark.table("silver.demographics")
    drug_details = spark.table("silver.drug_details")
    reactions = spark.table("silver.reactions")
    indications = spark.table("silver.indications")
    therapy_durations = spark.table("silver.therapy_dates")
    outcomes = spark.table("silver.outcomes")
    reports = spark.table("silver.reports")

    logger.info("Read all silver layer source tables")

    fact_base = reactions.select(
        F.col("primary_id"),
        F.col("caseid"),
        F.col("pt").alias("reaction_preferred_term"),
        F.col("drug_rec_act").alias("drug_action_code"),
        F.col("reaction_category"),
        F.col("severity_classification"),
        F.col("reaction_priority_score"),
    )

    fact_with_drugs = fact_base.join(
        drug_details.select(
            "primary_id",
            "caseid",
            "drug_seq",
            "drugname",
            "role_cod",
            "dose_amt",
            "route",
        ),
        ["primary_id", "caseid"],
        "inner",
    )

    fact_with_patient = fact_with_drugs.join(
        demographics.select(
            "primary_id",
            "caseid",
            "age",
            "sex",
            "wt",
            "age_group",
            "reporter_country",
            "event_dt",
            "rept_dt",
            "fda_dt",
            "init_fda_dt",
        ),
        ["primary_id", "caseid"],
        "inner",
    )

    fact_with_indication = fact_with_patient.join(
        indications.select(
            "primary_id", "caseid", "indi_drug_seq", "indi_pt"
        ).withColumnRenamed("indi_drug_seq", "drug_seq"),
        ["primary_id", "caseid", "drug_seq"],
        "left",
    )

    fact_with_therapy = fact_with_indication.join(
        therapy_durations.select(
            "primary_id",
            "caseid",
            "dsg_drug_seq",
            "start_dt",
            "end_dt",
            "dur",
            "dur_cod",
        ).withColumnRenamed("dsg_drug_seq", "drug_seq"),
        ["primary_id", "caseid", "drug_seq"],
        "left",
    )

    fact_with_outcome = fact_with_therapy.join(
        outcomes.select(
            "primary_id",
            "caseid",
            "outc_cod",
            "outcome_description",
            "outcome_severity_rank",
            "outcome_category",
        ),
        ["primary_id", "caseid"],
        "left",
    )

    fact_complete = fact_with_outcome.join(
        reports.select(
            "primary_id",
            "caseid",
            "rpsr_cod",
            "reporter_source_description",
            "reporter_reliability_score",
            "regulatory_priority",
        ),
        ["primary_id", "caseid"],
        "left",
    )

    dim_date = spark.table("gold.dim_date")

    fact_with_event_date = fact_complete.join(
        dim_date.select(
            F.col("date_key").alias("event_date_key"),
            F.col("date_value").alias("event_date"),
            F.col("year").alias("event_year"),
            F.col("quarter").alias("event_quarter"),
            F.col("month").alias("event_month"),
            F.col("year_quarter").alias("event_year_quarter"),
            F.col("reporting_period").alias("event_reporting_period"),
        ),
        F.to_date(F.col("event_dt"), "yyyyMMdd") == F.col("event_date"),
        "left",
    )

    fact_with_report_date = fact_with_event_date.join(
        dim_date.select(
            F.col("date_key").alias("report_date_key"),
            F.col("date_value").alias("report_date"),
            F.col("year").alias("report_year"),
            F.col("quarter").alias("report_quarter"),
            F.col("month").alias("report_month"),
            F.col("year_quarter").alias("report_year_quarter"),
            F.col("reporting_period").alias("report_reporting_period"),
        ),
        F.to_date(F.col("rept_dt"), "yyyyMMdd") == F.col("report_date"),
        "left",
    )

    fact_final = fact_with_report_date.join(
        dim_date.select(
            F.col("date_key").alias("fda_received_date_key"),
            F.col("date_value").alias("fda_received_date"),
            F.col("year").alias("fda_received_year"),
            F.col("quarter").alias("fda_received_quarter"),
            F.col("year_quarter").alias("fda_received_year_quarter"),
        ),
        F.to_date(F.col("fda_dt"), "yyyyMMdd") == F.col("fda_received_date"),
        "left",
    )

    logger.info("Completed all joins for fact table including date dimensions")

    fact_adverse_events = fact_final.select(
        F.col("event_date_key"),
        F.col("report_date_key"),
        F.col("fda_received_date_key"),
        F.col("primary_id").alias("report_primary_id"),
        F.col("caseid").alias("report_case_id"),
        F.col("drug_seq").alias("drug_sequence_number"),
        F.col("reaction_preferred_term"),
        F.coalesce(F.col("indi_pt"), F.lit("Unknown")).alias(
            "indication_preferred_term"
        ),
        F.coalesce(F.col("outc_cod"), F.lit("Unknown")).alias("outcome_code"),
        F.col("reaction_priority_score")
        .cast(DecimalType(5, 2))
        .alias("reaction_severity_score"),
        F.coalesce(F.col("outcome_severity_rank"), F.lit(0))
        .cast(IntegerType())
        .alias("outcome_severity_rank"),
        F.coalesce(F.col("reporter_reliability_score"), F.lit(1))
        .cast(IntegerType())
        .alias("reporter_reliability_score"),
        F.col("age").cast(IntegerType()).alias("patient_age"),
        F.col("wt").cast(DecimalType(8, 2)).alias("patient_weight"),
        F.col("dose_amt").cast(DecimalType(10, 4)).alias("drug_dose_amount"),
        F.when(F.col("dur_cod") == "DAY", F.col("dur"))
        .when(F.col("dur_cod") == "WK", F.col("dur") * 7)
        .when(F.col("dur_cod") == "MON", F.col("dur") * 30)
        .when(F.col("dur_cod") == "YR", F.col("dur") * 365)
        .otherwise(F.col("dur"))
        .cast(IntegerType())
        .alias("therapy_duration_days"),
        (F.col("role_cod") == "PS").alias("is_primary_suspect_drug"),
        (F.col("severity_classification") == "Life-threatening").alias(
            "is_life_threatening_reaction"
        ),
        (F.col("outcome_category") == "Fatal").alias("is_fatal_outcome"),
        (F.col("sex") == "F").alias("is_female_patient"),
        (F.col("age") < 18).alias("is_pediatric_case"),
        (F.col("age") >= 65).alias("is_elderly_case"),
        (F.col("reporter_source_description") == "Healthcare Professional").alias(
            "is_healthcare_professional_report"
        ),
        F.col("drug_action_code").isNotNull().alias("has_drug_action_taken"),
        F.col("indi_pt").isNotNull().alias("has_indication_data"),
        F.col("dur").isNotNull().alias("has_therapy_duration"),
        F.col("outc_cod").isNotNull().alias("has_outcome_data"),
        F.col("event_date_key").isNotNull().alias("has_event_date"),
        F.col("report_date_key").isNotNull().alias("has_report_date"),
        F.col("fda_received_date_key").isNotNull().alias("has_fda_received_date"),
        F.col("reaction_category"),
        F.col("outcome_category"),
        F.col("age_group").alias("patient_age_group"),
        F.col("reporter_country"),
        F.col("regulatory_priority"),
        F.col("route").alias("administration_route"),
        F.col("drugname").alias("drug_name"),
        F.to_date(F.col("event_dt"), "yyyyMMdd").alias("adverse_event_date"),
        F.to_date(F.col("start_dt"), "yyyyMMdd").alias("therapy_start_date"),
        F.to_date(F.col("end_dt"), "yyyyMMdd").alias("therapy_end_date"),
        F.col("event_year").alias("event_occurrence_year"),
        F.col("event_quarter").alias("event_occurrence_quarter"),
        F.col("event_year_quarter").alias("event_occurrence_year_quarter"),
        F.col("event_reporting_period").alias("event_reporting_period"),
        F.col("report_year").alias("report_submission_year"),
        F.col("report_quarter").alias("report_submission_quarter"),
        F.col("fda_received_year").alias("fda_processing_year"),
        F.datediff(F.col("report_date"), F.col("event_date")).alias(
            "days_event_to_report"
        ),
        F.datediff(F.col("fda_received_date"), F.col("report_date")).alias(
            "days_report_to_fda"
        ),
        F.datediff(F.col("fda_received_date"), F.col("event_date")).alias(
            "days_event_to_fda_receipt"
        ),
        # Data quality indicators
        F.when(
            F.col("event_dt").isNull()
            | F.col("reaction_preferred_term").isNull()
            | F.col("drugname").isNull(),
            "Poor",
        )
        .when(
            F.col("indi_pt").isNull()
            | F.col("dur").isNull()
            | F.col("outc_cod").isNull(),
            "Moderate",
        )
        .otherwise("Good")
        .alias("data_quality_tier"),
        # Metadata
        F.current_timestamp().alias("fact_created_ts"),
        F.current_date().alias("fact_created_date"),
    )

    # Add computed risk metrics
    fact_adverse_events = fact_adverse_events.withColumn(
        "clinical_risk_score",
        # Weighted risk score based on multiple factors
        (F.col("reaction_severity_score") * 0.4)
        + (F.col("outcome_severity_rank") * 0.3)
        + (F.when(F.col("is_life_threatening_reaction"), 3).otherwise(0) * 0.2)
        + (F.when(F.col("is_fatal_outcome"), 5).otherwise(0) * 0.1),
    )

    # Add case complexity indicator
    fact_adverse_events = fact_adverse_events.withColumn(
        "case_complexity",
        F.when(
            (F.col("has_indication_data"))
            & (F.col("has_therapy_duration"))
            & (F.col("has_outcome_data"))
            & (F.col("is_healthcare_professional_report")),
            "High",
        )
        .when((F.col("has_indication_data")) & (F.col("has_outcome_data")), "Medium")
        .otherwise("Low"),
    )

    logger.info(f"Created fact table with {fact_adverse_events.count()} records")
    return fact_adverse_events


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Create the fact table
    logger.info("Starting fact_adverse_events creation")
    fact_data = create_fact_adverse_events(spark)

    # Write to gold layer with partitioning for performance
    logger.info("Writing fact table to gold layer")
    fact_data.write.mode("overwrite").partitionBy(
        "fact_created_date", "reporter_country"
    ).option("overwriteSchema", "true").saveAsTable("gold.fact_adverse_events")

    # Optimize for query performance
    spark.sql("OPTIMIZE gold.fact_adverse_events")

    # Update statistics
    spark.sql("ANALYZE TABLE gold.fact_adverse_events COMPUTE STATISTICS")

    logger.info("Adverse events fact table creation complete")
