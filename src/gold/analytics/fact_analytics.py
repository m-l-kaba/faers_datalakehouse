# Fact Table Analytics and Insights Generator
import os
import sys

sys.path.append("../../")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job
from utils.logger import setup_logger

config = load_config()
target_catalog = os.getenv("target_catalog")
logger = setup_logger("fact_analytics")


def generate_adverse_events_analytics(spark) -> None:
    """
    Generate comprehensive analytics on the adverse events fact table.
    This provides insights into drug safety patterns, reporting trends,
    and clinical outcomes for pharmaceutical safety monitoring.
    """
    logger.info("Generating adverse events fact table analytics")

    fact_table = spark.table("gold.fact_adverse_events")

    # Overall fact table metrics
    total_events = fact_table.count()
    distinct_cases = fact_table.select("report_primary_id").distinct().count()
    distinct_drugs = fact_table.select("drug_name").distinct().count()
    distinct_reactions = fact_table.select("reaction_preferred_term").distinct().count()

    logger.info("=== FACT TABLE OVERVIEW ===")
    logger.info(f"Total adverse event records: {total_events:,}")
    logger.info(f"Unique cases reported: {distinct_cases:,}")
    logger.info(f"Unique drugs involved: {distinct_drugs:,}")
    logger.info(f"Unique reaction types: {distinct_reactions:,}")
    logger.info(f"Average reactions per case: {total_events / distinct_cases:.1f}")

    # Drug safety insights
    logger.info("=== DRUG SAFETY PATTERNS ===")

    # Top drugs by adverse event frequency
    top_drugs_by_events = (
        fact_table.groupBy("drug_name")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("report_primary_id").alias("unique_cases"),
            F.avg("clinical_risk_score").alias("avg_risk_score"),
            F.sum(F.col("is_life_threatening_reaction").cast("int")).alias(
                "life_threatening_count"
            ),
            F.sum(F.col("is_fatal_outcome").cast("int")).alias("fatal_count"),
        )
        .orderBy(F.desc("total_events"))
        .limit(20)
    )

    logger.info("Top 20 drugs by adverse event frequency:")
    top_drugs_by_events.show(20, truncate=False)

    # Highest risk drugs (by clinical risk score)
    high_risk_drugs = (
        fact_table.groupBy("drug_name")
        .agg(
            F.count("*").alias("total_events"),
            F.avg("clinical_risk_score").alias("avg_risk_score"),
            F.max("clinical_risk_score").alias("max_risk_score"),
        )
        .filter(F.col("total_events") >= 10)  # Minimum threshold for significance
        .orderBy(F.desc("avg_risk_score"))
        .limit(15)
    )

    logger.info("Highest risk drugs (min 10 events):")
    high_risk_drugs.show(15, truncate=False)

    # Reaction pattern analysis
    logger.info("=== ADVERSE REACTION PATTERNS ===")

    # Most common reactions
    reaction_patterns = (
        fact_table.groupBy("reaction_preferred_term", "reaction_category")
        .agg(
            F.count("*").alias("total_occurrences"),
            F.countDistinct("drug_name").alias("drugs_associated"),
            F.avg("reaction_severity_score").alias("avg_severity"),
            F.sum(F.col("is_life_threatening_reaction").cast("int")).alias(
                "life_threatening_cases"
            ),
        )
        .orderBy(F.desc("total_occurrences"))
        .limit(25)
    )

    logger.info("Top 25 adverse reactions:")
    reaction_patterns.show(25, truncate=False)

    # Patient demographic insights
    logger.info("=== PATIENT DEMOGRAPHICS ===")

    # Age group analysis
    age_demographics = (
        fact_table.groupBy("patient_age_group")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("report_primary_id").alias("unique_cases"),
            F.avg("clinical_risk_score").alias("avg_risk_score"),
            F.avg("patient_age").alias("avg_age"),
            F.sum(F.col("is_fatal_outcome").cast("int")).alias("fatal_outcomes"),
        )
        .orderBy(F.desc("total_events"))
    )

    logger.info("Adverse events by age group:")
    age_demographics.show(truncate=False)

    # Gender analysis
    gender_analysis = fact_table.groupBy("is_female_patient").agg(
        F.count("*").alias("total_events"),
        F.countDistinct("report_primary_id").alias("unique_cases"),
        F.avg("clinical_risk_score").alias("avg_risk_score"),
        F.sum(F.col("is_life_threatening_reaction").cast("int")).alias(
            "life_threatening_count"
        ),
        F.sum(F.col("is_fatal_outcome").cast("int")).alias("fatal_count"),
    )

    logger.info("Adverse events by gender:")
    gender_analysis.show()

    # Reporting source analysis
    logger.info("=== REPORTING PATTERNS ===")

    reporting_analysis = (
        fact_table.groupBy("is_healthcare_professional_report", "regulatory_priority")
        .agg(
            F.count("*").alias("total_events"),
            F.avg("reporter_reliability_score").alias("avg_reliability"),
            F.avg("clinical_risk_score").alias("avg_risk_score"),
        )
        .orderBy(F.desc("total_events"))
    )

    logger.info("Events by reporting source:")
    reporting_analysis.show()

    # Data quality metrics
    logger.info("=== DATA QUALITY ASSESSMENT ===")

    data_quality_metrics = (
        fact_table.groupBy("data_quality_tier")
        .agg(
            F.count("*").alias("record_count"),
            F.round((F.count("*") / total_events * 100), 2).alias("percentage"),
        )
        .orderBy(F.desc("record_count"))
    )

    logger.info("Data quality distribution:")
    data_quality_metrics.show()

    # Case complexity analysis
    complexity_analysis = fact_table.groupBy("case_complexity").agg(
        F.count("*").alias("case_count"),
        F.avg("clinical_risk_score").alias("avg_risk_score"),
        F.sum(F.col("has_indication_data").cast("int")).alias("has_indication"),
        F.sum(F.col("has_therapy_duration").cast("int")).alias("has_duration"),
        F.sum(F.col("has_outcome_data").cast("int")).alias("has_outcome"),
    )

    logger.info("Case complexity distribution:")
    complexity_analysis.show()

    # Temporal trends (if we have date data)
    logger.info("=== TEMPORAL TRENDS ===")

    temporal_trends = (
        fact_table.filter(F.col("adverse_event_date").isNotNull())
        .groupBy(F.year("adverse_event_date").alias("event_year"))
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("report_primary_id").alias("unique_cases"),
            F.avg("clinical_risk_score").alias("avg_risk_score"),
            F.sum(F.col("is_fatal_outcome").cast("int")).alias("fatal_outcomes"),
        )
        .orderBy("event_year")
    )

    logger.info("Adverse events by year:")
    temporal_trends.show()

    # Generate summary insights
    generate_summary_insights(spark, fact_table)

    logger.info("Fact table analytics generation complete")


def generate_summary_insights(spark, fact_table: DataFrame) -> None:
    """
    Generate high-level summary insights from the fact table data.
    """
    logger.info("=== KEY INSIGHTS SUMMARY ===")

    # Calculate key metrics
    metrics = fact_table.select(
        F.count("*").alias("total_events"),
        F.countDistinct("report_primary_id").alias("total_cases"),
        F.countDistinct("drug_name").alias("total_drugs"),
        F.avg("clinical_risk_score").alias("avg_risk"),
        F.sum(F.col("is_life_threatening_reaction").cast("int")).alias(
            "life_threatening"
        ),
        F.sum(F.col("is_fatal_outcome").cast("int")).alias("fatal"),
        F.sum(F.col("is_pediatric_case").cast("int")).alias("pediatric"),
        F.sum(F.col("is_elderly_case").cast("int")).alias("elderly"),
        F.sum(F.col("is_healthcare_professional_report").cast("int")).alias(
            "hcp_reports"
        ),
    ).collect()[0]

    # Calculate percentages and insights
    total_events = metrics["total_events"]
    life_threatening_pct = metrics["life_threatening"] / total_events * 100
    fatal_pct = metrics["fatal"] / total_events * 100
    pediatric_pct = metrics["pediatric"] / total_events * 100
    elderly_pct = metrics["elderly"] / total_events * 100
    hcp_report_pct = metrics["hcp_reports"] / total_events * 100

    logger.info(f"📊 FAERS Adverse Event Analytics Summary:")
    logger.info(f"   🔢 Total Events: {total_events:,}")
    logger.info(f"   📋 Total Cases: {metrics['total_cases']:,}")
    logger.info(f"   💊 Unique Drugs: {metrics['total_drugs']:,}")
    logger.info(f"   ⚠️  Average Risk Score: {metrics['avg_risk']:.2f}")
    logger.info(
        f"   🚨 Life-Threatening: {life_threatening_pct:.1f}% ({metrics['life_threatening']:,})"
    )
    logger.info(f"   ☠️  Fatal Outcomes: {fatal_pct:.1f}% ({metrics['fatal']:,})")
    logger.info(
        f"   👶 Pediatric Cases: {pediatric_pct:.1f}% ({metrics['pediatric']:,})"
    )
    logger.info(f"   👴 Elderly Cases: {elderly_pct:.1f}% ({metrics['elderly']:,})")
    logger.info(
        f"   🏥 Healthcare Professional Reports: {hcp_report_pct:.1f}% ({metrics['hcp_reports']:,})"
    )


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Generate comprehensive analytics
    generate_adverse_events_analytics(spark)

    logger.info("Fact table analytics complete")
