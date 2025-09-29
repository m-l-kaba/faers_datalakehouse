# Date dimension for FAERS gold layer (Type 1 - static reference data)
import os
import sys

sys.path.append("../../")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job
from utils.logger import setup_logger

config = load_config("../../config.yml")
target_catalog = os.getenv("target_catalog")
logger = setup_logger("dim_date")


def create_date_dimension(
    spark, start_date: str = "2000-01-01", end_date: str = "2030-12-31"
) -> DataFrame:
    """
    Create a comprehensive date dimension table.

    This is typically a Type 1 dimension that doesn't change over time.
    Provides a rich set of date attributes for time-based analysis.

    Args:
        spark: SparkSession
        start_date: Start date for dimension in 'YYYY-MM-DD' format
        end_date: End date for dimension in 'YYYY-MM-DD' format

    Returns:
        DataFrame with comprehensive date dimension
    """
    logger.info(f"Creating date dimension from {start_date} to {end_date}")

    # Generate continuous date range
    date_range_sql = f"""
        SELECT explode(sequence(
            to_date('{start_date}'), 
            to_date('{end_date}'), 
            interval 1 day
        )) as date_value
    """
    date_range = spark.sql(date_range_sql)

    # Create comprehensive date dimension with all attributes
    dim_date = date_range.select(
        # Primary key - date itself
        F.col("date_value").alias("date_key"),
        F.col("date_value"),
        # Basic date components
        F.year("date_value").alias("year"),
        F.month("date_value").alias("month"),
        F.dayofmonth("date_value").alias("day"),
        F.quarter("date_value").alias("quarter"),
        F.weekofyear("date_value").alias("week_of_year"),
        F.dayofweek("date_value").alias("day_of_week"),  # 1=Sunday, 7=Saturday
        F.dayofyear("date_value").alias("day_of_year"),
        # Formatted date strings
        F.date_format("date_value", "yyyy-MM-dd").alias("date_string"),
        F.date_format("date_value", "yyyyMMdd").alias("date_string_compact"),
        # Month and day names
        F.date_format("date_value", "MMMM").alias("month_name"),
        F.date_format("date_value", "MMM").alias("month_short_name"),
        F.date_format("date_value", "EEEE").alias("day_name"),
        F.date_format("date_value", "EEE").alias("day_short_name"),
        # Useful date groupings
        F.concat(F.year("date_value"), F.lit("-Q"), F.quarter("date_value")).alias(
            "year_quarter"
        ),
        F.concat(
            F.year("date_value"), F.lit("-"), F.lpad(F.month("date_value"), 2, "0")
        ).alias("year_month"),
        F.concat(
            F.year("date_value"),
            F.lit("-W"),
            F.lpad(F.weekofyear("date_value"), 2, "0"),
        ).alias("year_week"),
        # Boolean indicators
        F.when(F.dayofweek("date_value").isin(1, 7), True)
        .otherwise(False)
        .alias("is_weekend"),
        F.when(F.dayofweek("date_value").between(2, 6), True)
        .otherwise(False)
        .alias("is_weekday"),
        # First and last day indicators
        F.when(F.dayofmonth("date_value") == 1, True)
        .otherwise(False)
        .alias("is_first_day_of_month"),
        F.when(
            F.dayofmonth("date_value") == F.dayofmonth(F.last_day("date_value")), True
        )
        .otherwise(False)
        .alias("is_last_day_of_month"),
        F.when(F.dayofyear("date_value") == 1, True)
        .otherwise(False)
        .alias("is_first_day_of_year"),
        F.when((F.month("date_value") == 12) & (F.dayofmonth("date_value") == 31), True)
        .otherwise(False)
        .alias("is_last_day_of_year"),
        # Metadata
        F.current_timestamp().alias("created_ts"),
        F.current_date().alias("created_date"),
    )

    # Add additional business-relevant date classifications
    dim_date = dim_date.withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), "Winter")
        .when(F.col("month").isin(3, 4, 5), "Spring")
        .when(F.col("month").isin(6, 7, 8), "Summer")
        .otherwise("Fall"),
    )

    # Add fiscal year (assuming July 1 - June 30)
    dim_date = dim_date.withColumn(
        "fiscal_year",
        F.when(F.col("month") >= 7, F.col("year") + 1).otherwise(F.col("year")),
    ).withColumn(
        "fiscal_quarter",
        F.when(F.col("month").between(7, 9), 1)
        .when(F.col("month").between(10, 12), 2)
        .when(F.col("month").between(1, 3), 3)
        .otherwise(4),
    )

    # Add relative date indicators (useful for analysis)
    current_date_lit = F.current_date()
    dim_date = (
        dim_date.withColumn(
            "days_from_today", F.datediff(F.col("date_value"), current_date_lit)
        )
        .withColumn(
            "is_past",
            F.when(F.col("date_value") < current_date_lit, True).otherwise(False),
        )
        .withColumn(
            "is_future",
            F.when(F.col("date_value") > current_date_lit, True).otherwise(False),
        )
        .withColumn(
            "is_today",
            F.when(F.col("date_value") == current_date_lit, True).otherwise(False),
        )
    )

    # Add period classifications for FAERS analysis
    dim_date = dim_date.withColumn(
        "reporting_period",
        F.when(F.col("year") < 2010, "Pre-2010")
        .when(F.col("year").between(2010, 2015), "2010-2015")
        .when(F.col("year").between(2016, 2020), "2016-2020")
        .when(F.col("year") >= 2021, "2021+")
        .otherwise("Unknown"),
    )

    logger.info(f"Created date dimension with {dim_date.count()} date records")
    return dim_date


def process_date_dimension(
    spark, start_date: str = "2000-01-01", end_date: str = "2030-12-31"
) -> None:
    """
    Process the date dimension (Type 1 - overwrite mode).
    """
    logger.info("Starting date dimension processing")

    # Create the date dimension
    date_data = create_date_dimension(spark, start_date, end_date)

    # Write to target table (overwrite since it's Type 1)
    logger.info("Writing date dimension to gold.dim_date")
    date_data.write.format("delta").mode("overwrite").option(
        "delta.autoOptimize.optimizeWrite", "true"
    ).saveAsTable("gold.dim_date")

    # Optimize the table for better query performance
    spark.sql("OPTIMIZE gold.dim_date")

    # Update table statistics
    spark.sql("ANALYZE TABLE gold.dim_date COMPUTE STATISTICS")

    logger.info("Date dimension processing complete")


def get_date_by_key(spark, date_key: str) -> DataFrame:
    """
    Get date dimension record by date key.
    """
    return spark.table("gold.dim_date").filter(F.col("date_key") == date_key)


def get_dates_in_range(spark, start_date: str, end_date: str) -> DataFrame:
    """
    Get date dimension records within a date range.
    """
    return spark.table("gold.dim_date").filter(
        (F.col("date_key") >= start_date) & (F.col("date_key") <= end_date)
    )


def get_business_days(spark, start_date: str, end_date: str) -> DataFrame:
    """
    Get only business days (weekdays) within a date range.
    """
    return get_dates_in_range(spark, start_date, end_date).filter(
        F.col("is_weekday") == True
    )


def analyze_date_dimension(spark) -> None:
    """
    Analyze the date dimension data.
    """
    logger.info("=== Date Dimension Analysis ===")
    dim_date = spark.table("gold.dim_date")

    # Basic statistics
    total_dates = dim_date.count()
    min_date = dim_date.agg(F.min("date_key")).collect()[0][0]
    max_date = dim_date.agg(F.max("date_key")).collect()[0][0]

    logger.info(f"Total date records: {total_dates}")
    logger.info(f"Date range: {min_date} to {max_date}")

    # Weekend vs weekday distribution
    day_type_dist = dim_date.select(
        F.sum(F.col("is_weekend").cast("int")).alias("weekend_days"),
        F.sum(F.col("is_weekday").cast("int")).alias("weekdays"),
    )
    logger.info("Day type distribution:")
    day_type_dist.show()

    # Seasonal distribution
    season_dist = dim_date.groupBy("season").count().orderBy("count", ascending=False)
    logger.info("Seasonal distribution:")
    season_dist.show()

    # Reporting period distribution
    period_dist = (
        dim_date.groupBy("reporting_period").count().orderBy("reporting_period")
    )
    logger.info("Reporting period distribution:")
    period_dist.show()

    # Current date context
    current_date_info = dim_date.filter(F.col("is_today") == True).select(
        "date_key", "day_name", "month_name", "year", "quarter", "fiscal_year", "season"
    )
    logger.info("Current date information:")
    current_date_info.show()


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "gold")

    # Process the date dimension
    process_date_dimension(spark)

    # Run analysis
    analyze_date_dimension(spark)

    logger.info("Date dimension processing complete")
