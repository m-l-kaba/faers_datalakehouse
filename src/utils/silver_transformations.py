# Utility functions for silver layer transformations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    length,
    to_date,
    concat,
    lit,
    when,
    col,
    upper,
    trim,
    regexp_replace,
    regexp_extract,
    current_timestamp,
    current_date,
)
from pyspark.sql.types import DoubleType
from typing import List


def read_latest_partition(
    spark: SparkSession, table_name: str, schema: str = "bronze"
) -> DataFrame:
    """
    Read only the latest partition from a bronze table based on _ingest_ts.
    Simple solution for incremental processing.

    Args:
        spark: SparkSession
        table_name: Name of the table to read from

    Returns:
        DataFrame with only the latest partition data
    """

    latest_partition = spark.sql(
        f"""
        SELECT MAX(_ingest_ts) as max_ingest_ts 
        FROM {schema}.{table_name}
    """
    ).collect()[0]["max_ingest_ts"]

    return spark.table(f"{schema}/{table_name}").filter(
        col("_ingest_ts") == latest_partition
    )


def standardize_date_fields(df: DataFrame, date_columns: List[str]) -> DataFrame:
    """
    Standardize date fields from various FAERS date formats to proper date type.
    FAERS dates can be in formats: YYYYMMDD, YYYYMM, YYYY, or empty
    """
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(
                    length(col(col_name)) == 8,
                    to_date(col(col_name), "yyyyMMdd"),
                )
                .when(
                    length(col(col_name)) == 6,
                    to_date(concat(col(col_name), lit("01")), "yyyyMMdd"),
                )
                .when(
                    length(col(col_name)) == 4,
                    to_date(concat(col(col_name), lit("0101")), "yyyyMMdd"),
                )
                .otherwise(None),
            )
    return df


def standardize_numeric_fields(df: DataFrame, numeric_columns: List[str]) -> DataFrame:
    """
    Convert string numeric fields to proper numeric types with validation.
    """
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(
                    col(col_name).rlike("^[0-9]+\.?[0-9]*$"),
                    col(col_name).cast(DoubleType()),
                ).otherwise(None),
            )
    return df


def calculate_age_groups(df: DataFrame, age_column: str = "age") -> DataFrame:
    """
    Calculate standardized age groups from numeric age.
    """
    if age_column in df.columns:
        df = df.withColumn(
            "age_group",
            when(col(age_column) < 18, "Pediatric (0-17)")
            .when((col(age_column) >= 18) & (col(age_column) < 65), "Adult (18-64)")
            .when(col(age_column) >= 65, "Elderly (65+)")
            .otherwise("Unknown"),
        ).drop("age_grp")
    return df


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """
    Add silver layer specific metadata columns.
    """
    return df.withColumn("silver_processed_ts", current_timestamp()).withColumn(
        "silver_processing_date", current_date()
    )


def standardize_drug_names(
    df: DataFrame, drugname_column: str = "drugname"
) -> DataFrame:
    """
    Standardize drug names for consistency.
    """
    if drugname_column in df.columns:
        df = df.withColumn(
            drugname_column,
            upper(trim(regexp_replace(col(drugname_column), r"[^\w\s]", ""))),
        )
    return df


def parse_dosage_information(df: DataFrame) -> DataFrame:
    """
    Parse and standardize dosage information from dose_vbm field.
    """
    if "dose_vbm" in df.columns:
        df = df.withColumn(
            "dose",
            when(
                regexp_extract(col("dose_vbm"), r"(\d+\.?\d*)", 1) != "",
                regexp_extract(col("dose_vbm"), r"(\d+\.?\d*)", 1).cast(DoubleType()),
            ).otherwise(None),
        )

        df = df.withColumn(
            "dose_unit",
            when(
                regexp_extract(col("dose_vbm"), r"\d+\.?\d*\s*(\w+)", 1) != "",
                upper(trim(regexp_extract(col("dose_vbm"), r"\d+\.?\d*\s*(\w+)", 1))),
            ).otherwise(""),
        )

        df = df.withColumn(
            "dose_frequency",
            when(
                regexp_extract(col("dose_vbm"), r",(.*)", 1) != "",
                upper(trim(regexp_extract(col("dose_vbm"), r",(.*)", 1))),
            ).otherwise(""),
        )

    return df
