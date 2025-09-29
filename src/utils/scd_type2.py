# Slowly Changing Dimension Type 2 utilities for FAERS data lakehouse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    current_date,
    md5,
    concat_ws,
    coalesce,
)
from pyspark.sql.types import DateType, BooleanType
from typing import List
from utils.logger import setup_logger

logger = setup_logger("scd_type2")


def add_scd_metadata(df: DataFrame) -> DataFrame:
    """
    Add standard SCD Type 2 metadata columns to a DataFrame.

    Args:
        df: Source DataFrame

    Returns:
        DataFrame with SCD metadata columns added
    """
    return (
        df.withColumn("effective_date", current_date())
        .withColumn("end_date", lit(None).cast(DateType()))
        .withColumn("is_current", lit(True).cast(BooleanType()))
        .withColumn("created_ts", current_timestamp())
        .withColumn("updated_ts", current_timestamp())
    )


def generate_surrogate_key(
    df: DataFrame, business_keys: List[str], surrogate_key_col: str = "dim_key"
) -> DataFrame:
    """
    Generate hash-based surrogate keys based on business keys and effective date.
    This ensures uniqueness across different versions of the same business key.

    Args:
        df: Source DataFrame
        business_keys: List of business key columns
        surrogate_key_col: Name of surrogate key column

    Returns:
        DataFrame with surrogate key added
    """
    key_cols = business_keys + ["effective_date"]
    return df.withColumn(
        surrogate_key_col,
        md5(
            concat_ws(
                "||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in key_cols]
            )
        ),
    )


def create_change_hash(
    df: DataFrame, tracked_columns: List[str], hash_col: str = "row_hash"
) -> DataFrame:
    """
    Create a hash of tracked columns for change detection.

    Args:
        df: Source DataFrame
        tracked_columns: List of columns to track for changes
        hash_col: Name of hash column

    Returns:
        DataFrame with row hash added
    """
    return df.withColumn(
        hash_col,
        md5(
            concat_ws(
                "||",
                *[
                    coalesce(col(c).cast("string"), lit("NULL"))
                    for c in tracked_columns
                ],
            )
        ),
    )


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if a table exists in the catalog.

    Args:
        spark: SparkSession
        table_name: Name of the table to check

    Returns:
        Boolean indicating if table exists
    """
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception as e:
        logger.debug(f"Table {table_name} doesn't exist or is not accessible: {str(e)}")
        return False


def apply_scd_type2_merge(
    spark: SparkSession,
    target_table: str,
    new_data: DataFrame,
    business_keys: List[str],
    tracked_columns: List[str],
    dim_key_col: str = "dim_key",
) -> None:
    """
    Apply SCD Type 2 logic using Delta Lake MERGE statement for optimal performance.

    This implementation:
    1. Uses a single MERGE to handle closing old records and inserting new business keys
    2. Uses a second MERGE to insert new versions of changed records
    3. Leverages Delta Lake's optimized MERGE operations

    Args:
        spark: SparkSession
        target_table: Name of target dimension table
        new_data: New data to process
        business_keys: List of business key columns
        tracked_columns: List of columns to track for changes
        dim_key_col: Surrogate key column name
    """
    logger.info(f"Starting SCD Type 2 MERGE processing for table {target_table}")

    new_data_prepared = create_change_hash(new_data, tracked_columns)
    new_data_prepared = add_scd_metadata(new_data_prepared)
    new_data_prepared = generate_surrogate_key(
        new_data_prepared, business_keys, dim_key_col
    )

    logger.info(f"Prepared {new_data_prepared.count()} records for processing")

    if not table_exists(spark, target_table):
        logger.info(
            f"Target table {target_table} doesn't exist - performing initial load"
        )
        new_data_prepared.write.format("delta").mode("overwrite").option(
            "delta.autoOptimize.optimizeWrite", "true"
        ).saveAsTable(target_table)
        logger.info(f"Initial load complete for {target_table}")
        return

    temp_view_name = f"temp_new_data_{target_table.replace('.', '_')}"
    new_data_prepared.createOrReplaceTempView(temp_view_name)

    try:
        business_key_joins = " AND ".join(
            [f"target.{key} = source.{key}" for key in business_keys]
        )

        all_columns = (
            [dim_key_col]
            + business_keys
            + tracked_columns
            + [
                "row_hash",
                "effective_date",
                "end_date",
                "is_current",
                "created_ts",
                "updated_ts",
            ]
        )

        merge_sql_1 = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view_name} AS source
        ON {business_key_joins} AND target.is_current = true
        
        WHEN MATCHED AND target.row_hash != source.row_hash THEN
            UPDATE SET
                end_date = current_date(),
                is_current = false,
                updated_ts = current_timestamp()
        
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(all_columns)})
            VALUES ({', '.join([f'source.{col}' for col in all_columns])})
        """

        logger.info(
            "Executing first MERGE to close old records and insert new business keys"
        )
        spark.sql(merge_sql_1)

        merge_sql_2 = f"""
        MERGE INTO {target_table} AS target
        USING (
            SELECT DISTINCT source.*
            FROM {temp_view_name} source
            INNER JOIN (
                SELECT {', '.join(business_keys)}
                FROM {target_table}
                WHERE is_current = false AND end_date = current_date()
            ) closed_today
            ON {' AND '.join([f'source.{key} = closed_today.{key}' for key in business_keys])}
        ) AS source
        ON false
        
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(all_columns)})
            VALUES ({', '.join([f'source.{col}' for col in all_columns])})
        """

        logger.info("Executing second MERGE to insert new versions of updated records")
        spark.sql(merge_sql_2)

        logger.info(f"SCD Type 2 MERGE processing complete for {target_table}")

    finally:
        try:
            spark.catalog.dropTempView(temp_view_name)
        except Exception as e:
            logger.warning(f"Could not drop temp view {temp_view_name}: {str(e)}")
