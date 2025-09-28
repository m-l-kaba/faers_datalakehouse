from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


def initialize_job(
    spark: SparkSession, catalog: str, schema: Optional[str] = None
) -> None:
    spark.sql(f"USE CATALOG {catalog}")
    if schema:
        spark.sql(f"USE SCHEMA {schema}")


def add_ingestion_metadata(df_raw: DataFrame) -> DataFrame:
    bronze = df_raw.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )

    return bronze
