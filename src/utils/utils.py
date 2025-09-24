from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


def initialize_bronze_job(spark: SparkSession):
    spark.sql("USE CATALOG production")
    spark.sql("USE SCHEMA bronze")


def add_ingestion_metadata(df_raw: DataFrame) -> DataFrame:
    bronze = df_raw.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )

    return bronze
