from src.utils.utils import add_ingestion_metadata
import pytest


@pytest.fixture(scope="session")
def spark_session():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("UnitTests").master("local[1]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


def test_add_ingestion_metadata(spark_session):
    data = [("1", "A"), ("2", "B")]
    columns = ["id", "value"]
    df_raw = spark_session.createDataFrame(data, columns)

    df_bronze = add_ingestion_metadata(df_raw)

    assert "_ingest_ts" in df_bronze.columns
    assert "_source_file" in df_bronze.columns
