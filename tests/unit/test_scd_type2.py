"""
Unit tests for SCD Type 2 utility functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from utils.scd_type2 import (
    add_scd_metadata,
    generate_surrogate_key,
    create_change_hash,
    table_exists,
)


@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder.appName("SCD_Type2_UnitTests")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )

    data = [
        ("C001", "John Doe", "Active"),
        ("C002", "Jane Smith", "Inactive"),
    ]

    return spark_session.createDataFrame(data, schema)


def test_add_scd_metadata(sample_dataframe):
    """Test that SCD metadata columns are added correctly."""
    result_df = add_scd_metadata(sample_dataframe)

    # Check that all SCD columns are added
    expected_columns = {
        "effective_date",
        "end_date",
        "is_current",
        "created_ts",
        "updated_ts",
    }
    actual_columns = set(result_df.columns)
    assert expected_columns.issubset(actual_columns)

    # Check that all rows have is_current = True and end_date = NULL
    assert result_df.filter(col("is_current") == True).count() == result_df.count()
    assert result_df.filter(col("end_date").isNull()).count() == result_df.count()


def test_generate_surrogate_key(sample_dataframe):
    """Test that surrogate keys are generated correctly."""
    df_with_metadata = add_scd_metadata(sample_dataframe)
    result_df = generate_surrogate_key(df_with_metadata, ["customer_id"], "dim_key")

    # Check that surrogate key column is added
    assert "dim_key" in result_df.columns

    # Check that all surrogate keys are not null
    assert result_df.filter(col("dim_key").isNotNull()).count() == result_df.count()


def test_create_change_hash(sample_dataframe):
    """Test that change hash is created correctly."""
    result_df = create_change_hash(sample_dataframe, ["customer_name", "status"])

    # Check that hash column is added
    assert "row_hash" in result_df.columns

    # Check that all hashes are not null
    assert result_df.filter(col("row_hash").isNotNull()).count() == result_df.count()


def test_table_exists(spark_session, sample_dataframe):
    """Test table existence checking."""
    # Test non-existent table
    assert table_exists(spark_session, "non_existent_table") == False

    # Test existing temp view
    sample_dataframe.createOrReplaceTempView("test_view")
    assert table_exists(spark_session, "test_view") == True
    spark_session.catalog.dropTempView("test_view")
