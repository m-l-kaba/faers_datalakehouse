import pytest
from src.utils.silver_transformations import (
    standardize_date_fields,
    standardize_numeric_fields,
    calculate_age_groups,
    standardize_drug_names,
    parse_dosage_information,
)
from pyspark.testing import assertDataFrameEqual
from datetime import date


@pytest.fixture(scope="session")
def spark_session():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("UnitTests").master("local[1]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


def test_standardize_date_fields(spark_session):
    data = [
        ("20230101", "202301", "2023", ""),
        ("20231231", "202312", "1999", "invalid"),
    ]
    columns = ["date_full", "date_month", "date_year", "date_empty"]
    df_raw = spark_session.createDataFrame(data, columns)

    expected_data = [
        (date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1), None),
        (date(2023, 12, 31), date(2023, 12, 1), date(1999, 1, 1), None),
    ]

    schema = "date_full DATE, date_month DATE, date_year DATE, date_empty DATE"
    expected_df = spark_session.createDataFrame(expected_data, schema)

    df_transformed = standardize_date_fields(df_raw, columns)

    assertDataFrameEqual(
        df_transformed,
        expected_df,
    )


def test_standarize_numeric_fields(spark_session):
    data = [("25", "70.5"), ("invalid", "not_a_number"), ("30.0", "100")]
    columns = ["age", "weight"]
    df_raw = spark_session.createDataFrame(data, columns)

    expected_data = [
        (25.0, 70.5),
        (None, None),
        (30.0, 100.0),
    ]
    schema = "age DOUBLE, weight DOUBLE"
    expected_df = spark_session.createDataFrame(expected_data, schema)
    actual_df = standardize_numeric_fields(df_raw, columns)
    assertDataFrameEqual(
        actual_df,
        expected_df,
    )


def test_calculate_age_groups(spark_session):
    data = [(5,), (25,), (70,), (None,)]
    columns = ["age"]
    df_raw = spark_session.createDataFrame(data, columns)

    expected_data = [
        (5, "Pediatric (0-17)"),
        (25, "Adult (18-64)"),
        (70, "Elderly (65+)"),
        (None, "Unknown"),
    ]
    schema = "age LONG, age_group STRING"
    expected_df = spark_session.createDataFrame(expected_data, schema)

    df_transformed = calculate_age_groups(df_raw, "age")

    assertDataFrameEqual(
        df_transformed,
        expected_df,
    )


def test_standardize_drug_names(spark_session):
    """Test drug name standardization - uppercase, trim whitespace, remove special characters"""
    data = [
        ("aspirin",),
        ("  Tylenol  ",),
        ("ibuprofen-400mg",),
        ("Advil (R)",),
        ("",),
        (None,),
    ]
    columns = ["drugname"]
    df_raw = spark_session.createDataFrame(data, columns)

    expected_data = [
        ("ASPIRIN",),
        ("TYLENOL",),
        ("IBUPROFEN400MG",),
        ("ADVIL R",),
        ("",),
        (None,),
    ]
    schema = "drugname STRING"
    expected_df = spark_session.createDataFrame(expected_data, schema)

    df_transformed = standardize_drug_names(df_raw, "drugname")

    assertDataFrameEqual(
        df_transformed,
        expected_df,
    )


def test_parse_dosage_information(spark_session):
    """Test parsing dosage information from dose_vbm field"""
    data = [
        ("10 mg, TWICE DAILY",),
        ("25.5 tablets, ONCE",),
        ("500 units",),
        ("unknown dose",),
        ("",),
        (None,),
    ]
    columns = ["dose_vbm"]
    df_raw = spark_session.createDataFrame(data, columns)

    expected_data = [
        ("10 mg, TWICE DAILY", 10.0, "MG", "TWICE DAILY"),
        ("25.5 tablets, ONCE", 25.5, "TABLETS", "ONCE"),
        ("500 units", 500.0, "UNITS", ""),
        ("unknown dose", None, "", ""),
        ("", None, "", ""),
        (None, None, "", ""),
    ]
    schema = "dose_vbm STRING, dose DOUBLE, dose_unit STRING, dose_frequency STRING"
    expected_df = spark_session.createDataFrame(expected_data, schema)

    df_transformed = parse_dosage_information(df_raw)

    assertDataFrameEqual(
        df_transformed,
        expected_df,
    )
