# Ingests the demographic data into the bronze layer\
import os
import sys

sys.path.append("../")

from pyspark.sql.types import StructType, StructField, StringType
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job, add_ingestion_metadata
from utils.logger import setup_logger

config = load_config()
target_catalog = os.getenv("target_catalog")
src = config["bronze"]["volume_path"] + "/year=2025/quarter=1/DEMO25Q1.txt"
logger = setup_logger("ingest_demographics")

if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "bronze")
    logger.info("Ingesting demographics data from %s", src)

    schema = StructType(
        [
            StructField("primaryid", StringType(), True),
            StructField("caseid", StringType(), True),
            StructField("caseversion", StringType(), True),
            StructField("i_f_code", StringType(), True),
            StructField("event_dt", StringType(), True),
            StructField("mfr_dt", StringType(), True),
            StructField("init_fda_dt", StringType(), True),
            StructField("fda_dt", StringType(), True),
            StructField("rept_cod", StringType(), True),
            StructField("auth_num", StringType(), True),
            StructField("mfr_num", StringType(), True),
            StructField("mfr_sndr", StringType(), True),
            StructField("lit_ref", StringType(), True),
            StructField("age", StringType(), True),
            StructField("age_cod", StringType(), True),
            StructField("age_grp", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("e_sub", StringType(), True),
            StructField("wt", StringType(), True),
            StructField("wt_cod", StringType(), True),
            StructField("rept_dt", StringType(), True),
            StructField("to_mfr", StringType(), True),
            StructField("occp_cod", StringType(), True),
            StructField("reporter_country", StringType(), True),
            StructField("occr_country", StringType(), True),
        ]
    )

    df_raw = (
        spark.read.option("sep", "$")
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("emptyValue", None)
        .schema(schema)
        .csv(src)
    )

    bronze = add_ingestion_metadata(df_raw)

    logger.info("Writing demographics data to bronze layer")
    bronze.write.format("delta").mode("append").saveAsTable("demographics")
    logger.info("Demographics data ingestion complete")
