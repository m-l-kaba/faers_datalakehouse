# Ingests the drug details data into the bronze layer
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
src = config["bronze"]["volume_path"] + "/year=2025/quarter=1/DRUG25Q1.txt"
logger = setup_logger("ingest_drug_details")

if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "bronze")
    logger.info("Ingesting drug details data from %s", src)

    schema = StructType(
        [
            StructField("primaryid", StringType(), True),
            StructField("caseid", StringType(), True),
            StructField("drug_seq", StringType(), True),
            StructField("role_cod", StringType(), True),
            StructField("drugname", StringType(), True),
            StructField("prod_ai", StringType(), True),
            StructField("val_vbm", StringType(), True),
            StructField("route", StringType(), True),
            StructField("dose_vbm", StringType(), True),
            StructField("cum_dose_chr", StringType(), True),
            StructField("cum_dose_unit", StringType(), True),
            StructField("dechal", StringType(), True),
            StructField("rechal", StringType(), True),
            StructField("lot_num", StringType(), True),
            StructField("exp_dt", StringType(), True),
            StructField("nda_num", StringType(), True),
            StructField("dose_amt", StringType(), True),
            StructField("dose_unit", StringType(), True),
            StructField("dose_form", StringType(), True),
            StructField("dose_freq", StringType(), True),
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

    logger.info("Writing drug details data to bronze layer")
    bronze.write.format("delta").mode("append").saveAsTable("drug_details")
    logger.info("Drug details data ingestion complete")
