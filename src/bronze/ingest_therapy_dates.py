# Ingests the therapy dates data into the bronze layer
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
src = config["bronze"]["volume_path"] + "/year=2025/quarter=1/THER25Q1.txt"
logger = setup_logger("ingest_therapy_dates")

if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "bronze")
    logger.info("Ingesting therapy dates data from %s", src)

    schema = StructType(
        [
            StructField("primaryid", StringType(), True),
            StructField("caseid", StringType(), True),
            StructField("dsg_drug_seq", StringType(), True),
            StructField("start_dt", StringType(), True),
            StructField("end_dt", StringType(), True),
            StructField("dur", StringType(), True),
            StructField("dur_cod", StringType(), True),
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

    logger.info("Writing therapy dates data to bronze layer")
    bronze.write.format("delta").mode("append").saveAsTable("therapy_dates")
    logger.info("Therapy dates data ingestion complete")
