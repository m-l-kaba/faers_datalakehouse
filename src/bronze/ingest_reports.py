# Ingests the reports data into the bronze layer
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
src = config["bronze"]["volume_path"] + "/year=2025/quarter=1/RPSR25Q1.txt"
logger = setup_logger("ingest_reports")

if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog, "bronze")
    logger.info("Ingesting reports data from %s", src)

    schema = StructType(
        [
            StructField("primaryid", StringType(), True),
            StructField("caseid", StringType(), True),
            StructField("rpsr_cod", StringType(), True),
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

    logger.info("Writing reports data to bronze layer")
    bronze.write.format("delta").mode("append").saveAsTable("reports")
    logger.info("Reports data ingestion complete")
