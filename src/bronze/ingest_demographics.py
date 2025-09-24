# Ingests the demographic data into the bronze layer\
from pyspark.sql.types import StructType, StructField, StringType
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.utils import initialize_bronze_job, add_ingestion_metadata

config = load_config()


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_bronze_job(spark)
    src = config["bronze"]["volume_path"] + "/DEMO25Q1.txt"

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

    bronze.write.format("delta").mode("append").saveAsTable("demographics")
