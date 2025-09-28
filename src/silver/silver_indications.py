# Silver layer transformation for indications data
import os
import sys

sys.path.append("../")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from databricks.connect import DatabricksSession
from utils.config import load_config
from utils.jobs import initialize_job
from utils.logger import setup_logger
from utils.silver_transformations import (
    add_silver_metadata,
    read_latest_partition,
)

config = load_config()
target_catalog = os.getenv("target_catalog")
logger = setup_logger("silver_indications")


def transform_indications(spark) -> DataFrame:
    """
    Transform bronze indications data to silver layer with:
    - Indication standardization
    - Medical condition categorization
    - Only processes latest partition
    """
    logger.info("Starting indications silver transformation")

    df_bronze = read_latest_partition(spark, "indications")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze indications partition"
    )

    df_silver = df_bronze.withColumn(
        "indi_pt_standardized",
        F.upper(F.trim(F.regexp_replace(F.col("indi_pt"), r"[^\w\s]", " "))),
    )

    df_silver = df_silver.withColumn(
        "therapeutic_area",
        F.when(
            F.upper(F.col("indi_pt")).rlike(
                ".*CANCER.*|.*MALIGNANT.*|.*NEOPLASM.*|.*CARCINOMA.*|.*TUMOR.*|.*METASTATIC.*"
            ),
            "Oncology",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(".*DIABETES.*|.*DIABETIC.*"),
            "Endocrinology",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*CARDIAC.*|.*HEART.*|.*HYPERTENSION.*|.*CARDIOVASCULAR.*"
            ),
            "Cardiology",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*DEPRESSION.*|.*ANXIETY.*|.*PSYCHIATRIC.*|.*BIPOLAR.*|.*SCHIZOPHRENIA.*"
            ),
            "Psychiatry",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*INFECTION.*|.*BACTERIAL.*|.*VIRAL.*|.*FUNGAL.*|.*ANTIBIOTIC.*"
            ),
            "Infectious Diseases",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*PAIN.*|.*ARTHRITIS.*|.*RHEUMAT.*|.*INFLAMMATION.*"
            ),
            "Rheumatology",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*ASTHMA.*|.*COPD.*|.*RESPIRATORY.*|.*LUNG.*"
            ),
            "Pulmonology",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*ALZHEIMER.*|.*DEMENTIA.*|.*PARKINSON.*|.*EPILEPSY.*|.*SEIZURE.*"
            ),
            "Neurology",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*GASTRO.*|.*ULCER.*|.*CROHN.*|.*COLITIS.*"
            ),
            "Gastroenterology",
        )
        .otherwise("Other"),
    )

    df_silver = df_silver.withColumn(
        "indication_severity",
        F.when(
            F.upper(F.col("indi_pt")).rlike(
                ".*METASTATIC.*|.*STAGE IV.*|.*TERMINAL.*|.*ADVANCED.*"
            ),
            "Severe",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(
                ".*ACUTE.*|.*SEVERE.*|.*CRISIS.*|.*EMERGENCY.*"
            ),
            "Severe",
        )
        .when(
            F.upper(F.col("indi_pt")).rlike(".*CHRONIC.*|.*MODERATE.*|.*PERSISTENT.*"),
            "Moderate",
        )
        .when(F.upper(F.col("indi_pt")).rlike(".*MILD.*|.*MINOR.*|.*EARLY.*"), "Mild")
        .otherwise("Unspecified"),
    )

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(
        f"Completed indications transformation with {df_silver.count()} records"
    )
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog)

    df_indications_silver = transform_indications(spark)

    logger.info("Writing indications silver data")

    df_indications_silver.write.mode("overwrite").saveAsTable("silver.indications")

    spark.sql("OPTIMIZE silver.indications")

    logger.info("Indications silver transformation complete")
