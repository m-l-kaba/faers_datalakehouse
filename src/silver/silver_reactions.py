# Silver layer transformation for reactions data
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
logger = setup_logger("silver_reactions")


def transform_reactions(spark) -> DataFrame:
    """
    Transform bronze reactions data to silver layer with:
    - Preferred term standardization
    - Reaction categorization
    - Severity classification
    - Only processes latest partition
    """
    logger.info("Starting reactions silver transformation")

    df_bronze = read_latest_partition(spark, "reactions")
    logger.info(
        f"Read {df_bronze.count()} records from latest bronze reactions partition"
    )

    df_silver = df_silver.withColumn(
        "reaction_category",
        F.when(F.upper(F.col("pt")).contains("DEATH"), "Fatal")
        .when(
            F.upper(F.col("pt")).rlike(
                ".*CANCER.*|.*MALIGNANT.*|.*NEOPLASM.*|.*CARCINOMA.*|.*TUMOR.*"
            ),
            "Neoplastic",
        )
        .when(
            F.upper(F.col("pt")).rlike(
                ".*CARDIAC.*|.*HEART.*|.*MYOCARDIAL.*|.*ARRHYTHMIA.*"
            ),
            "Cardiovascular",
        )
        .when(F.upper(F.col("pt")).rlike(".*HEPAT.*|.*LIVER.*|.*JAUNDICE.*"), "Hepatic")
        .when(F.upper(F.col("pt")).rlike(".*RENAL.*|.*KIDNEY.*|.*NEPHRO.*"), "Renal")
        .when(
            F.upper(F.col("pt")).rlike(
                ".*NEURO.*|.*BRAIN.*|.*SEIZURE.*|.*CONVULSION.*"
            ),
            "Neurological",
        )
        .when(
            F.upper(F.col("pt")).rlike(".*RASH.*|.*DERMAT.*|.*SKIN.*|.*ERYTHEMA.*"),
            "Dermatological",
        )
        .when(
            F.upper(F.col("pt")).rlike(".*GASTROINT.*|.*NAUSEA.*|.*VOMIT.*|.*DIARR.*"),
            "Gastrointestinal",
        )
        .when(
            F.upper(F.col("pt")).rlike(".*RESPIR.*|.*LUNG.*|.*PNEUM.*|.*DYSPNEA.*"),
            "Respiratory",
        )
        .when(
            F.upper(F.col("pt")).rlike(".*INFECTION.*|.*SEPSIS.*|.*PNEUMONIA.*"),
            "Infectious",
        )
        .when(
            F.upper(F.col("pt")).rlike(
                ".*PSYCHIATRIC.*|.*DEPRESSION.*|.*ANXIETY.*|.*PSYCHOSIS.*"
            ),
            "Psychiatric",
        )
        .when(
            F.upper(F.col("pt")).rlike(".*METASTASES.*|.*METASTATIC.*|.*PROGRESSION.*"),
            "Disease Progression",
        )
        .otherwise("Other"),
    )

    df_silver = df_silver.withColumn(
        "severity_classification",
        F.when(
            F.upper(F.col("pt")).rlike(".*DEATH.*|.*FATAL.*|.*DIED.*"),
            "Life-threatening",
        )
        .when(
            F.upper(F.col("pt")).rlike(
                ".*SERIOUS.*|.*SEVERE.*|.*ACUTE.*|.*EMERGENCY.*"
            ),
            "Serious",
        )
        .when(
            F.upper(F.col("pt")).rlike(".*HOSPITALI.*|.*ADMIT.*|.*ICU.*"),
            "Hospitalization Required",
        )
        .when(F.upper(F.col("pt")).rlike(".*MILD.*|.*MINOR.*|.*SLIGHT.*"), "Mild")
        .when(F.upper(F.col("pt")).rlike(".*MODERATE.*|.*MEDIUM.*"), "Moderate")
        .otherwise("Unspecified"),
    )

    df_silver = df_silver.withColumn(
        "system_organ_class",
        F.when(F.col("reaction_category") == "Cardiovascular", "Cardiac disorders")
        .when(F.col("reaction_category") == "Hepatic", "Hepatobiliary disorders")
        .when(F.col("reaction_category") == "Renal", "Renal and urinary disorders")
        .when(F.col("reaction_category") == "Neurological", "Nervous system disorders")
        .when(
            F.col("reaction_category") == "Dermatological",
            "Skin and subcutaneous tissue disorders",
        )
        .when(
            F.col("reaction_category") == "Gastrointestinal",
            "Gastrointestinal disorders",
        )
        .when(
            F.col("reaction_category") == "Respiratory",
            "Respiratory, thoracic and mediastinal disorders",
        )
        .when(F.col("reaction_category") == "Infectious", "Infections and infestations")
        .when(F.col("reaction_category") == "Psychiatric", "Psychiatric disorders")
        .when(
            F.col("reaction_category") == "Neoplastic",
            "Neoplasms benign, malignant and unspecified",
        )
        .otherwise("General disorders and administration site conditions"),
    )

    df_silver = df_silver.withColumn(
        "drug_rec_act_description",
        F.when(F.col("drug_rec_act") == "1", "Drug withdrawn")
        .when(F.col("drug_rec_act") == "2", "Dose reduced")
        .when(F.col("drug_rec_act") == "3", "Dose increased")
        .when(F.col("drug_rec_act") == "4", "Dose not changed")
        .when(F.col("drug_rec_act") == "5", "Unknown")
        .when(F.col("drug_rec_act") == "6", "Not applicable")
        .otherwise("No action specified"),
    )

    df_silver = df_silver.withColumn(
        "reaction_priority_score",
        F.when(F.col("severity_classification") == "Life-threatening", 10)
        .when(F.col("severity_classification") == "Serious", 8)
        .when(F.col("severity_classification") == "Hospitalization Required", 7)
        .when(F.col("severity_classification") == "Moderate", 5)
        .when(F.col("severity_classification") == "Mild", 3)
        .otherwise(1),
    )

    df_silver = df_silver.withColumnsRenamed(
        {"primaryid": "primary_id", "caseid": "caseid"}
    )

    df_silver = add_silver_metadata(df_silver)
    logger.info("Added silver metadata")

    logger.info(f"Completed reactions transformation with {df_silver.count()} records")
    return df_silver


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    initialize_job(spark, target_catalog)

    df_reactions_silver = transform_reactions(spark)

    logger.info("Writing reactions silver data")

    df_reactions_silver.write.mode("overwrite").saveAsTable("silver.reactions")

    spark.sql("OPTIMIZE silver.reactions")

    logger.info("Reactions silver transformation complete")
