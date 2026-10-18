import os
import sys
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from src.pipeline import run_pipeline

def get_spark_session():
    """
    Initializes and configures a single SparkSession for local execution.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("cafe-altura-local").master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":
    load_dotenv()
    spark = get_spark_session()
    try:
        run_pipeline(spark)
    finally:
        spark.stop()
