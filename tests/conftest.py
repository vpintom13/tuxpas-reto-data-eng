import os
import sys
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark():
    """
    Provides a single SparkSession configured for Delta Lake for the entire test session.
    Shared across all test files.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("cafe-altura-test").master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()

def load_csv_as_df(spark, path):
    """
    Helper to load a CSV file as a Spark DataFrame with inferred schema.
    """
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
