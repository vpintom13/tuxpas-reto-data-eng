import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.testing import assertDataFrameEqual
from delta import configure_spark_with_delta_pip
from scr.ingesta_bronze_stg import merge_upsert

@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("test-bronze").master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

@pytest.fixture
def test_data_schema():
    return StructType([
        StructField("id", StringType(), False),
        StructField("valor", StringType(), True),
        StructField("fecha_actualizacion", StringType(), False)
    ])

def test_merge_upsert_initial_deduplication(spark, tmp_path, test_data_schema):
    # 1. Setup data with internal duplicates for the same PK
    # PK "1" has two versions: 2023-01-01 and 2023-01-02 (Winner)
    data = [
        ("1", "Version Old", "2023-01-01 10:00:00"),
        ("1", "Version New", "2023-01-02 10:00:00"),
        ("2", "Unique", "2023-01-01 10:00:00"),
    ]
    df = spark.createDataFrame(data, schema=test_data_schema)

    target_path = str(tmp_path / "test_table")
    pk_cols = ["id"]

    # Execute merge_upsert (Initial Load)
    merge_upsert(df, target_path, pk_cols)

    # Verify
    result = spark.read.format("delta").load(target_path)

    # Expect: 2 rows, PK "1" should be "Version New"
    expected_data = [
        ("1", "Version New", "2023-01-02 10:00:00"),
        ("2", "Unique", "2023-01-01 10:00:00"),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=test_data_schema)

    assertDataFrameEqual(result, expected_df)

def test_merge_upsert_incremental_complex(spark, tmp_path, test_data_schema):
    # 1. Lote 1: Initial Base Data
    lote1_data = [
        ("1", "L1-V1", "2023-01-01 10:00:00"),
        ("2", "L1-V1", "2023-01-01 10:00:00"),
    ]
    df_lote1 = spark.createDataFrame(lote1_data, schema=test_data_schema)
    target_path = str(tmp_path / "incremental_table")
    pk_cols = ["id"]

    merge_upsert(df_lote1, target_path, pk_cols)

    # 2. Lote 2: Complex mix
    lote2_data = [
        # Case A: New record (Insert)
        ("3", "L2-New", "2023-01-02 10:00:00"),
        # Case B: Existing PK, newer date (Update)
        ("1", "L2-Updated", "2023-01-02 10:00:00"),
        # Case C: Existing PK, older date (Ignore)
        ("2", "L2-Older", "2022-12-31 10:00:00"),
        # Case D: Internal duplicates in Lote 2 (Must deduplicate to latest)
        ("4", "L2-Dup-Old", "2023-01-01 10:00:00"),
        ("4", "L2-Dup-New", "2023-01-02 10:00:00"),
    ]
    df_lote2 = spark.createDataFrame(lote2_data, schema=test_data_schema)

    merge_upsert(df_lote2, target_path, pk_cols)

    # Verification
    result = spark.read.format("delta").load(target_path)

    # Expected outcome:
    # id 1 -> Updated by Lote 2
    # id 2 -> Kept from Lote 1 (Lote 2 was older)
    # id 3 -> Inserted from Lote 2
    # id 4 -> Inserted from Lote 2 (latest version)
    expected_data = [
        ("1", "L2-Updated", "2023-01-02 10:00:00"),
        ("2", "L1-V1", "2023-01-01 10:00:00"),
        ("3", "L2-New", "2023-01-02 10:00:00"),
        ("4", "L2-Dup-New", "2023-01-02 10:00:00"),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=test_data_schema)

    assertDataFrameEqual(result, expected_df)
