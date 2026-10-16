from decimal import Decimal
import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.testing import assertDataFrameEqual
from delta import configure_spark_with_delta_pip
from scr.transform_silver_ventas import process_pos_sales, process_web_sales, unify_sales
from scr.schemas import SCHEMA_STG_MAPEO_SKU, SCHEMA_STG_PRODUCTOS, SCHEMA_STG_TIPO_CAMBIO, SCHEMA_STG_VENTAS_POS, SCHEMA_STG_PEDIDOS_WEB

@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("test-silver").master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

@pytest.fixture
def mock_mapeo_sku(spark):
    schema = SCHEMA_STG_MAPEO_SKU
    data = [("SKU1", "ERP1", "WEB1"), ("SKU2", "ERP2", "WEB2")]
    return spark.createDataFrame(data, schema=schema)

@pytest.fixture
def mock_productos(spark):
    schema = SCHEMA_STG_PRODUCTOS
    data = [
        ("ERP1", "Prod 1", "CAFETERIA"),
        ("ERP2", "Prod 2", "servicio"), # Should be filtered
    ]
    return spark.createDataFrame(data, schema=schema)

@pytest.fixture
def mock_tipo_cambio(spark):
    schema = SCHEMA_STG_TIPO_CAMBIO
    data = [(date(2023, 1, 1), Decimal("3.70"))]
    return spark.createDataFrame(data, schema=schema)

def test_process_pos_sales_filters(spark, mock_mapeo_sku, mock_productos):
    schema = SCHEMA_STG_VENTAS_POS

    data = [
        # Valid
        ("01", "S1", 1, 1, datetime(2023, 1, 1, 10, 0), "T1", "SKU1", "Desc 1", 1, Decimal("11.80"), "EMITIDO", None, datetime(2023, 1, 1, 10, 0)),
        # Invalid Comprobante (05)
        ("05", "S1", 2, 1, datetime(2023, 1, 1, 10, 0), "T1", "SKU1", "Desc 1", 1, Decimal("11.80"), "EMITIDO", None, datetime(2023, 1, 1, 10, 0)),
        # Invalid Estado (ANULADO)
        ("03", "S1", 3, 1, datetime(2023, 1, 1, 10, 0), "T1", "SKU1", "Desc 1", 1, Decimal("11.80"), "ANULADO", None, datetime(2023, 1, 1, 10, 0)),
        # Invalid Category (servicio)
        ("01", "S1", 4, 1, datetime(2023, 1, 1, 10, 0), "T1", "SKU2", "Desc 2", 1, Decimal("11.80"), "EMITIDO", None, datetime(2023, 1, 1, 10, 0)),
    ]
    df_pos = spark.createDataFrame(data, schema=schema)

    result = process_pos_sales(df_pos, mock_mapeo_sku, mock_productos)

    # Only the first record should remain
    assert result.count() == 1
    assert result.collect()[0]["id_tienda"] == "T1"

def test_process_pos_sales_join_case_and_trim(spark, mock_mapeo_sku, mock_productos):
    schema = SCHEMA_STG_VENTAS_POS

    # Case: "  sku1  " should match "SKU1"
    data = [
        ("01", "S1", 1, 1, datetime(2023, 1, 1, 10, 0), "T1", "  sku1  ", "Desc 1", 1, Decimal("11.80"), "EMITIDO", None, datetime(2023, 1, 1, 10, 0)),
    ]
    df_pos = spark.createDataFrame(data, schema=schema)

    result = process_pos_sales(df_pos, mock_mapeo_sku, mock_productos)

    assert result.count() == 1
    assert result.collect()[0]["sku_erp"] == "ERP1"

def test_process_web_sales_filters(spark, mock_mapeo_sku, mock_productos, mock_tipo_cambio):
    schema = SCHEMA_STG_PEDIDOS_WEB

    data = [
        # Valid
        ("W1", 1, datetime(2023, 1, 1), "WEB1", 1, Decimal("11.80"), "PEN", "PAGADO", "email1", "doc1", datetime(2023, 1, 1)),
        # Invalid Estado
        ("W2", 1, datetime(2023, 1, 1), "WEB1", 1, Decimal("11.80"), "PEN", "CANCELADO", "email2", "doc2", datetime(2023, 1, 1)),
        # Invalid Category (servicio)
        ("W3", 1, datetime(2023, 1, 1), "WEB2", 1, Decimal("11.80"), "PEN", "PAGADO", "email3", "doc3", datetime(2023, 1, 1)),
    ]
    df_web = spark.createDataFrame(data, schema=schema)

    result = process_web_sales(df_web, mock_mapeo_sku, mock_productos, mock_tipo_cambio)

    assert result.count() == 1
    assert result.collect()[0]["ticket"] == "W1"

def test_unify_sales(spark):
    # Testing unionByName: verify that two DFs with the same schema unify correctly
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("nombre", StringType(), True),
    ])

    data_pos = [("1", "Tienda 1")]
    data_web = [("W1", "Tienda Web")]

    df_pos = spark.createDataFrame(data_pos, schema=schema)
    df_web = spark.createDataFrame(data_web, schema=schema)

    result = unify_sales(df_pos, df_web)

    assert result.count() == 2
    # Verify that columns are correctly preserved
    row = result.collect()[0]
    assert "id" in row and "nombre" in row
