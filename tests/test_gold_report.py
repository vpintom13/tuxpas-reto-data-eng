from decimal import Decimal
import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, DateType, LongType
from pyspark.testing import assertDataFrameEqual
from src.reporte_gold_margen import generate_margin_report

@pytest.fixture
def report_schema():
    return StructType([
        StructField("mes", DateType(), True),
        StructField("id_tienda", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("unidades", LongType(), True),
        StructField("tickets", LongType(), True),
        StructField("unidades_por_ticket", IntegerType(), True),
        StructField("venta_neta", DecimalType(22, 2), True),
        StructField("costo", DecimalType(14, 2), True),
        StructField("margen", DecimalType(23, 2), True),
        StructField("margen_pct", DecimalType(9, 2), True),
    ])

@pytest.fixture
def mock_silver_ventas(spark):
    schema = StructType([
        StructField("id_tienda", StringType(), True),
        StructField("mes", DateType(), True),
        StructField("sku_erp", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("ticket", StringType(), True),
        StructField("unidades", IntegerType(), True),
        StructField("venta_sin_igv", DecimalType(12, 2), True),
        StructField("costo", DecimalType(14, 4), True),
    ])

    data = [
        # Store 1, Jan, Cat A: 4 units, 2 tickets, 40.00 venta, 19.00 costo -> margen 21.00, pct 52.50
        ("T1", date(2023, 1, 1), "SKU1", "Categoria A", "TK1", 2, Decimal("20.00"), Decimal("10.00")),
        ("T1", date(2023, 1, 1), "SKU2", "Categoria A", "TK1", 1, Decimal("10.00"), Decimal("4.00")),
        ("T1", date(2023, 1, 1), "SKU1", "Categoria A", "TK2", 1, Decimal("10.00"), Decimal("5.00")),

        # Store 1, Jan, Cat B: 1 unit, 1 ticket, 15.00 venta, 12.00 costo -> margen 3.00, pct 20.00
        ("T1", date(2023, 1, 1), "SKU3", "Categoria B", "TK3", 1, Decimal("15.00"), Decimal("12.00")),

        # Store 2, Jan, Cat A: 1 unit, 1 ticket, 10.00 venta, 5.00 costo -> margen 5.00, pct 50.00
        ("T2", date(2023, 1, 1), "SKU1", "Categoria A", "TK4", 1, Decimal("10.00"), Decimal("5.00")),

        # Store 1, Feb, Cat A: 1 unit, 1 ticket, 10.00 venta, 5.00 costo -> margen 5.00, pct 50.00
        ("T1", date(2023, 2, 1), "SKU1", "Categoria A", "TK5", 1, Decimal("10.00"), Decimal("5.00")),

        # Edge Case: Zero sales
        ("T3", date(2023, 1, 1), "SKU_ZERO", "Categoria Z", "TKZ", 0, Decimal("0.00"), Decimal("0.00")),
    ]
    return spark.createDataFrame(data, schema=schema)

def test_generate_margin_report_schema(spark, mock_silver_ventas, report_schema):
    result = generate_margin_report(mock_silver_ventas)

    expected_columns = [field.name for field in report_schema.fields]

    assert result.columns == expected_columns

def test_generate_margin_report_full_aggregation(spark, mock_silver_ventas, report_schema):
    result = generate_margin_report(mock_silver_ventas)

    # Expected data mapping all combinations in the mock
    expected_data = [
        # mes, id_tienda, categoria, unidades, tickets, unidades_por_ticket, venta_neta, costo, margen, margen_pct
        (date(2023, 1, 1), "T1", "Categoria A", 4, 2, 2, Decimal("40.00"), Decimal("19.00"), Decimal("21.00"), Decimal("52.50")),
        (date(2023, 1, 1), "T1", "Categoria B", 1, 1, 1, Decimal("15.00"), Decimal("12.00"), Decimal("3.00"), Decimal("20.00")),
        (date(2023, 1, 1), "T2", "Categoria A", 1, 1, 1, Decimal("10.00"), Decimal("5.00"), Decimal("5.00"), Decimal("50.00")),
        (date(2023, 2, 1), "T1", "Categoria A", 1, 1, 1, Decimal("10.00"), Decimal("5.00"), Decimal("5.00"), Decimal("50.00")),
        (date(2023, 1, 1), "T3", "Categoria Z", 0, 1, 0, Decimal("0.00"), Decimal("0.00"), Decimal("0.00"), None),
    ]

    expected_df = spark.createDataFrame(expected_data, schema=report_schema)

    # Compare results
    assertDataFrameEqual(result, expected_df)
