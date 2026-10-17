import os
import sys
import argparse
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
builder = (
    SparkSession.builder.appName("cafe-altura-gold").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

def generate_margin_report(df_sales):
    """
    Core business logic for the Monthly Margin Report.
    Takes silver sales data and returns the aggregated gold report.
    """
    # 1. Final Aggregation
    agg = df_sales.groupBy("mes", "id_tienda", "categoria").agg(
        F.sum("unidades").alias("unidades"),
        F.countDistinct("ticket").alias("tickets"),
        F.sum("venta_sin_igv").alias("venta_neta"),
        F.sum("costo").cast(DecimalType(14, 2)).alias("costo")
    )

    # 2. Report metrics
    reporte = agg.select(
        "mes",
        "id_tienda",
        "categoria",
        "unidades",
        "tickets",
        (F.col("unidades") / F.when(F.col("tickets") == 0, None).otherwise(F.col("tickets"))).cast(IntegerType()).alias("unidades_por_ticket"),
        "venta_neta",
        "costo",
        (F.col("venta_neta") - F.col("costo")).alias("margen"),
        (F.lit(100.0) * (F.col("venta_neta") - F.col("costo")) / F.when(F.col("venta_neta") == 0, None).otherwise(F.col("venta_neta")))
            .cast(DecimalType(9, 2)).alias("margen_pct")
    )
    return reporte

def run_report(delta_lake_path, output_csv_path):
    """
    Orchestrator that handles data loading, calling the report logic, and exporting results.
    """
    # Load data
    silver_ventas = spark.read.format("delta").load(f"{delta_lake_path}/silver/silver_ventas_unificadas")

    # Generate report
    reporte = generate_margin_report(silver_ventas)

    # Save as Delta table in Gold
    gold_path = f"{delta_lake_path}/gold/reporte_margen_mensual"
    reporte.write.format("delta").mode("overwrite").save(gold_path)

    # Export to CSV
    reporte.write.mode("overwrite").option("header", "true").csv(output_csv_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Layer: Monthly Margin Report")
    parser.add_argument("--delta_lake_path", required=True, help="Root path for Delta Lake")
    parser.add_argument("--output_csv_path", required=True, help="Path where final CSV should be saved")
    args = parser.parse_args()

    run_report(args.delta_lake_path, args.output_csv_path)
    spark.stop()
