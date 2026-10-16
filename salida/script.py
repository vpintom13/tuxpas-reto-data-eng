import os
import sys
import argparse

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
builder = (
    SparkSession.builder.appName("cafe-altura").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

###CLAUDE INSERT CODE HERE####
def run_migration():
    parser = argparse.ArgumentParser(description="Migrate Cafe Altura Monthly Margin Report")
    parser.add_argument("--input_path", required=True, help="Path to the directory containing input CSVs")
    parser.add_argument("--output_path", required=True, help="Path where the final CSV should be saved")
    args = parser.parse_args()

    # 1. Data Ingestion
    def read_csv(table_name):
        return (spark.read.option("header", "true")
            .option("delimiter", ",")
            .option("encoding", "UTF-8")
            .csv(f"{args.input_path}/{table_name}.csv"))

    ventas_pos = read_csv("ventas_pos")
    pedidos_web = read_csv("pedidos_web")
    mapeo_sku = read_csv("mapeo_sku")
    productos = read_csv("productos")
    costos_historico = read_csv("costos_historico")
    tipo_cambio = read_csv("tipo_cambio")

    # 2. POS Sales processing
    # Logic: boletas and facturas sum, credit notes subtract. Exclude 'SERVICIO'.
    lineas_pos = (ventas_pos.alias("v")
        .join(mapeo_sku.alias("m"), F.upper(mapeo_sku["sku_pos"]) == F.trim(F.upper(ventas_pos["sku"])), "left") # upper & trim added W
        .join(productos.alias("p"), F.col("p.sku_erp") == F.col("m.sku_erp"), "left")
        .filter(
            (F.col("v.tipo_comprobante").isin("01", "03", "07")) &
            (F.col("v.estado") != "ANULADO") &
            (F.col("p.categoria") != "servicio") # lowercased
        )
        .select(
            F.col("v.id_tienda"),
            F.trunc(F.col("v.fecha_emision").cast("date"), "month").alias("mes"),
            F.col("m.sku_erp"),
            F.col("p.categoria"),
            F.when(F.col("v.tipo_comprobante") != "07",
                   F.concat(F.col("v.tipo_comprobante"), F.lit("-"), F.col("v.serie"), F.lit("-"), F.col("v.numero")))
             .alias("ticket"),
            F.when(F.col("v.tipo_comprobante") == "07", -F.col("v.cantidad").cast("int"))
             .otherwise(F.col("v.cantidad").cast("int")).alias("unidades"),
            (F.when(F.col("v.tipo_comprobante") == "07", -F.col("v.importe_total").cast("double"))
             .otherwise(F.col("v.importe_total").cast("double")) / 1.18).cast(DecimalType(12, 2)).alias("venta_sin_igv")
        ))

    # 3. Web Sales processing
    # Logic: 'WEB' store. USD converted to PEN. Exclude 'SERVICIO'.
    lineas_web = (pedidos_web.alias("w")
        .join(mapeo_sku.alias("m"), F.col("m.sku_web") == F.col("w.sku_web"), "left")
        .join(productos.alias("p"), F.col("p.sku_erp") == F.col("m.sku_erp"), "left")
        .join(tipo_cambio.alias("tc"), F.col("tc.fecha") == F.col("w.fecha_pedido").cast("date"), "left")
        .filter(
            (F.col("w.estado") == "PAGADO") &
            (F.col("p.categoria") != "servicio") # lowercased
        )
        .select(
            F.lit("WEB").alias("id_tienda"),
            F.trunc(F.col("w.fecha_pedido").cast("date"), "month").alias("mes"),
            F.col("m.sku_erp"),
            F.col("p.categoria"),
            F.col("w.id_pedido").alias("ticket"),
            F.col("w.cantidad").cast("int").alias("unidades"),
            (F.when(F.col("w.moneda") == "USD", F.col("w.importe").cast("double") * F.col("tc.tc").cast("double"))
             .otherwise(F.col("w.importe").cast("double")) / 1.18).cast(DecimalType(12, 2)).alias("venta_sin_igv")
        ))

    # Union of POS and Web
    lineas = lineas_pos.unionByName(lineas_web)

    # 4. Cost Integration
    # Logic: Join with current cost (vigente_hasta IS NULL)
    con_costo = (lineas.alias("l")
        .join(costos_historico.alias("c"),
              (F.col("c.sku_erp") == F.col("l.sku_erp")) & (F.col("c.vigente_hasta").isNull()),
              "left")
        .withColumn("costo", (F.col("l.unidades") * F.col("c.costo_unitario").cast("double")).cast(DecimalType(14, 4))))

    # 5. Final Aggregation
    agg = con_costo.groupBy("mes", "id_tienda", "categoria").agg(
        F.sum("unidades").alias("unidades"),
        F.countDistinct("ticket").alias("tickets"),
        F.sum("venta_sin_igv").alias("venta_neta"),
        F.sum("costo").cast(DecimalType(14, 2)).alias("costo")
    )

    # Final report calculations
    reporte = agg.select(
        "mes",
        "id_tienda",
        F.lower(F.col("categoria")).alias("categoria"),
        "unidades",
        "tickets",
        (F.col("unidades") / F.when(F.col("tickets") == 0, None).otherwise(F.col("tickets"))).cast(IntegerType()).alias("unidades_por_ticket"),
        "venta_neta",
        "costo",
        (F.col("venta_neta") - F.col("costo")).alias("margen"),
        (F.lit(100.0) * (F.col("venta_neta") - F.col("costo")) / F.when(F.col("venta_neta") == 0, None).otherwise(F.col("venta_neta")))
            .cast(DecimalType(9, 2)).alias("margen_pct")
    )

    # Save output as CSV
    # Use coalesce(1) to get a single CSV file
    reporte.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output_path)

    spark.stop()


if __name__ == "__main__":
    run_migration()
