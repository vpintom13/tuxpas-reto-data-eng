from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def process_pos_sales(ventas_pos, mapeo_sku, productos):
    """
    Processes POS sales: joins with mapping and products, filters and calculates net sales.
    """
    return (ventas_pos.alias("v")
        .join(mapeo_sku.alias("m"), F.upper(mapeo_sku["sku_pos"]) == F.trim(F.upper(ventas_pos["sku"])), "left")
        .join(productos.alias("p"), F.col("p.sku_erp") == F.col("m.sku_erp"), "left")
        .filter(
            (F.col("v.tipo_comprobante").isin("01", "03", "07")) &
            (F.col("v.estado") != "ANULADO") &
            (F.col("p.categoria") != "servicio")
        )
        .select(
            F.col("v.id_tienda"),
            F.trunc(F.col("v.fecha_emision").cast("date"), "month").alias("mes"),
            F.col("m.sku_erp"),
            F.col("p.categoria"),
            F.when(F.col("v.tipo_comprobante") != "07",
                   F.concat(F.col("v.tipo_comprobante"), F.lit("-"), F.col("v.serie"), F.lit("-"), F.col("v.numero")))
             .alias("ticket"),
            F.when(F.col("v.tipo_comprobante") == "07", -F.col("v.cantidad"))
             .otherwise(F.col("v.cantidad")).alias("unidades"),
            (F.when(F.col("v.tipo_comprobante") == "07", -F.col("v.importe_total"))
             .otherwise(F.col("v.importe_total")) / 1.18).cast(DecimalType(12, 2)).alias("venta_sin_igv")
        ))

def process_web_sales(pedidos_web, mapeo_sku, productos, tipo_cambio):
    """
    Processes Web sales: joins with mapping, products, and exchange rate, filters and calculates net sales.
    """
    return (pedidos_web.alias("w")
        .join(mapeo_sku.alias("m"), F.upper(mapeo_sku["sku_web"]) == F.upper(pedidos_web["sku_web"]), "left")
        .join(productos.alias("p"), F.col("p.sku_erp") == F.col("m.sku_erp"), "left")
        .join(tipo_cambio.alias("tc"), F.col("tc.fecha") == F.col("w.fecha_pedido").cast("date"), "left")
        .filter(
            (F.col("w.estado") == "PAGADO") &
            (F.col("p.categoria") != "servicio")
        )
        .select(
            F.lit("WEB").alias("id_tienda"),
            F.trunc(F.col("w.fecha_pedido").cast("date"), "month").alias("mes"),
            F.col("m.sku_erp"),
            F.col("p.categoria"),
            F.col("w.id_pedido").alias("ticket"),
            F.col("w.cantidad").alias("unidades"),
            (F.when(F.col("w.moneda") == "USD", F.col("w.importe") * F.col("tc.tc"))
             .otherwise(F.col("w.importe")) / 1.18).cast(DecimalType(12, 2)).alias("venta_sin_igv")
        ))

def integrate_costs(sales_df, costs_df):
    """
    Joins sales with current costs (where vigente_hasta IS NULL) and calculates total cost.
    """
    return (sales_df.alias("l")
        .join(costs_df.alias("c"),
              (F.col("c.sku_erp") == F.col("l.sku_erp")) & (F.col("c.vigente_hasta").isNull()),
              "left")
        .withColumn("costo", (F.col("l.unidades") * F.col("c.costo_unitario")).cast(DecimalType(14, 4)))
        .select("l.*", "costo"))

def unify_sales(pos_df, web_df):
    """
    Unifies POS and Web sales DataFrames.
    """
    return pos_df.unionByName(web_df)

def run_transform(spark, delta_lake_path):
    bronze_path = f"{delta_lake_path}/bronze"

    # Load Bronze tables
    ventas_pos = spark.read.format("delta").load(f"{bronze_path}/stg.ventas_pos")
    pedidos_web = spark.read.format("delta").load(f"{bronze_path}/stg.pedidos_web")
    mapeo_sku = spark.read.format("delta").load(f"{bronze_path}/stg.mapeo_sku")
    productos = spark.read.format("delta").load(f"{bronze_path}/stg.productos")
    tipo_cambio = spark.read.format("delta").load(f"{bronze_path}/stg.tipo_cambio")
    costos_historico = spark.read.format("delta").load(f"{bronze_path}/stg.costos_historico")

    # Process sales
    pos_sales_df = process_pos_sales(ventas_pos, mapeo_sku, productos)
    web_sales_df = process_web_sales(pedidos_web, mapeo_sku, productos, tipo_cambio)

    # Unify
    unified_sales = unify_sales(pos_sales_df, web_sales_df)
    silver_ventas = integrate_costs(unified_sales, costos_historico)

    # Write to Silver
    silver_path = f"{delta_lake_path}/silver/silver_ventas_unificadas"
    silver_ventas.write.format("delta").mode("overwrite").save(silver_path)
