from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.schemas import (
    SCHEMA_STG_TIENDAS,
    SCHEMA_STG_MAPEO_SKU,
    SCHEMA_STG_PRODUCTOS,
    SCHEMA_STG_COSTOS_HISTORICO,
    SCHEMA_STG_TIPO_CAMBIO,
    SCHEMA_STG_VENTAS_POS,
    SCHEMA_STG_PEDIDOS_WEB,
)

def merge_upsert(spark, df, target_path, pk_cols, update_col="fecha_actualizacion"):
    """
    Performs a MERGE into a Delta table.
    Ensures source data is deduplicated first to prevent 'Multiple rows in source matched' errors.
    """
    # 1. Universal Deduplication: Only keep the latest record per PK from the source data
    window_spec = Window.partitionBy(*pk_cols).orderBy(F.col(update_col).desc())
    df_deduped = (df.withColumn("_rank", F.row_number().over(window_spec))
                  .filter(F.col("_rank") == 1)
                  .drop("_rank"))

    # 2. Initial Load: If the table doesn't exist, save the deduplicated data
    if not DeltaTable.isDeltaTable(spark, target_path):
        df_deduped.write.format("delta").mode("overwrite").save(target_path)
        return

    # 3. Incremental Load: Use MERGE on the deduplicated source
    target_table = DeltaTable.forPath(spark, target_path)
    join_condition = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])

    target_table.alias("target").merge(
        df_deduped.alias("source"),
        join_condition
    ).whenMatchedUpdateAll(
        condition=f"source.{update_col} > target.{update_col}"
    ).whenNotMatchedInsertAll().execute()

def run_ingestion(spark, input_path, delta_lake_path):
    # Table configuration: (table_name, is_upsert, pk_columns, schema)
    tables_config = [
        ("stg.tiendas", False, [], SCHEMA_STG_TIENDAS),
        ("stg.mapeo_sku", False, [], SCHEMA_STG_MAPEO_SKU),
        ("stg.productos", False, [], SCHEMA_STG_PRODUCTOS),
        ("stg.costos_historico", False, [], SCHEMA_STG_COSTOS_HISTORICO),
        ("stg.tipo_cambio", False, [], SCHEMA_STG_TIPO_CAMBIO),
        ("stg.ventas_pos", True, ["tipo_comprobante", "serie", "numero", "item"], SCHEMA_STG_VENTAS_POS),
        ("stg.pedidos_web", True, ["id_pedido", "linea"], SCHEMA_STG_PEDIDOS_WEB),
    ]

    for table_name, is_upsert, pks, schema in tables_config:
        # The CSVs in the input path are named without the 'stg.' prefix
        csv_name = table_name.replace("stg.", "")
        full_csv_path = f"{input_path}/{csv_name}.csv"

        print(f"Ingesting {csv_name} into {table_name}...")

        df = (spark.read.option("header", "true")
              .option("delimiter", ",")
              .option("encoding", "UTF-8")
              .schema(schema)
              .csv(full_csv_path))

        target_path = f"{delta_lake_path}/bronze/{table_name}"

        if is_upsert:
            merge_upsert(spark, df, target_path, pks)
        else:
            # Snapshot: overwrite the table
            df.write.format("delta").mode("overwrite").save(target_path)
