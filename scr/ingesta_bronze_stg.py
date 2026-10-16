import os
import sys
import argparse
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
builder = (
    SparkSession.builder.appName("cafe-altura-bronze").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

def merge_upsert(df, target_path, pk_cols, update_col="fecha_actualizacion"):
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

def run_ingestion(input_path, delta_lake_path):
    # Table configuration: (table_name, is_upsert, pk_columns)
    tables_config = [
        ("stg.dim_tiendas", False, []),
        ("stg.mapeo_sku", False, []),
        ("stg.productos", False, []),
        ("stg.costos_historico", False, []),
        ("stg.tipo_cambio", False, []),
        ("stg.ventas_pos", True, ["tipo_comprobante", "serie", "numero", "item"]),
        ("stg.pedidos_web", True, ["id_pedido", "linea"]),
    ]

    for table_name, is_upsert, pks in tables_config:
        # The CSVs in the input path are named without the 'stg.' prefix
        csv_name = table_name.replace("stg.", "")
        full_csv_path = f"{input_path}/{csv_name}.csv"

        print(f"Ingesting {csv_name} into {table_name}...")
        df = (spark.read.option("header", "true")
              .option("delimiter", ",")
              .option("encoding", "UTF-8")
              .csv(full_csv_path))

        target_path = f"{delta_lake_path}/bronze/{table_name}"

        if is_upsert:
            merge_upsert(df, target_path, pks)
        else:
            # Snapshot: overwrite the table
            df.write.format("delta").mode("overwrite").save(target_path)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer: Ingest CSVs to Delta")
    parser.add_argument("--input_path", required=True, help="Path to CSV directory")
    parser.add_argument("--delta_lake_path", required=True, help="Root path for Delta Lake")
    args = parser.parse_args()

    run_ingestion(args.input_path, args.delta_lake_path)
