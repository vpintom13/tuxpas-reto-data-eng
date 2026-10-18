import src.config as config
from src.ingesta_bronze_stg import run_ingestion
from src.transform_silver_ventas import run_transform
from src.reporte_gold_margen import run_report

def run_pipeline(spark):
    """
    Data pipeline orchestrator for the Monthly Margin Report.
    Executes the flow: Bronze -> Silver -> Gold using the provided SparkSession.
    """
    # Read configurations first
    input_path = config.get_config("INPUT_PATH")
    delta_path = config.get_config("DELTA_PATH")
    output_csv_path = config.get_config("OUTPUT_CSV_PATH")

    # 1. Bronze Layer: Ingest CSVs to Delta Lake
    run_ingestion(spark, input_path, delta_path)

    # 2. Silver Layer: Sales Transformation and Unification
    run_transform(spark, delta_path)

    # 3. Gold Layer: Final Report Generation
    run_report(spark, delta_path, output_csv_path)
