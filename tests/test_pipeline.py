import os
import sys
import pytest
from pyspark.testing import assertDataFrameEqual
from src.pipeline import run_pipeline
from tests.conftest import load_csv_as_df

def setup_pipeline_env(monkeypatch, input_path, delta_path, output_path):
    """Helper to override configuration environment variables."""
    monkeypatch.setenv("INPUT_PATH", input_path)
    monkeypatch.setenv("DELTA_LAKE_PATH", delta_path)
    monkeypatch.setenv("OUTPUT_CSV_PATH", output_path)

def test_pipeline_lote1_parity(spark, tmp_path, monkeypatch):
    """
    Test 1: Validate that running the pipeline with Lote 1 matches the legacy report.
    """
    # Setup paths
    input_path = "datos/lote_1"
    delta_path = str(tmp_path / "delta_lote1")
    output_path = str(tmp_path / "output_lote1")
    legacy_path = "legado/salida/rpt_margen_mensual_lote1.csv"

    # Override configuration
    setup_pipeline_env(monkeypatch, input_path, delta_path, output_path)

    # Run pipeline
    run_pipeline(spark)

    # Validation
    actual_df = load_csv_as_df(spark, output_path)
    expected_df = load_csv_as_df(spark, legacy_path)

    assertDataFrameEqual(actual_df, expected_df)

def test_pipeline_incremental_parity(spark, tmp_path, monkeypatch):
    """
    Test 2: Validate incremental load (Lote 1 -> Lote 2) matches the legacy report.
    """
    # Setup paths
    lote1_path = "/home/victor/repos/reto_cafe_altura/datos/lote_1"
    lote2_path = "/home/victor/repos/reto_cafe_altura/datos/lote_2"
    delta_path = str(tmp_path / "delta_incremental")
    output_path = str(tmp_path / "output_incremental")
    legacy_path_lote2 = "/home/victor/repos/reto_cafe_altura/legado/salida/rpt_margen_mensual_lote2.csv"

    # Step A: Ingest Lote 1
    setup_pipeline_env(monkeypatch, lote1_path, delta_path, output_path)
    run_pipeline(spark)

    # Step B: Ingest Lote 2 incrementally
    setup_pipeline_env(monkeypatch, lote2_path, delta_path, output_path)
    run_pipeline(spark)

    # Validation
    actual_df = load_csv_as_df(spark, output_path)
    expected_df = load_csv_as_df(spark, legacy_path_lote2)

    assertDataFrameEqual(actual_df, expected_df)

def test_pipeline_idempotency(spark, tmp_path, monkeypatch):
    """
    Test 3: Validate that running the same lot twice produces identical results.
    """
    # Setup paths
    input_path = "/home/victor/repos/reto_cafe_altura/datos/lote_1"
    delta_path = str(tmp_path / "delta_idempotency")
    output_path = str(tmp_path / "output_idempotency")
    legacy_path = "/home/victor/repos/reto_cafe_altura/legado/salida/rpt_margen_mensual_lote1.csv"

    # Override configuration
    setup_pipeline_env(monkeypatch, input_path, delta_path, output_path)

    # First run
    run_pipeline(spark)
    # Collect the data into memory to avoid SparkFileNotFoundException when files are overwritten
    df_first_run_data = load_csv_as_df(spark, output_path).collect()

    # Second run (identical config)
    run_pipeline(spark)
    # Collect again
    df_second_run_data = load_csv_as_df(spark, output_path).collect()

    # Convert back to DataFrames for assertDataFrameEqual or compare lists
    schema = load_csv_as_df(spark, legacy_path).schema
    df_first_run = spark.createDataFrame(df_first_run_data, schema=schema)
    df_second_run = spark.createDataFrame(df_second_run_data, schema=schema)

    # Validation: Result 1 == Result 2
    assertDataFrameEqual(df_first_run, df_second_run)

    # Validation: Result == Legacy
    expected_df = load_csv_as_df(spark, legacy_path)
    assertDataFrameEqual(df_second_run, expected_df)
