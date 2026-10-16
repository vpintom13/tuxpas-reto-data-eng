import subprocess
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

@pytest.fixture(scope="session")
def spark():
    """Provides a SparkSession for the test suite."""
    spark_session = SparkSession.builder \
        .appName("integration-test-parity") \
        .master("local[*]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_report_parity(spark, tmp_path):
    """
    Integration test:
    1. Executes the PySpark script for Lote 1.
    2. Compares the resulting CSV with the legacy reference CSV.
    """
    # Paths
    project_root = "/home/victor/repos/reto_cafe_altura"
    script_path = os.path.join(project_root, "salida/script.py")
    input_path = os.path.join(project_root, "datos/lote_1")
    expected_csv_path = os.path.join(project_root, "cafe_altura/legado/salida/rpt_margen_mensual_lote1.csv")
    output_temp_folder = str(tmp_path / "output")

    # 1. Execute the PySpark script
    # We use subprocess to run the script as a separate process, mirroring production execution
    cmd = [
        "python3",
        script_path,
        "--input_path", input_path,
        "--output_path", output_temp_folder
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Check if script executed successfully
    assert result.returncode == 0, f"Script failed with return code {result.returncode}. Error: {result.stderr}"

    # 2. Load the resulting CSV
    # PySpark writes to a folder; we need to find the CSV file inside it
    import glob
    actual_csv_files = glob.glob(os.path.join(output_temp_folder, "*.csv"))
    if not actual_csv_files:
        pytest.fail(f"No CSV output found in {output_temp_folder}")

    actual_csv_path = actual_csv_files[0]

    df_actual = spark.read.option("header", "true").option("inferSchema", "true").csv(actual_csv_path)

    # 3. Load the legacy reference CSV
    df_expected = spark.read.option("header", "true").option("inferSchema", "true").csv(expected_csv_path)

    # 4. Compare using pyspark.testing
    # This will raise an AssertionError with a detailed diff if they are not equal
    assertDataFrameEqual(df_actual, df_expected)
