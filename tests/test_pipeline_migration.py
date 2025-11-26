import pytest
import os
import shutil
import subprocess
import pandas as pd
import glob
from pyspark.sql import SparkSession

PROJECT_ROOT = os.getcwd()
ORIGINAL_DATA_INPUT_PATH = os.path.join(PROJECT_ROOT, "data-68ed", "data", "input")

@pytest.fixture(scope="module")
def spark_session():
    spark = (
        SparkSession.builder
        .appName("TestPipelineMigration")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def setup_test_environment(tmp_path):
    # Créer répertoire d'entrée temporaire
    temp_data_input_path = os.path.join(tmp_path, "data-68ed", "data", "input")
    os.makedirs(temp_data_input_path, exist_ok=True)
    for fname in ["customers.csv", "refunds.csv",
                  "orders_2025-03-01.json", "orders_2025-03-21.json"]:
        shutil.copy(os.path.join(ORIGINAL_DATA_INPUT_PATH, fname), temp_data_input_path)

    # Répertoires de sortie temporaires
    pandas_output = os.path.join(tmp_path, "daily_summary_pandas")
    spark_output = os.path.join(tmp_path, "daily_summary_spark")
    os.makedirs(pandas_output, exist_ok=True)
    os.makedirs(spark_output, exist_ok=True)

    return {
        "input_path": temp_data_input_path,
        "pandas_output": pandas_output,
        "spark_output": spark_output,
        "tmp_path": tmp_path,
        "project_root": PROJECT_ROOT
    }

def test_pipeline_migration_output(setup_test_environment, spark_session):
    env = setup_test_environment
    input_path = env["input_path"]
    pandas_output = env["pandas_output"]
    spark_output = env["spark_output"]
    project_root = env["project_root"]
    tmp_path = env["tmp_path"]

    # --- Pipeline Pandas ---
    subprocess.run([
        "python3", os.path.join(project_root, "src", "pandas", "tables.py"),
        "--input-dir", input_path,
        "--output-dir", pandas_output
    ], cwd=tmp_path, check=True)

    subprocess.run([
        "python3", os.path.join(project_root, "src", "pandas", "load_data.py"),
        "--input-dir", input_path,
    ], cwd=tmp_path, check=True)

    subprocess.run([
        "python3", os.path.join(project_root, "src", "pandas", "aggregator.py"),
        "--output-dir", pandas_output
    ], cwd=tmp_path, check=True)

    # --- Pipeline Spark ---
    subprocess.run([
        "spark-submit",
        os.path.join(project_root, "src", "spark", "spark_pipeline.py"),
        "--input-dir", input_path,
        "--output-dir", spark_output
    ], cwd=tmp_path, check=True)

    # --- Comparaison ---
    pandas_csv = sorted([f for f in os.listdir(pandas_output) if f.endswith(".csv")])
    spark_dirs = sorted([
        d for d in os.listdir(spark_output)
        if os.path.isdir(os.path.join(spark_output, d))
        and d.startswith("daily_summary_")
    ])

    assert len(pandas_csv) == len(spark_dirs), "Nombre de sorties par date ne correspond pas."

    for csv_file in pandas_csv:
        date = csv_file.replace("daily_summary_", "").replace(".csv", "")
        pandas_df = pd.read_csv(os.path.join(pandas_output, csv_file), sep=";")
        spark_subdir = os.path.join(spark_output, f"daily_summary_{date}")
        assert os.path.isdir(spark_subdir), f"Dossier Spark manquant pour {date}"
        spark_part = glob.glob(os.path.join(spark_subdir, "part-*.csv"))
        assert len(spark_part) == 1, f"Problème de sortie Spark pour {date}"
        spark_df = pd.read_csv(spark_part[0], sep=";")
        spark_df = spark_df[pandas_df.columns]  # même ordre de colonnes
        pd.testing.assert_frame_equal(pandas_df, spark_df, check_dtype=True, check_exact=False, atol=1e-2)
