from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import os
import glob

# Chemin vers le répertoire racine du projet (remonte de src/spark)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DATA_PATH = os.path.join(PROJECT_ROOT, "data-68ed", "data", "input")
OUTPUT_DB = os.path.join(PROJECT_ROOT, "warehouse")   # dossier où stocker parquet / tables parquet

spark = (
    SparkSession.builder
    .appName("LoadDataSpark")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

def clean_and_load_orders():
    print("=== Nettoyage et chargement des commandes (Spark) ===")

    # Chargement clients
    customers_path = os.path.join(DATA_PATH, "customers.csv")
    customers_df = spark.read.csv(customers_path, header=True, inferSchema=True)

    # Conserver uniquement les clients actifs
    active_customers = (
        customers_df
        .filter(F.col("is_active") == True)
        .select("customer_id", "city")
        .dropDuplicates(["customer_id"])
    )


    # Lecture JSON (multi-fichiers)
    order_files = glob.glob(os.path.join(DATA_PATH, "orders_*.json"))
    if not order_files:
        print("Aucun fichier JSON trouvé.")
        return

    raw_orders = spark.read.option("multiLine", True).json(order_files)

    # --- Règle: Conserver uniquement les commandes payées ---
    raw_orders = raw_orders.filter(F.col("payment_status") == "paid")

    # Exploser la colonne "items"
    exploded = raw_orders.withColumn("item", F.explode("items"))

    # Filtrer les items valides
    valid_items = exploded.filter(F.col("item.unit_price") > 0)

    # Agréger par commande
    agg_orders = valid_items.groupBy("order_id", "customer_id", "channel", "created_at") \
        .agg(
            F.sum("item.qty").alias("items_sold"),
            F.sum(F.col("item.qty") * F.col("item.unit_price")).alias("total_amount")
        )

    # Jointure avec clients actifs
    merged = agg_orders.join(active_customers, on="customer_id", how="inner")

    # --- Arrondi / cast ---
    merged = merged.withColumn("items_sold", F.col("items_sold").cast("int")) \
                   .withColumn("total_amount", F.round("total_amount", 2))
    # Dédupliquer
    merged = merged.dropDuplicates(["order_id"])

    # Sauvegarde parquet
    merged.write.mode("overwrite").parquet(f"{OUTPUT_DB}/orders_clean")

    print("✔ Commandes nettoyées et enregistrées en parquet.")

def load_refunds():
    print("=== Chargement des remboursements ===")

    refunds_path = os.path.join(DATA_PATH, "refunds.csv")
    refunds_df = spark.read.csv(refunds_path, header=True, inferSchema=True)

    refunds_df.write.mode("overwrite").parquet(f"{OUTPUT_DB}/refunds")
    print("✔ Remboursements enregistrés en parquet.")

if __name__ == "__main__":
    clean_and_load_orders()
    load_refunds()
