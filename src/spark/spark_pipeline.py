# spark_pipeline.py : fichier unique qui load et traite les données (cleaning, aggregation, deduplicate)

import os
import glob
import sys # Ajout de cette importation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Chemin vers le répertoire racine du projet (remonte de src/spark)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# --- Configuration ---
# Le chemin des données sera lu depuis les arguments en ligne de commande
# Si aucun argument n'est fourni, on utilise le chemin par défaut
if len(sys.argv) > 1:
    DATA_PATH = sys.argv[1]
else:
    DATA_PATH = os.path.join(PROJECT_ROOT, "data-68ed", "data", "input")

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "daily_summary")


def load_and_clean_data(spark):
    """
    Charge, nettoie et transforme les données brutes (clients, commandes, remboursements)
    en DataFrames Spark prêts pour l'agrégation.
    """
    print("=== Étape 1: Chargement et nettoyage des données ===")

    # --- Clients ---
    print("  - Chargement des clients...")
    customers_path = os.path.join(DATA_PATH, "customers.csv")
    customers_df = spark.read.csv(customers_path, header=True, inferSchema=True)
    active_customers_base = customers_df.filter(F.col("is_active") == True)                                   .select("customer_id", "city")
    
    # --- Règle: S'assurer que la table des clients est unique par customer_id ---
    active_customers = active_customers_base.dropDuplicates(['customer_id'])

    print(f"    {active_customers.count()} clients actifs uniques trouvés.")


    # --- Commandes ---
    print("  - Chargement et nettoyage des commandes...")
    order_files = glob.glob(os.path.join(DATA_PATH, "orders_*.json"))
    if not order_files:
        print("Aucun fichier de commande JSON trouvé.")
        return None, None

    raw_orders = spark.read.option("multiLine", True).json(order_files)

    # --- Règle: Dédupliquer sur order_id en gardant la première occurrence ---
    # C'est l'équivalent du `keep='first'` de Pandas, appliqué avant toute agrégation.
    window_spec = Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())
    raw_orders_dedup = raw_orders.withColumn("row_num", F.row_number().over(window_spec)) \
                                 .filter(F.col("row_num") == 1) \
                                 .drop("row_num")

    # Appliquer les autres règles métier sur les commandes maintenant dédupliquées
    paid_orders = raw_orders_dedup.filter(F.col("payment_status") == "paid")
    exploded_items = paid_orders.withColumn("item", F.explode("items"))
    valid_items = exploded_items.filter(F.col("item.unit_price") > 0)

    # Agréger les articles par commande pour obtenir les totaux
    orders_agg = valid_items.groupBy("order_id", "customer_id", "channel", "created_at") \
        .agg(
            F.sum("item.qty").alias("items_sold"),
            F.sum(F.col("item.qty") * F.col("item.unit_price")).alias("total_amount")
        )

    # Joindre avec les clients actifs. La déduplication a déjà été faite.
    orders_clean = orders_agg.join(active_customers, on="customer_id", how="inner")
    print(f"    {orders_clean.count()} commandes valides et nettoyées.")

    # --- Remboursements ---
    print("  - Chargement des remboursements...")
    refunds_path = os.path.join(DATA_PATH, "refunds.csv")
    refunds_clean = spark.read.csv(refunds_path, header=True, inferSchema=True)
    print(f"    {refunds_clean.count()} remboursements chargés.")

    return orders_clean, refunds_clean


def aggregate_and_export(orders_df, refunds_df):
    """
    Prend les DataFrames nettoyés, effectue les agrégations de ventes et de
    remboursements, et exporte les résumés quotidiens en CSV.
    """
    print("\n=== Étape 2: Agrégation des données et exportation ===")

    # --- Préparation ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    orders = orders_df.withColumn("date", F.to_date("created_at"))
    refunds = refunds_df.withColumn("date", F.to_date("created_at"))

    # --- Agrégation des ventes ---
    print("  - Agrégation des ventes...")
    sales_agg = orders.groupBy("date", "city", "channel").agg(
        F.count("order_id").alias("orders_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("items_sold").alias("items_sold"),
        F.sum("total_amount").alias("gross_revenue_eur")
    )

    # --- Agrégation des remboursements ---
    print("  - Agrégation des remboursements...")
    refunds_with_details = refunds.join(
        orders.select("order_id", "city", "channel").dropDuplicates(),
        on="order_id",
        how="left"
    )
    refunds_agg = refunds_with_details.groupBy("date", "city", "channel").agg(
        F.sum("amount").alias("refunds_eur")
    )

    # --- Combinaison et calculs finaux ---
    print("  - Combinaison des ventes et remboursements...")
    final_summary = sales_agg.join(refunds_agg, on=["date", "city", "channel"], how="left")
    final_summary = final_summary.fillna({"refunds_eur": 0})
    final_summary = final_summary.withColumn(
        "net_revenue_eur",
        F.col("gross_revenue_eur") + F.col("refunds_eur")
    )

    # Arrondi
    final_summary = final_summary.withColumn("gross_revenue_eur", F.round("gross_revenue_eur", 2)) \
                                 .withColumn("refunds_eur", F.round("refunds_eur", 2)) \
                                 .withColumn("net_revenue_eur", F.round("net_revenue_eur", 2))

    # Ordre des colonnes
    final_summary = final_summary.select(
        'date', 'city', 'channel', 'orders_count', 'unique_customers',
        'items_sold', 'gross_revenue_eur', 'refunds_eur', 'net_revenue_eur'
    )

    # Trier la sortie pour correspondre à Pandas
    final_summary = final_summary.orderBy('date', 'city', 'channel')

    # --- Exportation des fichiers CSV ---
    print("  - Début de la génération des fichiers CSV...")
    all_dates = [row['date'] for row in final_summary.select("date").distinct().collect()]

    for target_date in all_dates:
        daily_data = final_summary.filter(F.col("date") == target_date)
        file_date_str = target_date.strftime("%Y%m%d")
        output_path = f"{OUTPUT_DIR}/daily_summary_{file_date_str}"

        daily_data.coalesce(1).write.mode("overwrite").csv(
            output_path,
            header=True,
            sep=";"
        )
        print(f"    -> Fichier généré pour le {target_date} dans le dossier '{output_path}'")

    print("\n✔ Pipeline complet terminé avec succès.")


def main():
    """
    Point d'entrée principal du pipeline Spark.
    """
    spark = (
        SparkSession.builder
        .appName("FreshKartPipeline")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    orders, refunds = load_and_clean_data(spark)

    if orders and refunds:
        aggregate_and_export(orders, refunds)
    else:
        print("****Pipeline arrêté car les données initiales n'ont pas pu être chargées.")

    spark.stop()


if __name__ == "__main__":
    main()