from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Chemin vers le répertoire racine du projet (remonte de src/spark)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

spark = SparkSession.builder.appName("AggregatorSpark").getOrCreate()

INPUT_DB = os.path.join(PROJECT_ROOT, "warehouse")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "daily_summary")

def generate_all_daily_summaries():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    orders = spark.read.parquet(f"{INPUT_DB}/orders_clean")
    refunds = spark.read.parquet(f"{INPUT_DB}/refunds")

    # Convertir created_at → date
    orders = orders.withColumn("date", F.to_date("created_at"))
    refunds = refunds.withColumn("date", F.to_date("created_at"))

    # Agrégation ventes
    sales_agg = orders.groupBy("date", "city", "channel").agg(
        F.count("order_id").alias("orders_count"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("items_sold").alias("items_sold"),
        F.sum("total_amount").alias("gross_revenue_eur")
    )

    # Agrégation remboursements
    refunds_with_details = refunds.join(
        orders.select("order_id", "city", "channel").dropDuplicates(),
        on="order_id",
        how="left"
    )

    refunds_agg = refunds_with_details.groupBy("date", "city", "channel").agg(
        F.sum("amount").alias("refunds_eur")
    )

    # Combinaison ventes + remboursements
    final = sales_agg.join(refunds_agg,
                           on=["date", "city", "channel"],
                           how="left")

    final = final.fillna({"refunds_eur": 0})

    # Revenu net
    final = final.withColumn(
        "net_revenue_eur",
        F.col("gross_revenue_eur") + F.col("refunds_eur")
    )

    # Arrondir les colonnes financières à 2 décimales
    final = final.withColumn("gross_revenue_eur", F.round("gross_revenue_eur", 2)) \
                 .withColumn("refunds_eur", F.round("refunds_eur", 2)) \
                 .withColumn("net_revenue_eur", F.round("net_revenue_eur", 2))

    # Mettre les colonnes dans l'ordre final
    final = final.select(
        'date', 'city', 'channel', 'orders_count', 'unique_customers',
        'items_sold', 'gross_revenue_eur', 'refunds_eur', 'net_revenue_eur'
    )

    # Trier la sortie pour correspondre à Pandas
    final = final.orderBy('date', 'city', 'channel')

    # Export CSV par date
    print("Début de la génération des fichiers CSV...")
    all_dates = [row['date'] for row in final.select("date").distinct().collect()]

    for target_date in all_dates:
        daily_data = final.filter(F.col("date") == target_date)

        # Tri identique à Pandas : date, city, channel
        daily_data = daily_data.orderBy(["date", "city", "channel"])
        
        file_date_str = target_date.strftime("%Y%m%d")
        output_path = f"{OUTPUT_DIR}/daily_summary_{file_date_str}"

        # .coalesce(1) force la sortie en un seul fichier dans le dossier de destination
        daily_data.coalesce(1).write.mode("overwrite").csv(
            output_path,
            header=True,
            sep=";"
        )
        print(f"  - Fichier généré pour le {target_date} dans le dossier '{output_path}'")

    print("\n✔ Résumés quotidiens générés.")

if __name__ == "__main__":
    generate_all_daily_summaries()
