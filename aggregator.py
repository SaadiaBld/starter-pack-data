import sqlite3
import pandas as pd
import os

# --- Configuration ---
DB_FILE = "data.db"
OUTPUT_DIR = "daily_summary"

def generate_all_daily_summaries():
    """
    Génère un fichier CSV de résumé quotidien pour chaque date disponible dans la base de données.
    Les fichiers sont stockés dans le répertoire /daily_summary.
    """
    try:
        # --- Étape 1: Créer le répertoire de sortie ---
        print(f"Vérification/Création du répertoire de sortie: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # --- Étape 2: Charger toutes les données nécessaires ---
        print("Chargement des données depuis la base de données...")
        conn = sqlite3.connect(DB_FILE)
        orders_df = pd.read_sql_query("SELECT * FROM orders_clean", conn)
        
        if orders_df.empty:
            print("La table 'orders_clean' est vide. Aucun résumé ne peut être généré.")
            return

        print(f"{len(orders_df)} commandes chargées.")

        # --- Étape 3: Préparer les données et trouver les dates uniques ---
        orders_df['date'] = pd.to_datetime(orders_df['created_at']).dt.date.astype(str)
        unique_dates = orders_df['date'].unique()
        print(f"{len(unique_dates)} dates uniques trouvées. Début de la boucle de génération...")

        # --- Étape 4: Boucler sur chaque date et générer un CSV ---
        for target_date in unique_dates:
            print(f"  -> Traitement de la date: {target_date}")
            
            # Filtrer les données pour le jour en cours
            daily_df = orders_df[orders_df['date'] == target_date].copy()
            
            # Définir les opérations d'agrégation
            agg_operations = {
                'orders_count': ('order_id', 'count'),
                'unique_customers': ('customer_id', 'nunique'),
                'items_sold': ('items_sold', 'sum')
            }
            
            # Agréger les données pour la journée
            summary_df = daily_df.groupby(['date', 'city', 'channel']).agg(**agg_operations).reset_index()
            
            # S'assurer que les colonnes sont dans le bon ordre
            final_df = summary_df[['date', 'city', 'channel', 'orders_count', 'unique_customers', 'items_sold']]

            # Générer le nom du fichier et le chemin de sortie
            file_date_str = target_date.replace('-', '')
            output_filename = f"daily_summary_{file_date_str}.csv"
            output_path = os.path.join(OUTPUT_DIR, output_filename)

            # Sauvegarder le fichier CSV
            final_df.to_csv(
                output_path,
                sep=';',
                encoding='utf-8',
                index=False
            )
            print(f"     - Fichier '{output_path}' généré avec {len(final_df)} lignes.")

        print("\nProcessus terminé. Tous les résumés quotidiens ont été générés.")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    generate_all_daily_summaries()