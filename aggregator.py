import sqlite3
import pandas as pd
import os

# --- Configuration ---
DB_FILE = "sales.db"
OUTPUT_DIR = "daily_summary"

def generate_all_daily_summaries():
    """
    Génère des résumés quotidiens complets (ventes et remboursements),
    les charge dans la base de données et crée les fichiers CSV finaux.
    """
    conn = None  # Initialiser conn en dehors du bloc try
    try:
        # --- Étape 1: Créer le répertoire de sortie ---
        print(f"Vérification/Création du répertoire de sortie: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # --- Étape 2: Charger toutes les données nécessaires ---
        print("Chargement des données depuis la base de données...")
        conn = sqlite3.connect(DB_FILE)
        orders_df = pd.read_sql_query("SELECT * FROM orders_clean", conn)
        refunds_df = pd.read_sql_query("SELECT * FROM refunds", conn)
        
        if orders_df.empty:
            print("La table 'orders_clean' est vide. Aucun résumé ne peut être généré.")
            return

        print(f"{len(orders_df)} commandes et {len(refunds_df)} remboursements chargés.")

        # --- Étape 3: Préparation des données ---
        orders_df['date'] = pd.to_datetime(orders_df['created_at']).dt.date.astype(str)
        refunds_df['date'] = pd.to_datetime(refunds_df['created_at']).dt.date.astype(str)

        # --- Étape 4: Agréger les ventes ---
        sales_agg = orders_df.groupby(['date', 'city', 'channel']).agg(
            orders_count=('order_id', 'count'),
            unique_customers=('customer_id', 'nunique'),
            items_sold=('items_sold', 'sum'),
            gross_revenue_eur=('total_amount', 'sum')
        ).reset_index()
        print("Agrégation des ventes terminée.")

        # --- Étape 5: Agréger les remboursements ---
        # Joindre avec les commandes pour obtenir 'city' et 'channel'
        refunds_with_details = pd.merge(
            refunds_df,
            orders_df[['order_id', 'city', 'channel']],
            on='order_id',
            how='left'
        )
        
        refunds_agg = refunds_with_details.groupby(['date', 'city', 'channel']).agg(
            refunds_eur=('amount', 'sum')
        ).reset_index()
        # Les montants sont déjà négatifs, la somme est donc correcte.
        print("Agrégation des remboursements terminée.")

        # --- Étape 6: Combiner ventes et remboursements ---
        final_summary = pd.merge(
            sales_agg,
            refunds_agg,
            on=['date', 'city', 'channel'],
            how='left'
        )
        # Remplacer les NaN par 0 pour les jours/villes/canaux sans remboursement
        final_summary['refunds_eur'] = final_summary['refunds_eur'].fillna(0)
        print("Combinaison des ventes et des remboursements effectuée.")

        # --- Étape 7: Calculer le revenu net ---
        # Comme refunds_eur est négatif, on l'additionne
        final_summary['net_revenue_eur'] = final_summary['gross_revenue_eur'] + final_summary['refunds_eur']
        
        # Arrondir les colonnes financières à 2 décimales
        final_summary['gross_revenue_eur'] = final_summary['gross_revenue_eur'].round(2)
        final_summary['refunds_eur'] = final_summary['refunds_eur'].round(2)
        final_summary['net_revenue_eur'] = final_summary['net_revenue_eur'].round(2)
        print("Calcul du revenu net terminé.")

        # --- Étape 8: Mettre les colonnes dans l'ordre final ---
        final_summary = final_summary[[
            'date', 'city', 'channel', 'orders_count', 'unique_customers', 
            'items_sold', 'gross_revenue_eur', 'refunds_eur', 'net_revenue_eur'
        ]]

        # --- Étape 9: Charger le résumé dans la table daily_city_sales ---
        final_summary.to_sql('daily_city_sales', conn, if_exists='replace', index=False)
        print(f"{len(final_summary)} lignes insérées dans la table 'daily_city_sales'.")

        # --- Étape 10: Générer les fichiers CSV quotidiens ---
        unique_dates = final_summary['date'].unique()
        print(f"Début de la génération des {len(unique_dates)} fichiers CSV...")
        
        for target_date in unique_dates:
            daily_data = final_summary[final_summary['date'] == target_date]
            file_date_str = target_date.replace('-', '')
            output_filename = f"daily_summary_{file_date_str}.csv"
            output_path = os.path.join(OUTPUT_DIR, output_filename)

            daily_data.to_csv(output_path, sep=';', encoding='utf-8', index=False)
            print(f"  - Fichier '{output_path}' généré.")

        print("\nProcessus d'agrégation complet terminé avec succès.")

    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    finally:
        if conn:
            conn.close()
            print("Connexion à la base de données fermée.")

if __name__ == "__main__":
    generate_all_daily_summaries()