

import sqlite3
import pandas as pd
import glob
import os
import json

# Constantes pour les chemins
DB_FILE = "sales.db"
DATA_PATH = os.path.join('data-68ed', 'data', 'input')

def clean_and_load_orders():
    """
    Nettoie les données en respectant les règles métier et les charge
    dans la table orders_clean de la base de données SQLite.
    """
    print("Début du processus de nettoyage et de chargement...")

    # --- Règle: Exclure les clients inactifs ---
    customers_path = os.path.join(DATA_PATH, 'customers.csv')
    if not os.path.exists(customers_path):
        print(f"Erreur : Le fichier client {customers_path} est introuvable.")
        return
    
    customers_df = pd.read_csv(customers_path)
    # CORRECTION: Filtrer pour ne garder que les clients actifs
    active_customers_df = customers_df[customers_df["is_active"] == True].copy()
    print(f"{len(active_customers_df)} clients actifs sur {len(customers_df)} chargés.")
    
    # Garder uniquement les colonnes nécessaires pour la jointure
    customers_lookup = active_customers_df[['customer_id', 'city']]

    # --- Lecture des commandes ---
    order_files = glob.glob(os.path.join(DATA_PATH, 'orders_*.json'))
    if not order_files:
        print("Erreur : Aucun fichier de commande trouvé.")
        return

    all_orders_data = []
    rejected_items_log = []

    for order_file in order_files:
        with open(order_file, 'r') as f:
            orders = json.load(f)
            for order in orders:
                # --- Règle: Conserver uniquement les commandes payées ---
                if order.get('payment_status') != 'paid':
                    continue

                # --- Règle: Écarter les articles à prix unitaire négatif ---
                valid_items = []
                for item in order['items']:
                    if item['unit_price'] > 0:
                        valid_items.append(item)
                    else:
                        # Consigner les rejets
                        rejected_items_log.append(f"Article rejeté (prix <= 0): Commande {order['order_id']}, SKU {item['sku']}, Prix {item['unit_price']}")
                
                # Si après le filtre, il reste des articles, on traite la commande
                if valid_items:
                    items_sold = sum(item['qty'] for item in valid_items)
                    total_amount = sum(item['qty'] * item['unit_price'] for item in valid_items)
                    
                    all_orders_data.append({
                        'order_id': order['order_id'],
                        'customer_id': order['customer_id'],
                        'channel': order['channel'],
                        'created_at': order['created_at'],
                        'payment_status': order['payment_status'],
                        'items_sold': items_sold,
                        'total_amount': total_amount
                    })

    if not all_orders_data:
        print("Aucune commande valide à traiter.")
        return

    # Afficher les articles rejetés
    if rejected_items_log:
        print("\n--- Journal des articles rejetés ---")
        for log_entry in rejected_items_log:
            print(log_entry)
        print("------------------------------------\n")

    orders_df = pd.DataFrame(all_orders_data)
    print(f"{len(orders_df)} commandes valides prêtes à être chargées.")

    # --- Règle: Dédupliquer sur order_id ---
    initial_rows = len(orders_df)
    orders_df.drop_duplicates(subset=['order_id'], keep='first', inplace=True)
    deduplicated_rows = len(orders_df)
    if initial_rows > deduplicated_rows:
        print(f"{initial_rows - deduplicated_rows} commandes en double ont été supprimées.")

    # --- Jointure avec les clients (maintenant actifs) ---
    merged_df = pd.merge(orders_df, customers_lookup, on='customer_id', how='inner')
    # how='inner' garantit que seules les commandes de clients actifs sont conservées
    print(f"{len(merged_df)} commandes restantes après jointure avec les clients actifs.")
    
    # S'assurer que les colonnes sont dans le bon ordre pour la BDD
    final_df = merged_df[[
        'order_id', 'customer_id', 'city', 'channel', 'items_sold', 
        'created_at', 'payment_status', 'total_amount'
    ]]

    # --- Chargement des données dans SQLite ---
    try:
        conn = sqlite3.connect(DB_FILE)
        final_df.to_sql('orders_clean', conn, if_exists='replace', index=False)
        conn.close()
        print(f"\n{len(final_df)} lignes insérées dans la table 'orders_clean'.")
        print("Processus terminé avec succès.")

    except Exception as e:
        print(f"Une erreur est survenue lors du chargement BDD : {e}")

def load_refunds():
    """
    Charge les données de remboursement depuis refunds.csv dans la base de données.
    """
    print("\nDébut du chargement des remboursements...")
    refunds_path = os.path.join(DATA_PATH, 'refunds.csv')
    
    if not os.path.exists(refunds_path):
        print(f"Erreur : Le fichier de remboursements {refunds_path} est introuvable.")
        return

    refunds_df = pd.read_csv(refunds_path)
    print(f"{len(refunds_df)} lignes de remboursement chargées depuis le CSV.")

    try:
        conn = sqlite3.connect(DB_FILE)
        # Charger les données de remboursement dans une nouvelle table
        refunds_df.to_sql('refunds', conn, if_exists='replace', index=False)
        conn.close()
        print(f"{len(refunds_df)} lignes insérées dans la table 'refunds'.")
        print("Chargement des remboursements terminé avec succès.")

    except Exception as e:
        print(f"Une erreur est survenue lors du chargement des remboursements : {e}")

if __name__ == "__main__":
    try:
        # Exécuter tables.py pour s'assurer que le schéma est à jour
        exec(open("tables.py").read())
        print("Schéma de la base de données vérifié/créé.")
    except Exception as e:
        print(f"Erreur lors de l'exécution de tables.py : {e}")
        exit()
    
    # Charger les deux ensembles de données
    clean_and_load_orders()
    load_refunds()
