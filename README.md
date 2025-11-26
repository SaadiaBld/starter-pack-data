# Mini-pipeline de consolidation des ventes FreshKart

Ce projet implémente un pipeline de données quotidien pour consolider les informations de ventes, de clients et de remboursements pour l'entreprise FreshKart.

## Objectif

Le but de ce pipeline est de produire chaque jour un résumé financier de l'activité de la veille (J-1) dans un format stable et exploitable. Il nettoie les données brutes, applique des règles métier, et génère deux livrables principaux : une base de données SQLite et des fichiers CSV quotidiens.

## Structure du projet

- `load_data.py`: Script pour nettoyer les données sources (`customers.csv`, `orders_*.json`, `refunds.csv`) et les charger dans la base de données SQLite.
- `aggregator.py`: Script pour agréger les données nettoyées, calculer les métriques financières et générer les livrables finaux.
- `tables.py`: Script qui définit et crée le schéma de la base de données.
- `sales.db`: La base de données SQLite contenant les données nettoyées et agrégées.
- `requirements.txt`: Les dépendances Python nécessaires pour exécuter le projet.
- `data-68ed/data/input/`: Dossier contenant les données brutes (non versionné par Git).
- `daily_summary/`: Dossier où sont stockés les résumés CSV quotidiens (non versionné par Git).

## Comment exécuter le pipeline

1.  **Installer les dépendances :**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Exécuter le chargement des données :**
    Ce script va lire les fichiers sources, les nettoyer et peupler les tables `orders_clean` et `refunds` dans `sales.db`.
    ```bash
    python load_data.py
    ```

3.  **Exécuter l'agrégation :**
    Ce script va lire les tables nettoyées, effectuer les calculs et générer les résumés.
    ```bash
    python aggregator.py
    ```

## Livrables produits

1.  **Base de données `sales.db`** contenant deux tables :
    - `orders_clean`: Détails de chaque commande valide et nettoyée.
    - `daily_city_sales`: Agrégats quotidiens par ville et par canal de vente.

2.  **Fichiers CSV quotidiens** dans le dossier `daily_summary/`.
    - Nom du fichier : `daily_summary_YYYYMMDD.csv`
    - Séparateur : Point-virgule (`;`)
    - Encodage : UTF-8
    - Colonnes : `date;city;channel;orders_count;unique_customers;items_sold;gross_revenue_eur;refunds_eur;net_revenue_eur`


# Migration Spark
docker build -t jupyter-app .

docker run -d --rm   -p 8888:8888   -v "$(pwd):/workspace"   -v "$(pwd)/data-68ed/data:/app/data"   --name spark-jupyter-app   jupyter-app

docker logs spark-jupyter-app (pour avoir url et accéder au notebook sur localhost)

docker exec -it spark-jupyter-app bash (on rentre dans le shell du conteneur): puis on lance pytest

ou bien docker exec -it spark-jupyter-app pytest -vv (vv = verbose pour lire les logs, les prints des tests...)

