### Architecture AVANT migration (PAndas)

Fichiers bruts
    ↓
 load_data.py (Pandas)
    ↓
   SQLite
    ↓
 aggregator.py (Pandas)
    ↓
 Fichiers CSV


### Architectture APRES migration (Spark)

Fichiers bruts
    ↓
 Application PySpark (1 fichier plus adapté ou 2 pour la pedagogie)
    ↓
 DataFrames Spark
    ↓
 Agrégation Spark distribuée
    ↓
 Fichiers CSV/Parquet
