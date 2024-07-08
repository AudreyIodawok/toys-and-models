import duckdb
import pandas as pd
import os

directory = 'C:\\Users\\Utilisateur\\Documents\\Formation_Data\\Projets\\Projet_1_toys_and_models\\Datasets_csv\\vs_code\\df_cleaned_csv'

# Créer une connexion à DuckDB
con = duckdb.connect(database=':memory:')  # Utiliser une base de données en mémoire pour des opérations temporaires

# Dictionnaire de mapping des noms de fichiers aux noms de tables
files_to_tables = {
    'df_customers_cleaned.csv': 'customers',
    'df_employees_cleaned.csv': 'employees',
    'df_offices_cleaned.csv': 'offices',
    'df_orders_cleaned.csv': 'orders',
    'df_orderdetails_cleaned.csv': 'orderdetails',
    'df_payments_cleaned.csv': 'payments',
    'df_productlines.csv': 'productlines',
    'df_products.csv': 'products'
}

# Charger chaque fichier CSV dans une table DuckDB
for file_name, table_name in files_to_tables.items(): # parcours les clés (fichiers csv) et valeurs (noms de tables)
    file_path = os.path.join(directory, file_name)
    # exécute une requête SQL sur la connexion
    con.execute(f""" # 
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{file_path}')
    """)

# Afficher la liste des tables pour vérifier
print(con.execute("SHOW TABLES").fetchall())

## [('customers',), ('employees',), ('offices',), ('orderdetails',), ('orders',), ('payments',), ('productlines',), ('products',)]