import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector # module pour interagir avec MySQL
from fonctions import create_df_dict, export_df_to_mysql, original_engine

# Charger les variables d'environnement
load_dotenv()

# Récupérer les variables d'environnement
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
new_db_name = os.getenv('NEW_DB_NAME')

# Étape 1 : Créer une nouvelle base de données
conn = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password
)
cursor = conn.cursor() # utilisé pour exécuter les commandes SQL
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {new_db_name}") # crée la bdd si elle n'existe pas
cursor.close() # ferme le curseur
conn.close() # ferme la connexion à MySQL

# Étape 2 : Charger les données dans la nouvelle base de données
new_engine = create_engine(f'mysql+mysqlconnector://your_username:your_password@localhost/{new_db_name}')
# crée une nouvelle connexion SQLAlchemy pour la nouvelle bdd

def export_df_to_mysql(dataframes, engine):
    for name, df in dataframes.items(): # itère sur les paires nom/df du dico
        table_name = name.replace('df_', '').replace('_cleaned', '')  # Enlever le préfixe 'df_' et le suffixe '_cleaned'
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        # exporte le df vers MySQL en remplaçant la table si elle existe déjà
        print(f"DataFrame {name} exported to table {table_name} in MySQL")

# Exemple d'utilisation avec le dictionnaire des DataFrames
tables = ['customers', 'offices', 'employees', 'orderdetails', 'orders', 'payments', 'productlines', 'products']

# fonction qui crée un dico de df à partir des noms de tables et d'un moteur de 
# bdd existant.
dataframes = create_df_dict(tables, original_engine)

# appel la fonction pour exporter les df vers la nouvelle bdd
export_df_to_mysql(dataframes, new_engine)
