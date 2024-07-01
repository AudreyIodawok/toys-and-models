import pandas as pd
import os
#import sqlalchemy
from sqlalchemy import create_engine, inspect

#import dotenv
from dotenv import load_dotenv

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

# Vérifier que toutes les variables d'environnement sont définies
if not all([MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE]):
    raise EnvironmentError("One or more MySQL environment variables are not defined")
else: 
    print("MySQL environment variables are correctly defined")

# Connexion à la base de données MySQL
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

# Inspecter les tables dans la base de données
inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tables in the database:", tables)

# Exemple de lecture d'une table dans un DataFrame pandas
df = pd.read_sql_table('customers', engine)
print(df.head())