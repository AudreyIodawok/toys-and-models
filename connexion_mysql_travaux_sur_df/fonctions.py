import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

#___________________________________________________________________________________________________
# Créer l'engine depuis l'environnement '.env' :

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Récupérer les variables d'environnement
mysql_host = os.getenv('MYSQL_HOST')
mysql_port = os.getenv('MYSQL_PORT')
mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_database = os.getenv('MYSQL_DATABASE')

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
new_db_name = os.getenv('NEW_DB_NAME')

def create_engine_from_env(db_name):
    """
    Crée un moteur SQLAlchemy à partir des variables d'environnement.

    :param db_name: Le nom de la base de données à laquelle se connecter
    :param db_user: le nom d'utilisateur de la base de données
    :param db_password: le mot de passe de la base de données
    :param db_name: le nom de la base de données à laquelle se connecter
    :return: Le moteur SQLAlchemy
    """
    return create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

# Créer le moteur pour la base de données originale
original_engine = create_engine_from_env(mysql_database)

# Créer le moteur pour la nouvelle base de données
new_engine = create_engine_from_env(new_db_name)

#___________________________________________________________________________________________________
# Créer le dico des dataframes (voir toys_and_model_python_mysql.py) :

def create_df_dict(tables, engine):
    dataframes = {}
    for table in tables:
        df = pd.read_sql_table(table, engine) # crée un df pour chq table
        dataframes[f'df_{table}'] = df # ajout du df au dico avec clé "df_table"
        print(f"DataFrame df_{table} created") # impression du df créé
    return dataframes

#___________________________________________________________________________________________________
# Créer les dataframes (voir toys_and_model_python_mysql.py) :

def create_dataframe(dataframes, key):
    if key in dataframes:
        return dataframes[key]
        # Utilise le dico dataframes et retourne la valeur correspondante
    else:
        print(f"Dataframe{key} n'existe pas dans le dictionnaire 'dataframes'.")

#___________________________________________________________________________________________________
# state_dict.py (voir exploration_df_customers.py) :

complete_state_dict = {
    'Nantes': 'France', 'Stavern': 'Norway', 'Warszawa': 'Poland',
    'Frankfurt': 'Germany', 'Madrid': 'Spain', 'Luleå': 'Sweden',
    'Kobenhavn': 'Denmark', 'Lyon': 'France', 'Singapore': 'Singapore',
    'Bergen': 'Norway', 'Lisboa': 'Portugal', 'Lille': 'France', 'Paris': 'France',
    'Helsinki': 'Finland', 'Manchester': 'England', 'Dublin': 'Ireland',
    'Liverpool': 'England', 'Strasbourg': 'France', 'Central Hong Kong': 'Hong Kong',
    'Barcelona': 'Spain', 'Cunewalde': 'Germany', 'Århus': 'Denmark',
    'Toulouse': 'France', 'Torino': 'Italy', 'Versailles': 'France',
    'Köln': 'Germany', 'München': 'Germany', 'Bergamo': 'Italy', 'Fribourg': 'Switzerland',
    'Genève': 'Switzerland', 'Oslo': 'Norway', 'Amsterdam': 'Netherlands', 'Berlin': 'Germany',
    'Oulu': 'Finland', 'Bruxelles': 'Belgium', 'Auckland': 'New Zealand', 'London': 'England',
    'Espoo': 'Finland', 'Brandenburg': 'Germany', 'Marseille': 'France',
    'Reims': 'France', 'Münster': 'Germany', 'Bern': 'Switzerland', 'Charleroi': 'Belgium',
    'Salzburg': 'Austria', 'Makati City': 'Philippines', 'Reggio Emilia': 'Italy',
    'Stuttgart': 'Germany', 'Wellington': 'New Zealand', 'Münich': 'Germany',
    'Leipzig': 'Germany', 'Bräcke': 'Sweden', 'Graz': 'Austria', 'Aachen': 'Germany',
    'Milan': 'Italy', 'Mannheim': 'Germany', 'Saint Petersburg': 'Russia',
    'Herzlia': 'Israel', 'Sevilla': 'Spain'
}

#___________________________________________________________________________________________________
# Mise à jour du dictionnaire 'state' (voir exploration_df_offices.py) :

def update_state_dict(new_entries):
    global complete_state_dict
    if complete_state_dict is None:
        complete_state_dict = {}
    complete_state_dict.update(new_entries)
    return complete_state_dict

#___________________________________________________________________________________________________
# postalcode_dict.py (voir exploration_df_customers.py) :

complete_postalCode_dict = {'Central Hong Kong': '999077', 'Auckland': '0600', 'Cork': 'P31',\
                            'Wellington': '6011', 'Milan': '20019'}


#___________________________________________________________________________________________________
# Nettoyer et compléter le df (voir exploration_df_customers.py) :

def clean_and_complete_df(df):
    if df is not None:
        # Retirer les espaces en trop à la fin des noms de city ou state si existants
        df['city'] = df['city'].str.strip()
        df['state'] = df['state'].str.strip()
        
        # Corriger l'orthographe de 'Munich' à 'Münich'
        df['city'] = df['city'].str.replace('Munich', 'Münich')

        # Compléter les valeurs manquantes dans 'state' en fonction de 'city' et du dictionnaire complete_state_dict
        df['state'] = df.apply(
            lambda row: complete_state_dict.get(row['city']) if pd.isna(row['state']) else row['state'],
            axis=1
        )
        # Compléter les valeurs manquantes dans 'postalcode' en fonction de 'city' et du dictionnaire complete_postalCode_dict
        df['postalCode'] = df.apply(
            lambda row: complete_postalCode_dict.get(row['city']) if pd.isna(row['postalCode']) else row['postalCode'],
            axis=1
        )   

        return df
    else:
        print("Cannot proceed without df")
        return None

#___________________________________________________________________________________________________    
# Compléter le df (voir exploration_df_offices.py) :

def complete_df(df):
    if df is not None:
        # Mise à jour du dictionnaire complete_state_dict
        complete_offices_state_mapping = {'London': 'England', 'Paris': 'France', 'Sydney': 'Australia'}
        update_state_dict(complete_offices_state_mapping)
        # Compléter les valeurs manquantes dans 'state' en fonction de 'city' et du dictionnaire complete_state_dict
        df['state'] = df.apply(
            lambda row: complete_state_dict.get(row['city']) if pd.isna(row['state']) else row['state'],
            axis=1
        )
        return df
    else:
        print("Cannot proceed without df")
        return None

#___________________________________________________________________________________________________        
# Enregistrer cleaned_employees_df au format .csv :

def save_csv_file(df, save_path, file_name = 'df.csv'):

    """
    Enregistre un DataFrame en fichier .csv dans le dossier spécifié.

    :param df: Le DataFrame à enregistrer
    :param save_path: Le chemin du dossier où enregistrer le fichier .csv
    :param file_name: Le nom du fichier .csv à enregistrer (par défaut 'df.csv')
    """

    # Vérifier si le dossier existe, sinon le créer
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    # Chemin complet du fichier .csv
    csv_file_path = os.path.join(save_path, file_name)

    # Enregistrer le DataFrame au format .csv
    df.to_csv(csv_file_path, index=False)

    print(f"Le fichier '{file_name}' a été enregistré à l'emplacement : {csv_file_path}")

#___________________________________________________________________________________________________        
# Exporter les df vers la nouvelle base de données :

# Connexion à la nouvelle base de données via SQLAlchemy
# new_engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/new_toys_and_models')

# Fonction pour exporter les DataFrames vers MySQL
def export_df_to_mysql(dataframes, engine):
    for name, df in dataframes.items():
        table_name = name.replace('df_', '').replace('_cleaned','')  # Enlever 'df_' et '_cleaned' pour obtenir le nom de la table
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"DataFrame {name} exported to table {table_name} in MySQL")

