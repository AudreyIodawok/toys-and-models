import numpy as np
import pandas as pd
import pprint
import sys
import os

# Ajouter le répertoire parent au chemin de recherche du module 'engine'
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(parent_dir)
#print(parent_dir)
# Importer le module
from toys_and_model_python_mysql import engine

#**************************************************************************************************
# **DF_CUSTOMERS INFORMATION**
#**************************************************************************************************
tables = ['customers']

def create_df_dict(tables):
    dataframes = {}
    for table in tables:
        df = pd.read_sql_table(table, engine) # crée un df pour chq table
        dataframes["df_" + table] = df # ajout du df au dico avec clé "df_table"
        print(f"DataFrame df_{table} created") # impression du df créé
    return dataframes

dataframes = create_df_dict(tables)

df_customers = dataframes['df_customers']

df_customers

df_customers.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 122 entries, 0 to 121
##Data columns (total 13 columns):
## #   Column                  Non-Null Count  Dtype  
##---  ------                  --------------  -----  
## 0   customerNumber          122 non-null    int64  
## 1   customerName            122 non-null    object 
## 2   contactLastName         122 non-null    object 
## 3   contactFirstName        122 non-null    object 
## 4   phone                   122 non-null    object 
## 5   addressLine1            122 non-null    object 
## 6   addressLine2            22 non-null     object 
## 7   city                    122 non-null    object 
## 8   state                   49 non-null     object 
## 9   postalCode              115 non-null    object 
## 10  country                 122 non-null    object 
## 11  salesRepEmployeeNumber  100 non-null    float64
## 12  creditLimit             122 non-null    float64
##dtypes: float64(2), int64(1), object(10)
##memory usage: 12.5+ KB


print(len(df_customers))
## 122

# Nombre de valeurs à 0 :
for column in df_customers.columns:
   if (df_customers[column] == 0).any():
       print(f"Column '{column}' contains zeros")
## Column 'creditLimit' contains zeros

# Nombre de valeurs NaN :
for column in df_customers.columns:
    num_nans = df_customers[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'addressLine2' contains 100 NaN values
## Column 'state' contains 73 NaN values
## Column 'postalCode' contains 7 NaN values
## Column 'salesRepEmployeeNumber' contains 22 NaN values

## Il y a 73 valeurs manquantes pour les pays du client, 7 valeurs manquantes
## pour les codes postaux et 22 clients qui n'ont pas de commercial attribué.

#**************************************************************************************************
# **DF_CUSTOMERS NETTOYAGE ET COMPLETION**
#**************************************************************************************************

#___________________________________________________________________________________________________
# NETTOYAGE :
#___________________________________________________________________________________________________

# Retirer les espaces en trop à la fin des noms de city ou state si existants :
df_customers['city'] = df_customers['city'].str.strip()
df_customers['state'] = df_customers['state'].str.strip()

# Corriger orthographe de city == 'Münich' :
df_customers['city'] = df_customers['city'].str.replace('Munich', 'Münich')

#___________________________________________________________________________________________________
# COMPLETER LES VALEURS MANQUANTES DE STATE :
#___________________________________________________________________________________________________

# Pour les pays manquants :
rows_with_nan_state = df_customers[df_customers['state'].isna()]

# Imprimer les lignes avec NaN dans la colonne 'state', incluant les noms des villes
print("Rows with NaN in 'state' column:")
print(rows_with_nan_state[['customerNumber', 'city', 'state']])

#___________________________________________________________________________________________________
# SOLUTION 1 : CREER UN DICTIONNAIRE A PARTIR DU DF POUR REMPLACER LES VALEURS NAN

# Créer un dictionnaire de correspondance ville-état à partir des données complètes
city_state_mapping = df_customers.dropna().drop_duplicates(subset='city')\
    .set_index('city')['state'].to_dict()
print(city_state_mapping)
## {'Melbourne': 'Victoria', 'NYC': 'NY', 'New Haven': 'CT', 'Cowes': 'Isle of Wight', 
## 'North Sydney': 'NSW', 'Chatswood': 'NSW'}

# Remplacer les valeurs NaN dans la colonne 'state' en utilisant le dictionnaire de correspondance
df_customers['state'] = df_customers.apply(
    lambda row: city_state_mapping.get(row['city']) if pd.isna(row['state']) \
        else row['state'], axis=1
)

# Afficher le DataFrame après remplacement des valeurs NaN
print("DataFrame after filling NaN values in 'state' column:")
print(df_customers)

## -> Cette option ne fonctionnera pas car les valeurs de states manquantes ne 
## se trouvent pas dans le dictionnaire issu du df.

#___________________________________________________________________________________________________
# SOLUTION 2 : CREER UN DICTIONNAIRE MANUEL REMPLACER LES VALEURS NAN

# Identifier les villes avec des valeurs NaN dans la colonne 'state'
cities_with_nan_state = df_customers[df_customers['state'].isna()]['city'].unique()

# Créer un dictionnaire avec les villes uniques comme clés et NaN comme valeurs pour les états manquants
nan_city_state_mapping = {city: np.nan for city in cities_with_nan_state}

# Afficher le dictionnaire de correspondance pour les valeurs manquantes
print("Dictionnaire des valeurs manquantes pour les villes:")
pprint.pprint(nan_city_state_mapping)
print(len(nan_city_state_mapping))

## Dictionnaire des valeurs manquantes de pays pour les villes:
## {'Aachen': nan,
## 'Amsterdam': nan,
## 'Auckland': nan,
## 'Barcelona': nan,
## 'Bergamo': nan,
## 'Bergen': nan,
## 'Berlin': nan,
## 'Bern': nan,
## 'Brandenburg': nan,
## 'Bruxelles': nan,
## 'Bräcke': nan,
## 'Central Hong Kong': nan,
## 'Charleroi': nan,
## 'Cunewalde': nan,
## 'Dublin': nan,
## 'Espoo': nan,
## 'Frankfurt': nan,
## 'Fribourg': nan,
## 'Genève': nan,
## 'Graz': nan,
## 'Helsinki': nan,
## 'Herzlia': nan,
## 'Kobenhavn': nan,
## 'Köln': nan,
## 'Leipzig': nan,
## 'Lille': nan,
## 'Lisboa': nan,
## 'Liverpool': nan,
## 'London': nan,
## 'Luleå': nan,
## 'Lyon': nan,
## 'Madrid': nan,
## 'Makati City': nan,
## 'Manchester': nan,
## 'Mannheim': nan,
## 'Marseille': nan,
## 'Milan': nan,
## 'Münich': nan,
## 'München': nan,
## 'Münster': nan,
## 'Nantes': nan,
## 'Oslo': nan,
## 'Oulu': nan,
## 'Paris': nan,
## 'Reggio Emilia': nan,
## 'Reims': nan,
## 'Saint Petersburg': nan,
## 'Salzburg': nan,
## 'Sevilla': nan,
## 'Singapore': nan,
## 'Stavern': nan,
## 'Strasbourg': nan,
## 'Stuttgart': nan,
## 'Torino': nan,
## 'Toulouse': nan,
## 'Versailles': nan,
## 'Warszawa': nan,
## 'Wellington': nan,
## 'Århus': nan}
## 59

print(nan_city_state_mapping)

# Dictionnaire complété :
complete_state_dict = {'Nantes': 'France', 'Stavern': 'Norway', 'Warszawa': 'Poland',\
                       'Frankfurt': 'Germany', 'Madrid': 'Spain', 'Luleå': 'Sweden',\
                       'Kobenhavn': 'Denmark', 'Lyon': 'France', 'Singapore': 'Singapore',\
                       'Bergen': 'Norway', 'Lisboa': 'Portugal', 'Lille': 'France', 'Paris': 'France',\
                       'Helsinki': 'Finland', 'Manchester': 'England', 'Dublin': 'Ireland',\
                       'Liverpool': 'England', 'Strasbourg': 'France', 'Central Hong Kong': 'Hong Kong',\
                       'Barcelona': 'Spain', 'Cunewalde': 'Germany', 'Århus': 'Denmark', \
                       'Toulouse': 'France', 'Torino': 'Italy', 'Versailles': 'France', \
                       'Köln': 'Germany', 'München': 'Germany', 'Bergamo': 'Italy', 'Fribourg': 'Switzerland',\
                       'Genève': 'Switzerland', 'Oslo': 'Norway', 'Amsterdam': 'Netherlands', 'Berlin': 'Germany',\
                       'Oulu': 'Finland', 'Bruxelles': 'Belgium', 'Auckland': 'New Zealand', 'London': 'England',\
                       'Espoo': 'Finland', 'Brandenburg': 'Germany', 'Marseille': 'France',\
                       'Reims': 'France', 'Münster': 'Germany', 'Bern': 'Switzerland', 'Charleroi': 'Belgium',\
                       'Salzburg': 'Austria', 'Makati City': 'Philippines', 'Reggio Emilia': 'Italy',\
                       'Stuttgart': 'Germany', 'Wellington': 'New Zealand', 'Münich': 'Germany',\
                       'Leipzig': 'Germany', 'Bräcke': 'Sweden', 'Graz': 'Austria', 'Aachen': 'Germany',\
                       'Milan': 'Italy', 'Mannheim': 'Germany', 'Saint Petersburg': 'Russia',\
                       'Herzlia': 'Israel', 'Sevilla': 'Spain'}

# Remplacer les states NaN avec les valeurs du dictionnaire :
df_customers['state'] = df_customers.apply(
    lambda row: complete_state_dict.get(row['city']) if pd.isna(row['state']) \
        else row['state'], axis=1
)

# Nombre de valeurs NaN après remplacement des valeurs manquantes de state :
for column in df_customers.columns:
    num_nans = df_customers[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'addressLine2' contains 100 NaN values
## Column 'postalCode' contains 7 NaN values
## Column 'salesRepEmployeeNumber' contains 22 NaN values

## Les valeurs manquantes de state ont bien été complétées.

#___________________________________________________________________________________________________
# COMPLETER LES VALEURS MANQUANTES DE POSTALCODE :
#___________________________________________________________________________________________________

# Retirer les espaces en trop à la fin des postalCode (au cas où) :
df_customers['postalCode'] = df_customers['postalCode'].str.strip()

# Pour les codes postaux manquants :
rows_with_nan_postalCode = df_customers[df_customers['postalCode'].isna()]

# Imprimer les lignes avec NaN dans la colonne 'state', incluant les noms des villes
print("Rows with NaN in 'postalCode' column:")
print(rows_with_nan_postalCode[['customerNumber', 'city', 'postalCode']])

#___________________________________________________________________________________________________
# SOLUTION 1 : CREER UN DICTIONNAIRE A PARTIR DU DF POUR REMPLACER LES VALEURS NAN

# Créer un dictionnaire de correspondance ville-code postal à partir des données complètes
city_postalCode_mapping = df_customers.dropna().drop_duplicates(subset='city')\
    .set_index('city')['postalCode'].to_dict()

# Trier le dictionnaire par clés et imprimer les paires clé-valeur*
sorted_city_postalCode_mapping = {key: city_postalCode_mapping[key] for key in\
                                  sorted(city_postalCode_mapping)}

print(sorted_city_postalCode_mapping)
## {'Bergen': 'N 5804', 'Chatswood': '2067', 'Cowes': 'PO31 7PJ', 'Dublin': '2', 'Espoo': 'FIN-02271', 'Madrid': '28023', 'Makati City': '1227 MM', 'Melbourne': '3004', 'NYC': '10022', 'New Haven': '97823', 'North Sydney': '2060', 'Oslo': 'N 0106', 'Singapore': '079903'}

# Remplacer les valeurs NaN dans la colonne 'state' en utilisant le dictionnaire de correspondance
df_customers['state'] = df_customers.apply(
    lambda row: city_postalCode_mapping.get(row['city']) if pd.isna(row['state']) \
        else row['state'], axis=1
)

#___________________________________________________________________________________________________
# SOLUTION 2 : CREER UN DICTIONNAIRE MANUEL REMPLACER LES VALEURS NAN

# Identifier les villes avec des valeurs NaN dans la colonne 'postalCode'
cities_with_nan_postalCode = df_customers[df_customers['postalCode'].isna()]['city'].unique()
print(cities_with_nan_postalCode)

# Créer un dictionnaire avec les villes uniques comme clés et NaN comme valeurs pour les codes postaux manquants
nan_city_postalCode_mapping = {city: np.nan for city in cities_with_nan_postalCode}

# Afficher le dictionnaire de correspondance pour les valeurs manquantes
print("Dictionnaire des valeurs manquantes pour les villes:")
pprint.pprint(nan_city_postalCode_mapping)
print(len(nan_city_postalCode_mapping))

## Dictionnaire des valeurs manquantes pour les codes postaux :
## {'Auckland': nan,
## 'Central Hong Kong': nan,
## 'Cork': nan,
## 'Milan': nan,
## 'Wellington': nan}
##5

print(nan_city_postalCode_mapping)

# Dictionnaire complété :
complete_postalCode_dict = {'Central Hong Kong': '999077', 'Auckland': '0600', 'Cork': 'P31',\
                            'Wellington': '6011', 'Milan': '20019'}

# Remplacer les states NaN avec les valeurs du dictionnaire :
df_customers['postalCode'] = df_customers.apply(
    lambda row: complete_postalCode_dict.get(row['city']) if pd.isna(row['postalCode']) \
        else row['postalCode'], axis=1
)

# Nombre de valeurs NaN après remplacement des valeurs manquantes de state :
for column in df_customers.columns:
    num_nans = df_customers[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'addressLine2' contains 100 NaN values
## Column 'salesRepEmployeeNumber' contains 22 NaN values

## Les valeurs manquantes de postalCode ont bien été complétées.

#**************************************************************************************************
# *NOTA BENE
#**************************************************************************************************
# Trier le dictionnaire par clés et imprimer les paires clé-valeur :

sorted_city_postalCode_mapping = {key: city_postalCode_mapping[key] for key in\
                                  sorted(city_postalCode_mapping)}

# - La fonction sorted() trie les clés du dictionnaire city_postalCode_mapping 
# dans l'ordre alphabétique par défaut.
# - L'expression entière utilise une compréhension de dictionnaire pour créer
# un nouveau dictionnaire. Elle parcourt les clés triées
# (sorted(city_postalCode_mapping)) et pour chaque clé, elle récupère la
# la valeur correspondante dans 'city_postalCode_mapping'.

# **key représente chaque clé triée.
# **city_postalCode_mapping[key] est la valeur associée à cette clé dans le 
# dictionnaire original.

# -> Donc, pour chaque clé triée, elle crée une nouvelle entrée dans le 
# dictionnaire sorted_city_postalCode_mapping.

#**************************************************************************************************

