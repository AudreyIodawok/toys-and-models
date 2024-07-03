from toys_and_model_python_mysql import df_offices
from df_customers_clean import complete_state_dict

import numpy as np
import pandas as pd
import pprint

#**************************************************************************************************
# **DF_OFFICES**
#**************************************************************************************************

df_offices

##  officeCode           city             phone              addressLine1 addressLine2       state    country postalCode territory
##0          1  San Francisco   +1 650 219 4782         100 Market Street    Suite 300          CA        USA      94080        NA
##1          2         Boston   +1 215 837 0825          1550 Court Place    Suite 102          MA        USA      02107        NA
##2          3            NYC   +1 212 555 3000      523 East 53rd Street      apt. 5A          NY        USA      10022        NA
##3          4          Paris   +33 14 723 4404  43 Rue Jouffroy D'abbans         None        None     France      75017      EMEA
##4          5          Tokyo   +81 33 224 5000               4-1 Kioicho         None  Chiyoda-Ku      Japan   102-8578     Japan
##5          6         Sydney   +61 2 9264 2451     5-11 Wentworth Avenue     Floor #2        None  Australia   NSW 2010      APAC
##6          7         London  +44 20 7877 2041       25 Old Broad Street      Level 7        None         UK   EC2N 1HN      EMEA

df_offices.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 7 entries, 0 to 6
##Data columns (total 9 columns):
## #   Column        Non-Null Count  Dtype 
##---  ------        --------------  ----- 
## 0   officeCode    7 non-null      object
## 1   city          7 non-null      object
## 2   phone         7 non-null      object
## 3   addressLine1  7 non-null      object
## 4   addressLine2  5 non-null      object
## 5   state         4 non-null      object
## 6   country       7 non-null      object
## 7   postalCode    7 non-null      object
## 8   territory     7 non-null      object
##dtypes: object(9)
##memory usage: 632.0+ bytes

print(len(df_offices))
## 7

# Nombre de valeurs à 0 :
for column in df_offices.columns:
    if (df_offices[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## Il n'y a pas de valeur à 0.

# Nombre de valeurs NaN :
for column in df_offices.columns:
    num_nans = df_offices[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'addressLine2' contains 2 NaN values
## Column 'state' contains 3 NaN values


# Identifier les offices avec des valeurs NaN dans la colonne 'state'
offices_with_nan_state = df_offices[df_offices['state'].isna()]['city'].unique()

# Créer un dictionnaire avec les villes uniques comme clés et NaN comme valeurs pour les états manquants
nan_offices_state_mapping = {city: np.nan for city in offices_with_nan_state}

# Afficher le dictionnaire de correspondance pour les valeurs manquantes
print("Dictionnaire des valeurs manquantes pour les villes des offices:")
pprint.pprint(nan_offices_state_mapping)
print(len(nan_offices_state_mapping))
## {'London': nan, 'Paris': nan, 'Sydney': nan}
## 3

# Remplacer les valeurs NaN de state en fonction des villes :

# On réutilise le dictionnaire "villes : état" créé pour les clients et on l'update avec
# le dico créé "complete_offices_state_mapping"
complete_offices_state_mapping = {'London': 'England', 'Paris': 'France', 'Sydney': 'Australia'}
complete_state_dict.update(complete_offices_state_mapping)
pprint.pprint(complete_state_dict)

# Remplacer les states NaN avec les valeurs du dictionnaire :
df_offices['state'] = df_offices.apply(
    lambda row: complete_state_dict.get(row['city']) if pd.isna(row['state']) \
        else row['state'], axis=1
)

# Nombre de valeurs NaN :
for column in df_offices.columns:
    num_nans = df_offices[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'addressLine2' contains 2 NaN values
## La colonne state a bien été complétée.