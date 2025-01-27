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
# **DF_PRODUCTLINES INFORMATION**
#**************************************************************************************************

tables = ['productlines']

def create_df_dict(tables):
    dataframes = {}
    for table in tables:
        df = pd.read_sql_table(table, engine) # crée un df pour chq table
        dataframes["df_" + table] = df # ajout du df au dico avec clé "df_table"
        print(f"DataFrame df_{table} created") # impression du df créé
    return dataframes

dataframes = create_df_dict(tables)

df_productlines = dataframes['df_productlines']

df_productlines

##  productLine	    textDescription	                htmlDescription	    image
##0	Classic Cars    Attention car enthusiasts:      None	            None
##                  Make your wildest c...	

##1	Motorcycles	    Our motorcycles                 None	            None
##                  are state of the art replicas ...	

##2	Planes	        Unique, diecast airplane        None	            None
##                  and helicopter replic...	

##3	Ships	        The perfect holiday or          None	            None
##                  anniversary gift for ex...	
    
##4	Trains	        Model trains are a              None	            None
##                  rewarding hobby for enthusi...	
    
##5	Trucks and      The Truck and Bus models        None	            None
##  Buses	        are realistic replica...	


##6	Vintage Cars	Our Vintage Car models          None	            None
##                  realistically portray a...	


df_productlines.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 7 entries, 0 to 6
##Data columns (total 4 columns):
## #   Column           Non-Null Count  Dtype 
##---  ------           --------------  ----- 
## 0   productLine      7 non-null      object
## 1   textDescription  7 non-null      object
## 2   htmlDescription  0 non-null      object
## 3   image            0 non-null      object
##dtypes: object(4)
##memory usage: 352.0+ bytes

print(len(df_productlines))
## 7

# Nombre de valeurs à 0 :
for column in df_productlines.columns:
    if (df_productlines[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## Pas de colonne contenant des valeurs à 0.

# Nombre de valeurs NaN :
for column in df_productlines.columns:
    num_nans = df_productlines[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'htmlDescription' contains 7 NaN values
## Column 'image' contains 7 NaN values

## Aucune productline n'est attribuée à une description html ou à une image.

## -> Le df_productline ne sert qu'à donner une brève description de 
## chaque catégorie de produits.