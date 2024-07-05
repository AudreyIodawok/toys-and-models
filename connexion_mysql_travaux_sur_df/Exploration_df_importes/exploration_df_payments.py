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
# **DF_PAYMENTS INFORMATION**
#**************************************************************************************************

tables = ['payments']

def create_df_dict(tables):
    dataframes = {}
    for table in tables:
        df = pd.read_sql_table(table, engine) # crée un df pour chq table
        dataframes["df_" + table] = df # ajout du df au dico avec clé "df_table"
        print(f"DataFrame df_{table} created") # impression du df créé
    return dataframes

dataframes = create_df_dict(tables)

df_payments = dataframes['df_payments']

df_payments

##      customerNumber	    checkNumber	    paymentDate	    amount
##0	    103	                HQ336336	    2022-10-18	    6066.78
##1	    103	                JM555205	    2021-06-03	    14571.44
##2	    103	                OM314933	    2022-12-17	    1676.14
##3	    112	                BO864823	    2022-12-16	    14191.12
##4	    112	                HQ55022	        2021-06-04	    32641.98
##.     ...	                ...	            ...	            ...
##252	489	                PO860906	    2022-01-29	    7310.42
##253	495	                BH167026	    2021-12-24	    59265.14
##254	495	                FN155234	    2022-05-13	    6276.60
##255	496	                MB342426	    2021-07-14	    32077.44
##256	496	                MN89921	        2022-12-30	    52166.00

df_payments.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 257 entries, 0 to 256
##Data columns (total 4 columns):
## #   Column          Non-Null Count  Dtype         
##---  ------          --------------  -----         
## 0   customerNumber  257 non-null    int64         
## 1   checkNumber     257 non-null    object        
## 2   paymentDate     257 non-null    datetime64[ns]
## 3   amount          257 non-null    float64       
##dtypes: datetime64[ns](1), float64(1), int64(1), object(1)
##memory usage: 8.2+ KB

print(len(df_payments))
## 257

# Nombre de valeurs à 0 :
for column in df_payments.columns:
    if (df_payments[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## Pas de colonne à 0.

# Nombre de valeurs NaN :
for column in df_payments.columns:
    num_nans = df_payments[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Pas de colonnes contenant des valeurs NaN.


# Montants payés et nombre de paiements par clients :

# Agrégation des données par numéro de client
total_amount_per_customer = df_payments.groupby('customerNumber').agg(
    total_amount=('amount', 'sum'), # somme des montants payés
    payments_quantity = ('checkNumber', 'size') # nombre de paiements
).reset_index()

# Tri des commandes par quantité totale par ordre décroissant
total_amount_per_customer = total_amount_per_customer.sort_values(by = 'total_amount',\
                                                                ascending = False)
total_amount_per_customer

##      customerNumber	    total_amount	payments_quantity
##9	    141	                668843.50	    12
##5	    124	                500590.20	    8
##2	    114	                180585.07	    4
##14	151	                177913.95	    4
##13	148	                156251.03	    4
##.     ...	                ...	            ...
##72	381	                29217.18	    4
##90	473	                25358.32	    2
##0	    103	                22314.36	    3
##28	198	                21554.26	    3
##36	219	                7918.60	        2


# Période concernée par les paiements :

# Calcul des dates de paiements min et max
min_date_payment = df_payments['paymentDate'].min()
min_date_payment_str = min_date_payment.strftime('%d/%m/%Y')
max_date_payment = df_payments['paymentDate'].max()
max_date_payment_str = max_date_payment.strftime('%d/%m/%Y')

# Calcul de la période
duration_payment = (max_date_payment - min_date_payment).days

print(f"Les paiements s'échelonnent du {min_date_payment_str} au {max_date_payment_str}, couvrant une période de {duration_payment} jours.")
## Les paiements s'échelonnent du 14/01/2021 au 24/03/2023, couvrant une période de 799 jours.