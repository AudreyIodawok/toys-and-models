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
# **DF_ORDERS INFORMATION**
#**************************************************************************************************

tables = ['orders']

def create_df_dict(tables):
    dataframes = {}
    for table in tables:
        df = pd.read_sql_table(table, engine) # crée un df pour chq table
        dataframes["df_" + table] = df # ajout du df au dico avec clé "df_table"
        print(f"DataFrame df_{table} created") # impression du df créé
    return dataframes

dataframes = create_df_dict(tables)

df_orders = dataframes['df_orders']

df_orders

##  orderNumber	orderDate	requiredDate	shippedDate	status	comments	            customerNumber
##0	10100	    2021-07-23	2021-07-30	    2021-07-27	Shipped	None	                363
##1	10101	    2021-07-26	2021-08-04	    2021-07-28	Shipped	Check on availability.	128
##2	10102	    2021-07-27	2021-08-04	    2021-07-31	Shipped	None	                181
##3	10103	    2021-08-15	2021-08-24	    2021-08-19	Shipped	None	                121
##4	10104	    2021-08-17	2021-08-26	    2021-08-18	Shipped	None	                141
##.	...	        ...	        ...	            ...	        ...	    ...	                    ...

df_orders.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 299 entries, 0 to 298
##Data columns (total 7 columns):
## #   Column          Non-Null Count  Dtype         
##---  ------          --------------  -----         
## 0   orderNumber     299 non-null    int64         
## 1   orderDate       299 non-null    datetime64[ns]
## 2   requiredDate    299 non-null    datetime64[ns]
## 3   shippedDate     294 non-null    datetime64[ns]
## 4   status          299 non-null    object        
## 5   comments        71 non-null     object        
## 6   customerNumber  299 non-null    int64         
##dtypes: datetime64[ns](3), int64(2), object(2)
##memory usage: 16.5+ KB

print(len(df_orders))
## 299

# Nombre de valeurs à 0 :
for column in df_orders.columns:
    if (df_orders[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## Pas de valeurs à 0

# Nombre de valeurs NaN :
for column in df_orders.columns:
    num_nans = df_orders[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'shippedDate' contains 5 NaN values
## Column 'comments' contains 228 NaN values

## -> A priori, 5 commandes n'ont pas été envoyées (pas de date d'expédition).

# Contenus uniques de la colonne 'comments' :
print(df_orders['comments'].unique())
## [None 'Check on availability.'
## 'Difficult to negotiate with customer. We need more marketing materials'
## 'Customer requested that FedEx Ground is used for this shipping'
## 'Customer requested that ad materials (such as posters, pamphlets) be included 
## in the shippment'
## 'Customer has worked with some of our vendors in the past and is aware of their 
## MSRP'
## 'Customer very concerned about the exact color of the models. There is high 
## risk that he may dispute the order because there is a slight color mismatch'
## 'Customer requested special shippment. The instructions were passed along to 
## the warehouse'
## 'Customer is interested in buying more Ferrari models'
## 'Can we deliver the new Ford Mustang models by end-of-quarter?'
## 'They want to reevaluate their terms agreement with Finance.'
## "This order was disputed, but resolved on 11/1/2003; Customer doesn't like the 
## colors and precision of the models'..."]

## -> Apparemment les commentaires sont laissés par du personnel de l'entreprise
## concernant des demandes / remarques du client ayant passé commande.
## Peut servir pour une étude NLP ultérieure bien que peu de commentaires.

# Colonne 'status' :
print(df_orders['status'].unique())
## ['Shipped' 'Resolved' 'Cancelled' 'On Hold']

# Vérification des statuts de commandes pour les commandes où shippedDate est NaN :
order_status_wo_shipdate = df_orders['status'][df_orders['shippedDate'].isna()]
order_status_wo_shipdate

##67     Cancelled
##148    Cancelled
##160    Cancelled
##162    Cancelled
##234      On Hold

## Les commandes sans shippedDate sont soit annulées soit en attente.

# Voir si des n° de commandes sont dupliqués :
duplicates = df_orders[df_orders.duplicated(subset='orderNumber', keep=False)]
print(duplicates)

## Empty DataFrame
## Columns: [orderNumber, orderDate, requiredDate, shippedDate, status, comments, customerNumber]
## Index: []

# Afficher les commandes duppliquées si il y en a :
#duplicate_counts = df_orders['orderNumber'].value_counts()

# Afficher uniquement les commandes dupliquées
#print(duplicate_counts[duplicate_counts > 1])

# Période concernée par les paiements :

# Calcul des dates de commandes min et max
min_date_order = df_orders['orderDate'].min()
min_date_order_str = min_date_order.strftime('%d/%m/%Y')
max_date_order = df_orders['orderDate'].max()
max_date_order_str = max_date_order.strftime('%d/%m/%Y')

# Calcul de la période
duration_order = (max_date_order - min_date_order).days

print(f"Les commandes s'échelonnent du {min_date_order_str} au {max_date_order_str}, couvrant une période de {duration_order} jours.")
## Les commandes s'échelonnent du 23/07/2021 au 10/10/2023, couvrant une période de 809 jours.