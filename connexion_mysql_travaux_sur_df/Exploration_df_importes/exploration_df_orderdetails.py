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
# **DF_ORDERDETAILS INFORMATION**
#**************************************************************************************************

tables = ['orderdetails']

def create_df_dict(tables):
    dataframes = {}
    for table in tables:
        df = pd.read_sql_table(table, engine) # crée un df pour chq table
        dataframes["df_" + table] = df # ajout du df au dico avec clé "df_table"
        print(f"DataFrame df_{table} created") # impression du df créé
    return dataframes

dataframes = create_df_dict(tables)

df_orderdetails = dataframes['df_orderdetails']

df_orderdetails

##orderNumber	productCode	quantityOrdered	priceEach	orderLineNumber
##0	10100	    S18_1749	30	            136.00	    3
##1	10100	    S18_2248	50	            55.09	    2
##2	10100	    S18_4409	22	            75.46	    4
##3	10100	    S24_3969	49	            35.29	    1
##4	10101	    S18_2325	25	            108.06	    4
##. ...	        ...	        ...	            ...	        ...

df_orderdetails.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 2786 entries, 0 to 2785
##Data columns (total 5 columns):
## #   Column           Non-Null Count  Dtype  
##---  ------           --------------  -----  
## 0   orderNumber      2786 non-null   int64  
## 1   productCode      2786 non-null   object 
## 2   quantityOrdered  2786 non-null   int64  
## 3   priceEach        2786 non-null   float64
## 4   orderLineNumber  2786 non-null   int64  
##dtypes: float64(1), int64(3), object(1)
##memory usage: 109.0+ KB

df_orderdetails.describe()

#           #orderNumber	quantityOrdered	        priceEach	    orderLineNumber
##count     2786.000000	    2786.000000	            2786.000000	    2786.000000
##mean	    10248.902010	34.788586	            90.833148	    6.503948
##std	    85.561939	    8.981379	            36.581123	    4.236986
##min	    10100.000000	20.000000	            26.550000	    1.000000
##25%	    10176.000000	27.000000	            62.000000	    3.000000
##50%	    10250.000000	34.000000	            85.980000	    6.000000
##75%	    10322.000000	43.000000	            114.650000	    9.000000
##max	    10398.000000	59.000000	            214.300000	    18.000000

print(len(df_orderdetails))
## 2786

# Nombre de valeurs à 0 :
for column in df_orderdetails.columns:
    if (df_orderdetails[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## Pas de valeur à 0

# Nombre de valeurs NaN :
for column in df_orderdetails.columns:
    num_nans = df_orderdetails[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## pas de valeur NaN

# Voir la quantité de n° de commandes uniques :
print(len(df_orderdetails['orderNumber'].unique().tolist()))
## 299
## Ce nombre est conforme au nombre de commandes de df_orders.


# Calcul du montant total de chaque ligne de commande
df_orderdetails['amount_orderline'] = df_orderdetails['priceEach'] * df_orderdetails['quantityOrdered']

# Agrégation des données par numéro de commande
total_quantity_per_order = df_orderdetails.groupby('orderNumber').agg(
    total_quantity=('quantityOrdered', 'sum'), # somme des quantités commandées
    order_lines=('orderLineNumber', 'nunique'), # nombre de lignes de commande / commande
    amount_per_order=('amount_orderline', 'sum') # somme des montants de lignes de cde / cde
).reset_index()

# Tri des commandes par quantité totale par ordre décroissant
total_quantity_per_order = total_quantity_per_order.sort_values(by = 'total_quantity',\
                                                                ascending = False)
total_quantity_per_order

                ##orderNumber	total_quantity	order_lines	amount_per_order
##122	        10222	        717	            18	        56822.65
##6             10106	        675	            18	        52151.81
##65	        10165	        670	            18	        67392.85
##286	        10386	        650	            18	        46968.52
##68	        10168	        642	            18	        50743.65
##...	        ...	            ...	            ...	        ...
##177	        10277	        28	            1	        2611.84
##89	        10189	        28	            1	        3879.96
##16	        10116	        27	            1	        1627.56
##58	        10158	        22	            1	        1491.38
##44	        10144	        20	            1	        1128.20


max_quantity = total_quantity_per_order['total_quantity'].max()
max_order = total_quantity_per_order[total_quantity_per_order['total_quantity'] == max_quantity]
max_order

min_quantity = total_quantity_per_order['total_quantity'].min()
min_order = total_quantity_per_order[total_quantity_per_order['total_quantity'] == min_quantity]
min_order

mean_quantity = total_quantity_per_order['total_quantity'].mean()
print(mean_quantity)
## 324.1505016722408

# Calcul des montants moyens et max par commandes et des moyennes pondérées par commandes :

# Création de la colonne 'weighted_amount_orderline'
df_orderdetails['weighted_amount_orderline'] = df_orderdetails['amount_orderline'] * df_orderdetails['quantityOrdered']

# Fonction pour calculer la moyenne pondérée
def weighted_mean(x):
    return (x['amount_orderline'] * x['quantityOrdered']).sum() / x['quantityOrdered'].sum()

# Calcul de la moyenne pondérée par commande
weighted_means = df_orderdetails.groupby('orderNumber').apply(weighted_mean).reset_index(name='weighted_mean')

# Fusionner avec le DataFrame original
df = pd.merge(df_orderdetails, weighted_means, on='orderNumber', how='left')

# Création de la table pivot
pivot_table = df.pivot_table(
    values=['amount_orderline', 'weighted_amount_orderline', 'quantityOrdered', 'weighted_mean'],
    index='orderNumber',
    aggfunc={
        'amount_orderline': ['max', 'mean'],
        'weighted_amount_orderline': lambda x: x.sum(),
        'quantityOrdered': 'sum',
        'weighted_mean': 'mean'
    }
)

pivot_table

##                 amount_orderline	            quantityOrdered	    weighted_amount_orderline	weighted_mean
##                 max	        mean	        sum	                <lambda>	                mean
## orderNumber					
## 10100	       4080.00	    2555.957500	    151	                381378.93	                2525.688278
## 10101	       4343.56	    2637.252500	    142	                340187.91	                2395.689507
## 10102	       3726.45	    2747.390000	    80	                217833.08	                2722.913500
## 10103	       5571.80	    3138.684375	    541	                1741733.67	                3219.470739
## 10104	       4566.99	    3092.784615	    443	                1353949.62	                3056.319684
## ...	           ...	        ...	            ...	                ...	                        ...
## 10394	       4623.15	    2586.105714	    239	                616403.66	                2579.094812
## 10395	       6788.76	    4482.022500	    156	                677774.95	                4344.711218
## 10396	       5138.76	    3461.942500	    287	                1031738.06	                3594.906132
## 10397	       4135.20	    2486.464000	    172	                465348.48	                2705.514419
## 10398	       4328.81	    2592.052222	    629	                1700913.98	                2704.155771

#Exemple pour la commande n°10100 : 
result_2 = df_orderdetails[df_orderdetails['orderNumber'] == 10100][['amount_orderline', 'quantityOrdered']]
print(result_2)
##   amount_orderline  quantityOrdered
##0           4080.00               30
##1           2754.50               50
##2           1660.12               22
##3           1729.21               49

# weighted_amount_orderline = 4080*30 + 2754.50*50 + 1660.12*22 + 1729.21*49 = 381 378.93
# weighted_mean = 381 378.93 / (30 + 50 +22 + 49) = 381 378.93 / 151 = 2 525.688278

## Lorsque weighted_mean < mean_amount_orderline, le client commande plus de quantités de produits de tarifs
## inférieurs. Si c'est l'inverse, le client commande plus de quantités de tarifs supérieurs.




