from toys_and_model_python_mysql import df_orderdetails

import numpy as np
import pandas as pd
import pprint

#**************************************************************************************************
# **DF_ORDERDETAILS**
#**************************************************************************************************

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
df_orderdetails['total_order_amount'] = df_orderdetails['priceEach'] * df_orderdetails['quantityOrdered']

# Agrégation des données par numéro de commande
total_quantity_per_order = df_orderdetails.groupby('orderNumber').agg(
    total_quantity=('quantityOrdered', 'sum'), # somme des quantités commandées
    order_lines=('orderLineNumber', 'nunique'), # nombre de ligne de commande / commande
    amount_per_order=('total_order_amount', 'sum') # somme des montants de cde / cde
).reset_index()

# Tri des commandes par quantité totale par ordre décroissant
total_quantity_per_order = total_quantity_per_order.sort_values(by = 'total_quantity',\
                                                                ascending = False)
total_quantity_per_order

                ##orderNumber	total_quantity	order_lines	amount_per_order
##122	        10222	        717	            18	        56822.65
##6	            10106	        675	            18	        52151.81
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

