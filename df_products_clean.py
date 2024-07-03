from toys_and_model_python_mysql import df_products

import numpy as np
import pandas as pd
import pprint

#**************************************************************************************************
# **DF_PRODUCTS**
#**************************************************************************************************

df_products

df_products.info()
##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 110 entries, 0 to 109
##Data columns (total 9 columns):
## #   Column              Non-Null Count  Dtype  
##---  ------              --------------  -----  
## 0   productCode         110 non-null    object 
## 1   productName         110 non-null    object 
## 2   productLine         110 non-null    object 
## 3   productScale        110 non-null    object 
## 4   productVendor       110 non-null    object 
## 5   productDescription  110 non-null    object 
## 6   quantityInStock     110 non-null    int64  
## 7   buyPrice            110 non-null    float64
## 8   MSRP                110 non-null    float64
##dtypes: float64(2), int64(1), object(6)
##memory usage: 7.9+ KB

print(len(df_products))
## 110

# Nombre de valeurs à 0 :
for column in df_products.columns:
    if (df_products[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## Pas de valeurs à 0

# Nombre de valeurs NaN :
for column in df_products.columns:
    num_nans = df_products[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
# Pas de colonne contenant des NaN


# Produits uniques

unique_products = df_products['productName'].unique().tolist()
unique_products
print(len(unique_products))
## 110
## Il n'y a pas de doublons de produits.

# Catégories de produits uniques

unique_categories = df_products['productLine'].unique().tolist()
unique_categories
## ['Motorcycles',
## 'Classic Cars',
## 'Trucks and Buses',
## 'Vintage Cars',
## 'Planes',
## 'Ships',
## 'Trains']
print(len(unique_categories))
## 7

# Fonction pour trouver les produits les plus et les moins chers et leurs stocks respectifs
# par productLine :

def find_extreme_products(group):
    max_product = group.loc[group['buyPrice'].idxmax()]
    min_product = group.loc[group['buyPrice'].idxmin()]
    return pd.Series({
        'most_expensive_product': max_product['productName'],
        'most_expensive_price': max_product['buyPrice'],
        'stock_expensive': max_product['quantityInStock'],
        'cheapest_product': min_product['productName'],
        'cheapest_price': min_product['buyPrice'],
        'stock_cheap': min_product['quantityInStock']
    })

# Grouper par productLine et appliquer la fonction
df_extreme_products = df_products.groupby('productLine').apply(find_extreme_products).reset_index()

# Afficher le résultat
df_extreme_products

##productLine	    most_expensive_product	  most_expensive_price	stock_expensive	    cheapest_product	                cheapest_price	stock_cheap
##0	Classic Cars	1962 LanciaA Delta 16V	                103.42	            679	    1958 Chevy Corvette Limited         15.91           254
##                                                                                      Edition
		        
##1	Motorcycles	    2003 Harley-Davidson                     91.02              558	    1982 Ducati 996 R                   24.14           924
##                  Eagle Drag Bike	   
                 	                        		
##2	Planes	        1980s Black Hawk Helicopter	             77.27	            533	    Corsair F4U ( Bird Cage)	        29.34	        681

##3	Ships	        18th century schooner	                 82.34	            190	    Pont Yacht	                        33.30	        41

##4	Trains	        Collectable Wooden Train	             67.56	            645	    1950's Chicago Surface Lines        26.72           860
##                                                                                      Streetcar	
	
##5	Trucks and      1940s Ford truck	                     84.76	            313	    1926 Ford Fire Engine	            24.92	        202
##  Buses
	
##6	Vintage Cars	1917 Grand Touring Sedan	             86.70	            272	    1938 Cadillac V-16 Presidential     20.61	        285
##                  Limousine	


# Produits dont le stock est le plus bas et le plus haut par catégorie :

def find_extreme_stock(group):
    max_stock = group.loc[group['quantityInStock'].idxmax()]
    min_stock = group.loc[group['quantityInStock'].idxmin()]
    return pd.Series({
        'product_w_most_stock': max_stock['productName'],
        'max_stock_product_price': max_stock['buyPrice'],
        'product_stock_max': max_stock['quantityInStock'],
        'product_w_min_stock': min_stock['productName'],
        'min_stock_product_price': min_stock['buyPrice'],
        'product_stock_min': min_stock['quantityInStock']
    })

# Grouper par productLine et appliquer la fonction
df_extreme_stock = df_products.groupby('productLine').apply(find_extreme_stock).reset_index()

# Afficher le résultat
df_extreme_stock


##  productLine	    product_w_most_stock	max_stock_product_price	    product_stock_max	    product_w_min_stock	    min_stock_product_price	    product_stock_min
##0	Classic Cars	1995 Honda Civic	    93.89	                    977	                    1968 Ford Mustang	    95.34	                    7

##1	Motorcycles	    2002 Suzuki XREO	    66.27	                    1000	                1960 BSA Gold Star      37.32	                    2
##                                                                                              DBD34	

##2	Planes	        America West Airlines 	68.80	                    965	                    F/A 18 Hornet 1/72	    54.40	                    55
##                  B757-200

##3	Ships	        The USS Constitution    33.97	                    708	                    Pont Yacht	            33.30	                    41
##                  Ship	

##4	Trains	        1950's Chicago Surface  26.72                       860	                    1962 City of Detroit    37.49	                    165 
##                  Lines Streetcar	                                                            Streetcar	               	

##5	Trucks and      1964 Mercedes Tour Bus	74.86	                    826	                    1996 Peterbilt 379      33.61	                    81
##  Buses                                                                                       Stake Bed with Outrigger	

##6	Vintage Cars	1932 Model A Ford       58.48	                    935	                    1928 Ford Phaeton 	    33.02	                    14
##                  J-Coupe	                                                                    Deluxe


# Quantités moyennes par catégories et tarifs moyens :

# Création du pivot table
pivot_table = pd.pivot_table(
    df_products, 
    values=['quantityInStock', 'buyPrice'], 
    index='productLine', 
    aggfunc={
        'quantityInStock': ['mean', 'max', 'min'],
        'buyPrice': 'mean'
    }
).reset_index()

# Aplatir les MultiIndex columns
pivot_table.columns = ['_'.join(col).strip() for col in pivot_table.columns.values]

# Arrondir les moyennes à 2 décimales
pivot_table = pivot_table.round(2)

# Renommer les colonnes pour plus de clarté
pivot_table = pivot_table.rename(columns={
    'quantityInStock_mean': 'average_quantityInStock',
    'buyPrice_mean': 'average_buyPrice',
    'quantityInStock_max': 'stock_max',
    'quantityInStock_min': 'stock_min'
})

# Afficher le résultat
pivot_table

##productLine_	    average_buyPrice	stock_max	average_quantityInStock	stock_min
##0	Classic Cars	64.45	            977	        576.84	                7

##1	Motorcycles	    50.69	            1000	    533.92	                2

##2	Planes	        49.63	            965	        519.08	                55

##3	Ships	        47.01	            708	        298.22	                41

##4	Trains	        43.92	            860	        556.67	                165

##5	Trucks and      56.33	            826	        326.00	                81
##  Buses	

##6	Vintage Cars	46.07	            935	        520.29	                14





