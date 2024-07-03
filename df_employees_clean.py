from toys_and_model_python_mysql import df_employees

import numpy as np
import pandas as pd
import pprint

#**************************************************************************************************
# **DF_EMPLOYEES**
#**************************************************************************************************

df_employees

df_employees.info()

##<class 'pandas.core.frame.DataFrame'>
##RangeIndex: 23 entries, 0 to 22
##Data columns (total 8 columns):
 ##   Column          Non-Null Count  Dtype  
##---  ------          --------------  -----  
## 0   employeeNumber  23 non-null     int64  
## 1   lastName        23 non-null     object 
## 2   firstName       23 non-null     object 
## 3   extension       23 non-null     object 
## 4   email           23 non-null     object 
## 5   officeCode      23 non-null     object 
## 6   reportsTo       22 non-null     float64
## 7   jobTitle        23 non-null     object 
##dtypes: float64(1), int64(1), object(6)
##memory usage: 1.6+ KB

print(len(df_employees))
## 23

# Nombre de valeurs à 0 :
for column in df_employees.columns:
    if (df_employees[column] == 0).any():
        print(f"Column '{column}' contains zeros")
## /
## Aucune colonne ne contient de 0.

# Nombre de valeurs NaN :
for column in df_employees.columns:
    num_nans = df_employees[column].isna().sum()
    if num_nans > 0:
        print(f"Column '{column}' contains {num_nans} NaN values")
## Column 'reportsTo' contains 1 NaN values
## Il s'agit du Président.

print(len(df_employees['lastName'].unique().tolist()))
## 19
pprint.pprint(df_employees['lastName'].unique().tolist())
## ['Murphy',
## 'Patterson',
## 'Firrelli',
## 'Bondur',
## 'Bow',
## 'Jennings',
## 'Thompson',
## 'Tseng',
## 'Vanauf',
## 'Hernandez',
## 'Castillo',
## 'Bott',
## 'Jones',
## 'Fixter',
## 'Marsh',
## 'King',
## 'Nishi',
## 'Kato',
## 'Gerard']

pprint.pprint(df_employees['firstName'].unique().tolist())
## ['Diane',
##  'Mary',
##  'Jeff',
##  'William',
##  'Gerard',
##  'Anthony',
##  'Leslie',
##  'Julie',
##  'Steve',
##  'Foon Yue',
##  'George',
##  'Loui',
##  'Pamela',
##  'Larry',
##  'Barry',
##  'Andy',
##  'Peter',
##  'Tom',
##  'Mami',
##  'Yoshimi',
##  'Martin']
print(len(df_employees['firstName'].unique().tolist()))
## 21

pprint.pprint(df_employees['officeCode'].unique().tolist())
## ['1', '6', '4', '2', '3', '7', '5']


# Employés par localisation des bureaux :
employees_location = df_employees[['lastName', 'firstName', 'officeCode']]
print(employees_location)

df_offices_employees = employees_location.groupby('officeCode')\
[['lastName', 'firstName']].apply(lambda x: x)

df_offices_employees

## 	         lastName	     firstName
## officeCode			
## 1	0	 Murphy	         Diane
## 	    1    Patterson	     Mary
##      2    Firrelli	     Jeff
##      5    Bow	         Anthony
##      6    Jennings	     Leslie
## 	    7    Thompson	     Leslie
## 2	8	 Firrelli	     Julie
## 	    9    Patterson	     Steve
## 3	10	 Tseng	         Foon Yue
## 	    11   Vanauf	         George
## 4	4	 Bondur	         Gerard
## 	    12   Bondur	         Loui
## 	    13   Hernandez	     Gerard
## 	    14   Castillo	     Pamela
## 	    22   Gerard	         Martin
## 5	20	 Nishi	         Mami
## 	    21   Kato	         Yoshimi
## 6	3	 Patterson	     William
## 	    17   Fixter	         Andy
## 	    18   Marsh	         Peter
## 	    19   King	         Tom
## 7	15	 Bott	         Larry
## 	    16   Jones	         Barry

# Grouper par officeCode et compter le nombre d'employés dans chaque groupe
employees_by_officeCode = df_employees.groupby('officeCode').size().reset_index(name='count')
print(employees_by_officeCode)

##      officeCode  count
##  0          1      6
##  1          2      2
##  2          3      2
##  3          4      5
##  4          5      2
##  5          6      4
##  6          7      2

## Les bureaux qui emploient le plus de salariés sont les bureaux n°4 et 1.


# Pour voir si des employés sont présents dans plusieurs bureaux :

# Grouper par employé (par exemple, par lastName et firstName) et compter les officeCode uniques
employee_office_counts = df_employees.groupby(['lastName', 'firstName'])['officeCode'].nunique().reset_index()

# Filtrer pour trouver les employés avec plus d'un officeCode
employees_in_multiple_offices = employee_office_counts[employee_office_counts['officeCode'] > 1]

print(employees_in_multiple_offices)

## Empty DataFrame
## Columns: [lastName, firstName, officeCode]
## Index: []
## Il n'y a pas d'employés qui officient pour plusieurs bureaux.


# Fonctions des employés :
employees_positions = df_employees[['lastName', 'firstName', 'jobTitle']]
print(employees_positions)

##     lastName firstName              jobTitle
##0      Murphy     Diane             President
##1   Patterson      Mary              VP Sales
##2    Firrelli      Jeff          VP Marketing
##3   Patterson   William  Sales Manager (APAC)
##4      Bondur    Gerard   Sale Manager (EMEA)
##5         Bow   Anthony    Sales Manager (NA)
##6    Jennings    Leslie             Sales Rep
##7    Thompson    Leslie             Sales Rep
##8    Firrelli     Julie             Sales Rep
##9   Patterson     Steve             Sales Rep
##10      Tseng  Foon Yue             Sales Rep
##11     Vanauf    George             Sales Rep
##12     Bondur      Loui             Sales Rep
##13  Hernandez    Gerard             Sales Rep
##14   Castillo    Pamela             Sales Rep
##15       Bott     Larry             Sales Rep
##16      Jones     Barry             Sales Rep
##17     Fixter      Andy             Sales Rep
##18      Marsh     Peter             Sales Rep
##19       King       Tom             Sales Rep
##20      Nishi      Mami             Sales Rep
##21       Kato   Yoshimi             Sales Rep
##22     Gerard    Martin             Sales Rep

# Grouper par jobTitle et compter le nombre d'employés dans chaque groupe
employees_by_jobTitle = df_employees.groupby('jobTitle').size().reset_index(name='count')
print(employees_by_jobTitle)

##               jobTitle  count
## 0             President      1
## 1   Sale Manager (EMEA)      1
## 2  Sales Manager (APAC)      1
## 3    Sales Manager (NA)      1
## 4             Sales Rep     17
## 5          VP Marketing      1
## 6              VP Sales      1

