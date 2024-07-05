from fonctions import create_df_dict, create_dataframe, clean_and_complete_df, complete_df, save_csv_file
from toys_and_model_python_mysql import engine

def process_and_save_table(table_name, save_path, file_name, clean_func=None, complete_func=None):
    """
    Traite et enregistre une table spécifique.

    :param table_name: Le nom de la table à traiter
    :param save_path: Le chemin du dossier où enregistrer le fichier .csv
    :param file_name: Le nom du fichier .csv à enregistrer
    :param clean_func: La fonction de nettoyage à appliquer (si nécessaire)
    :param complete_func: La fonction de complétion à appliquer (si nécessaire)
    """
    tables = [table_name] # liste des tables à traiter
    
    dataframes = create_df_dict(tables, engine) # créer le dico dataframes
    df = create_dataframe(dataframes, f'df_{table_name}') # créer les dataframes
    
    # si le df existe, on peut lui appliquer les fonctions 'clean', 'complete' (optionnelles) 
    # et enfin 'save'.
    if df is not None:
        if clean_func:
            df = clean_func(df)
        if complete_func:
            df = complete_func(df)
        
        if df is not None:
            print(df.head())
            save_csv_file(df, save_path, file_name)
        else:
            print(f"DataFrame for table {table_name} could not be processed.")
    else:
        print(f"DataFrame for table {table_name} could not be created.")

def main():
    # Définir les tables à traiter et leurs noms de fichiers .csv respectifs
    tables_info = {
        'customers': {
            'file_name': 'df_customers_cleaned.csv',
            'clean_func': clean_and_complete_df,
            'complete_func': None
        },
        'offices': {
            'file_name': 'df_offices_cleaned.csv',
            'clean_func': None,
            'complete_func': complete_df
        },
        'employees': {
            'file_name': 'df_employees_cleaned.csv',
            'clean_func': None,
            'complete_func': None
        },
        'orderdetails': {
            'file_name': 'df_orderdetails_cleaned.csv',
            'clean_func': None,
            'complete_func': None
        },
        'orders': {
            'file_name': 'df_orders_cleaned.csv',
            'clean_func' : None,
            'complete_func': None
        },
        'payments': {
            'file_name': 'df_payments_cleaned.csv',
            'clean_func' : None,
            'complete_func': None
        },
        'productlines': {
            'file_name': 'df_productlines.csv',
            'clean_func' : None,
            'complete_func' : None
        },
        'products': {
            'file_name': 'df_products.csv',
            'clean_func' : None,
            'complete_func' : None
        }
    }
    
    # Chemin où enregistrer les fichiers .csv nettoyés
    save_path = r'C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv'
    
    # Traiter et enregistrer chaque table
    for table_name, info in tables_info.items():
        process_and_save_table(
            table_name,
            save_path,
            info['file_name'],
            clean_func=info.get('clean_func'),
            complete_func=info.get('complete_func')
        )

if __name__ == "__main__":
    main()


#__________________________________________________________________________________________________________________________________
# MySQL environment variables are correctly defined
# Tables in the database: ['customers', 'employees', 'offices', 'orderdetails', 'orders', 'payments', 'productlines', 'products']
#    customerNumber                customerName contactLastName  \
# 0             103           Atelier graphique         Schmitt   
# 1             112          Signal Gift Stores            King   
# 2             114  Australian Collectors, Co.        Ferguson   
# 3             119           La Rochelle Gifts         Labrune   
# 4             121          Baane Mini Imports      Bergulfsen   

#   contactFirstName         phone                  addressLine1 addressLine2  \
# 0          Carine     40.32.2555                54, rue Royale         None   
# 1             Jean    7025551838               8489 Strong St.         None   
# 2            Peter  03 9520 4555             636 St Kilda Road      Level 3   
# 3          Janine     40.67.8555  67, rue des Cinquante Otages         None   
# 4           Jonas     07-98 9555        Erling Skakkes gate 78         None   

#         city     state postalCode    country  salesRepEmployeeNumber  \
# 0     Nantes      None      44000     France                  1370.0   
# 1  Las Vegas        NV      83030        USA                  1166.0   
# 2  Melbourne  Victoria       3004  Australia                  1611.0   
# 3     Nantes      None      44000     France                  1370.0   
# 4    Stavern      None       4110     Norway                  1504.0   

#    creditLimit  
# 0      21000.0  
# 1      71800.0  
# 2     117300.0  
# 3     118200.0  
# 4      81700.0  
# DataFrame df_customers created
# DataFrame df_employees created
# DataFrame df_offices created
# DataFrame df_orderdetails created
# DataFrame df_orders created
# DataFrame df_payments created
# DataFrame df_productlines created
# DataFrame df_products created
#    employeeNumber   lastName firstName extension  \
# 0            1002     Murphy     Diane     x5800   
# 1            1056  Patterson      Mary     x4611   
# 2            1076   Firrelli      Jeff     x9273   
# 3            1088  Patterson   William     x4871   
# 4            1102     Bondur    Gerard     x5408   

#                             email officeCode   reportsTo              jobTitle  
# 0     dmurphy@classicmodelcars.com          1        NaN             President  
# 1   mpatterso@classicmodelcars.com          1     1002.0              VP Sales  
# 2   jfirrelli@classicmodelcars.com          1     1002.0          VP Marketing  
# 3  wpatterson@classicmodelcars.com          6     1056.0  Sales Manager (APAC)  
# 4     gbondur@classicmodelcars.com          4     1056.0   Sale Manager (EMEA)
 
# DataFrame df_customers created
# Variable df_customers created and available globally.
#    customerNumber                customerName contactLastName  \
#0             103           Atelier graphique         Schmitt   
#1             112          Signal Gift Stores            King   
#2             114  Australian Collectors, Co.        Ferguson   
#3             119           La Rochelle Gifts         Labrune   
#4             121          Baane Mini Imports      Bergulfsen   

#   contactFirstName         phone                  addressLine1 addressLine2  \
# 0           Carine    40.32.2555                54, rue Royale         None   
# 1             Jean    7025551838               8489 Strong St.         None   
# 2            Peter  03 9520 4555             636 St Kilda Road      Level 3   
# 3           Janine    40.67.8555  67, rue des Cinquante Otages         None   
# 4            Jonas    07-98 9555        Erling Skakkes gate 78         None   

#         city     state postalCode    country  salesRepEmployeeNumber  \
# 0     Nantes      None      44000     France                  1370.0   
# 1  Las Vegas        NV      83030        USA                  1166.0   
# 2  Melbourne  Victoria       3004  Australia                  1611.0   
# 3     Nantes      None      44000     France                  1370.0   
# 4    Stavern      None       4110     Norway                  1504.0   

#    creditLimit  
# 0      21000.0  
# 1      71800.0  
# 2     117300.0  
# 3     118200.0  
# 4      81700.0  

# DataFrame df_employees created
# Variable df_employees created and available globally.
#    employeeNumber   lastName firstName extension  \
# 0            1002     Murphy     Diane     x5800   
# 1            1056  Patterson      Mary     x4611   
# 2            1076   Firrelli      Jeff     x9273   
# 3            1088  Patterson   William     x4871   
# 4            1102     Bondur    Gerard     x5408   

#                              email officeCode  reportsTo              jobTitle  
# 0     dmurphy@classicmodelcars.com          1        NaN             President  
# 1   mpatterso@classicmodelcars.com          1     1002.0              VP Sales  
# 2   jfirrelli@classicmodelcars.com          1     1002.0          VP Marketing  
# 3  wpatterson@classicmodelcars.com          6     1056.0  Sales Manager (APAC)  
# 4     gbondur@classicmodelcars.com          4     1056.0   Sale Manager (EMEA) 
#  
# DataFrame df_offices created
# Variable df_offices created and available globally.
#   officeCode           city            phone              addressLine1  \
# 0          1  San Francisco  +1 650 219 4782         100 Market Street   
# 1          2         Boston  +1 215 837 0825          1550 Court Place   
# 2          3            NYC  +1 212 555 3000      523 East 53rd Street   
# 3          4          Paris  +33 14 723 4404  43 Rue Jouffroy D'abbans   
# 4          5          Tokyo  +81 33 224 5000               4-1 Kioicho   

#   addressLine2       state country postalCode territory  
# 0    Suite 300          CA     USA      94080        NA  
# 1    Suite 102          MA     USA      02107        NA  
# 2      apt. 5A          NY     USA      10022        NA  
# 3         None        None  France      75017      EMEA  
# 4         None  Chiyoda-Ku   Japan   102-8578     Japan  

# DataFrame df_orderdetails created
# Variable df_orderdetails created and available globally.
#    orderNumber productCode  quantityOrdered  priceEach  orderLineNumber
# 0        10100    S18_1749               30     136.00                3
# 1        10100    S18_2248               50      55.09                2
# 2        10100    S18_4409               22      75.46                4
# 3        10100    S24_3969               49      35.29                1
# 4        10101    S18_2325               25     108.06                4

# DataFrame df_orders created
# Variable df_orders created and available globally.
#    orderNumber  orderDate requiredDate shippedDate   status  \
# 0        10100 2021-07-23   2021-07-30  2021-07-27  Shipped   
# 1        10101 2021-07-26   2021-08-04  2021-07-28  Shipped   
# 2        10102 2021-07-27   2021-08-04  2021-07-31  Shipped   
# 3        10103 2021-08-15   2021-08-24  2021-08-19  Shipped   
# 4        10104 2021-08-17   2021-08-26  2021-08-18  Shipped   

#                  comments  customerNumber  
# 0                    None             363  
# 1  Check on availability.             128  
# 2                    None             181  
# 3                    None             121  
# 4                    None             141  

# DataFrame df_payments created
# Variable df_payments created and available globally.
#    customerNumber checkNumber paymentDate    amount
# 0             103    HQ336336  2022-10-18   6066.78
# 1             103    JM555205  2021-06-03  14571.44
# 2             103    OM314933  2022-12-17   1676.14
# 3             112    BO864823  2022-12-16  14191.12
# 4             112     HQ55022  2021-06-04  32641.98

# DataFrame df_productlines created
# Variable df_productlines created and available globally.
#     productLine                                    textDescription  \
# 0  Classic Cars  Attention car enthusiasts: Make your wildest c...   
# 1   Motorcycles  Our motorcycles are state of the art replicas ...   
# 2        Planes  Unique, diecast airplane and helicopter replic...   
# 3         Ships  The perfect holiday or anniversary gift for ex...   
# 4        Trains  Model trains are a rewarding hobby for enthusi...   

#   htmlDescription image  
# 0            None  None  
# 1            None  None  
# 2            None  None  
# 3            None  None  
# 4            None  None  

# DataFrame df_products created
# Variable df_products created and available globally.
#   productCode                            productName   productLine  \
# 0    S10_1678  1969 Harley Davidson Ultimate Chopper   Motorcycles   
# 1    S10_1949               1952 Alpine Renault 1300  Classic Cars   
# 2    S10_2016                  1996 Moto Guzzi 1100i   Motorcycles   
# 3    S10_4698   2003 Harley-Davidson Eagle Drag Bike   Motorcycles   
# 4    S10_4757                    1972 Alfa Romeo GTA  Classic Cars   

#   productScale             productVendor  \
# 0         1:10           Min Lin Diecast   
# 1         1:10   Classic Metal Creations   
# 2         1:10  Highway 66 Mini Classics   
# 3         1:10         Red Start Diecast   
# 4         1:10   Motor City Art Classics   

#                                   productDescription  quantityInStock  \
# 0  This replica features working kickstand, front...              793   
# 1  Turnable front wheels; steering function; deta...              731   
# 2  Official Moto Guzzi logos and insignias, saddl...              663   
# 3  Model features, official Harley Davidson logos...              558   
# 4  Features include: Turnable front wheels; steer...              325   

#    buyPrice    MSRP  
# 0     48.81   95.70  
# 1     98.58  214.30  
# 2     68.99  118.94  
# 3     91.02  193.66  
# 4     85.68  136.00  

# DataFrame df_customers created
#    customerNumber                customerName contactLastName  \
# 0             103           Atelier graphique         Schmitt   
# 1             112          Signal Gift Stores            King   
# 2             114  Australian Collectors, Co.        Ferguson   
# 3             119           La Rochelle Gifts         Labrune   
# 4             121          Baane Mini Imports      Bergulfsen   

#   contactFirstName         phone                  addressLine1 addressLine2  \
# 0          Carine     40.32.2555                54, rue Royale         None   
# 1             Jean    7025551838               8489 Strong St.         None   
# 2            Peter  03 9520 4555             636 St Kilda Road      Level 3   
# 3          Janine     40.67.8555  67, rue des Cinquante Otages         None   
# 4           Jonas     07-98 9555        Erling Skakkes gate 78         None   

#         city     state postalCode    country  salesRepEmployeeNumber  \
# 0     Nantes    France      44000     France                  1370.0   
# 1  Las Vegas        NV      83030        USA                  1166.0   
# 2  Melbourne  Victoria       3004  Australia                  1611.0   
# 3     Nantes    France      44000     France                  1370.0   
# 4    Stavern    Norway       4110     Norway                  1504.0   

#    creditLimit  
# 0      21000.0  
# 1      71800.0  
# 2     117300.0  
# 3     118200.0  
# 4      81700.0  
# Le fichier 'df_customers_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_customers_cleaned.csv

# DataFrame df_offices created
#   officeCode           city            phone              addressLine1  \
# 0          1  San Francisco  +1 650 219 4782         100 Market Street   
# 1          2         Boston  +1 215 837 0825          1550 Court Place   
# 2          3            NYC  +1 212 555 3000      523 East 53rd Street   
# 3          4          Paris  +33 14 723 4404  43 Rue Jouffroy D'abbans   
# 4          5          Tokyo  +81 33 224 5000               4-1 Kioicho   

#   addressLine2       state country postalCode territory  
# 0    Suite 300          CA     USA      94080        NA  
# 1    Suite 102          MA     USA      02107        NA  
# 2      apt. 5A          NY     USA      10022        NA  
# 3         None      France  France      75017      EMEA  
# 4         None  Chiyoda-Ku   Japan   102-8578     Japan  
# Le fichier 'df_offices_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_offices_cleaned.csv

# DataFrame df_employees created
#    employeeNumber   lastName firstName extension  \
# 0            1002     Murphy     Diane     x5800   
# 1            1056  Patterson      Mary     x4611   
# 2            1076   Firrelli      Jeff     x9273   
# 3            1088  Patterson   William     x4871   
# 4            1102     Bondur    Gerard     x5408   

#                              email officeCode  reportsTo              jobTitle  
# 0     dmurphy@classicmodelcars.com          1        NaN             President  
# 1   mpatterso@classicmodelcars.com          1     1002.0              VP Sales  
# 2   jfirrelli@classicmodelcars.com          1     1002.0          VP Marketing  
# 3  wpatterson@classicmodelcars.com          6     1056.0  Sales Manager (APAC)  
# 4     gbondur@classicmodelcars.com          4     1056.0   Sale Manager (EMEA)  
# Le fichier 'df_employees_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_employees_cleaned.csv

# DataFrame df_orderdetails created
#    orderNumber productCode  quantityOrdered  priceEach  orderLineNumber
# 0        10100    S18_1749               30     136.00                3
# 1        10100    S18_2248               50      55.09                2
# 2        10100    S18_4409               22      75.46                4
# 3        10100    S24_3969               49      35.29                1
# 4        10101    S18_2325               25     108.06                4
# Le fichier 'df_orderdetails_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_orderdetails_cleaned.csv

# DataFrame df_orders created
#    orderNumber  orderDate requiredDate shippedDate   status  \
# 0        10100 2021-07-23   2021-07-30  2021-07-27  Shipped   
# 1        10101 2021-07-26   2021-08-04  2021-07-28  Shipped   
# 2        10102 2021-07-27   2021-08-04  2021-07-31  Shipped   
# 3        10103 2021-08-15   2021-08-24  2021-08-19  Shipped   
# 4        10104 2021-08-17   2021-08-26  2021-08-18  Shipped   

#                  comments  customerNumber  
# 0                    None             363  
# 1  Check on availability.             128  
# 2                    None             181  
# 3                    None             121  
# 4                    None             141  
# Le fichier 'df_orders_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_orders_cleaned.csv

# DataFrame df_payments created
#    customerNumber checkNumber paymentDate    amount
# 0             103    HQ336336  2022-10-18   6066.78
# 1             103    JM555205  2021-06-03  14571.44
# 2             103    OM314933  2022-12-17   1676.14
# 3             112    BO864823  2022-12-16  14191.12
# 4             112     HQ55022  2021-06-04  32641.98
# Le fichier 'df_payments_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_payments_cleaned.csv

# DataFrame df_productlines created
#     productLine                                    textDescription  \
# 0  Classic Cars  Attention car enthusiasts: Make your wildest c...   
# 1   Motorcycles  Our motorcycles are state of the art replicas ...   
# 2        Planes  Unique, diecast airplane and helicopter replic...   
# 3         Ships  The perfect holiday or anniversary gift for ex...   
# 4        Trains  Model trains are a rewarding hobby for enthusi...   

#   htmlDescription image  
# 0            None  None  
# 1            None  None  
# 2            None  None  
# 3            None  None  
# 4            None  None  
# Le fichier 'df_productlines.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_productlines.csv

# DataFrame df_products created
#   productCode                            productName   productLine  \
# 0    S10_1678  1969 Harley Davidson Ultimate Chopper   Motorcycles   
# 1    S10_1949               1952 Alpine Renault 1300  Classic Cars   
# 2    S10_2016                  1996 Moto Guzzi 1100i   Motorcycles   
# 3    S10_4698   2003 Harley-Davidson Eagle Drag Bike   Motorcycles   
# 4    S10_4757                    1972 Alfa Romeo GTA  Classic Cars   

#   productScale             productVendor  \
# 0         1:10           Min Lin Diecast   
# 1         1:10   Classic Metal Creations   
# 2         1:10  Highway 66 Mini Classics   
# 3         1:10         Red Start Diecast   
# 4         1:10   Motor City Art Classics   

#                                   productDescription  quantityInStock  \
# 0  This replica features working kickstand, front...              793   
# 1  Turnable front wheels; steering function; deta...              731   
# 2  Official Moto Guzzi logos and insignias, saddl...              663   
# 3  Model features, official Harley Davidson logos...              558   
# 4  Features include: Turnable front wheels; steer...              325   

#    buyPrice    MSRP  
# 0     48.81   95.70  
# 1     98.58  214.30  
# 2     68.99  118.94  
# 3     91.02  193.66  
# 4     85.68  136.00  
# Le fichier 'df_products.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_products.csv