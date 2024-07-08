from fonctions import create_df_dict, create_dataframe, clean_and_complete_df, complete_df, save_csv_file,\
export_df_to_mysql, original_engine, new_engine


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
    
    print(f"Creating DataFrame for table: {table_name}")
    dataframes = create_df_dict(tables, original_engine) # créer le dico dataframes
    print(f"DataFrames dictionary created: {dataframes}")
    df = create_dataframe(dataframes, f'df_{table_name}') # créer les dataframes
    
    # si le df existe, on peut lui appliquer les fonctions 'clean', 'complete' (optionnelles) 
    # et enfin 'save'.
    if df is not None:
        print(f"DataFrame {table_name} created successfully")
        if clean_func:
            print(f"Cleaning DataFrame for table: {table_name}")
            df = clean_func(df)
            print(f"DataFrame {table_name} cleaned")
        if complete_func:
            print(f"Completing DataFrame for table: {table_name}")
            df = complete_func(df)
            print(f"DataFrame {table_name} completed")
        
        if df is not None:
            print(f"Saving DataFrame for table: {table_name}")
            print(df.head())
            save_csv_file(df, save_path, file_name)
            return df
        else:
            print(f"DataFrame for table {table_name} could not be processed.")
    else:
        print(f"DataFrame for table {table_name} could not be created.")
    return None

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
            'file_name': 'df_productlines_cleaned.csv',
            'clean_func' : None,
            'complete_func' : None
        },
        'products': {
            'file_name': 'df_products_cleaned.csv',
            'clean_func' : None,
            'complete_func' : None
        }
    }
    
    # Chemin où enregistrer les fichiers .csv nettoyés
    save_path = r'C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv'
    
    processed_dataframes = {}

    # Traiter et enregistrer chaque table
    for table_name, info in tables_info.items():
        print(f"Processing table: {table_name}")
        df = process_and_save_table(
                table_name,
                save_path,
                info['file_name'],
                clean_func=info.get('clean_func'),
                complete_func=info.get('complete_func')
            )

        if df is not None:
            processed_dataframes[f'df_{table_name}'] = df
            print(f"Processed DataFrame added for table: {table_name}")
        else:
            print(f"No DataFrame returned for table: {table_name}")

    if processed_dataframes:
        print("Exporting DataFrames to MySQL")
        export_df_to_mysql(processed_dataframes, new_engine)
    else:
        print("No DataFrames were processed")

if __name__ == "__main__":
    main()


#__________________________________________________________________________________________________________________________________
# Processing table: customers
# Creating DataFrame for table: customers
# DataFrame df_customers created
# DataFrames dictionary created: {'df_customers':      customerNumber                    customerName contactLastName  \
# 0               103               Atelier graphique         Schmitt   
# 1               112              Signal Gift Stores            King   
# 2               114      Australian Collectors, Co.        Ferguson   
# 3               119               La Rochelle Gifts         Labrune   
# 4               121              Baane Mini Imports      Bergulfsen   
# ..              ...                             ...             ...   
# 117             486    Motor Mint Distributors Inc.         Salazar   
# 118             487        Signal Collectibles Ltd.          Taylor   
# 119             489  Double Decker Gift Stores, Ltd           Smith   
# 120             495            Diecast Collectables          Franco   
# 121             496               Kelly's Gift Shop         Snowden   

#     contactFirstName           phone                  addressLine1  \
# 0            Carine       40.32.2555                54, rue Royale   
# 1               Jean      7025551838               8489 Strong St.   
# 2              Peter    03 9520 4555             636 St Kilda Road   
# 3            Janine       40.67.8555  67, rue des Cinquante Otages   
# 4             Jonas       07-98 9555        Erling Skakkes gate 78   
# ..               ...             ...                           ...   
# 117             Rosa      2155559857             11328 Douglas Av.   
# 118              Sue      4155554312             2793 Furth Circle   
# 119          Thomas   (171) 555-7555               120 Hanover Sq.   
# 120          Valarie      6175552555                6251 Ingle Ln.   
# 121             Tony   +64 9 5555500            Arenales 1938 3'A'   

#     addressLine2          city     state postalCode      country  \
# 0           None        Nantes      None      44000       France   
# 1           None     Las Vegas        NV      83030          USA   
# 2        Level 3     Melbourne  Victoria       3004    Australia   
# 3           None        Nantes      None      44000       France   
# 4           None       Stavern      None       4110       Norway   
# ..           ...           ...       ...        ...          ...   
# 117         None  Philadelphia        PA      71270          USA   
# 118         None      Brisbane        CA      94217          USA   
# 119         None        London      None    WA1 1DP           UK   
# 120         None        Boston        MA      51003          USA   
# 121         None    Auckland        None       None  New Zealand   

#      salesRepEmployeeNumber  creditLimit  
# 0                    1370.0      21000.0  
# 1                    1166.0      71800.0  
# 2                    1611.0     117300.0  
# 3                    1370.0     118200.0  
# 4                    1504.0      81700.0  
# ..                      ...          ...  
# 117                  1323.0      72600.0  
# 118                  1165.0      60300.0  
# 119                  1501.0      43300.0  
# 120                  1188.0      85100.0  
# 121                  1612.0     110000.0  

# [122 rows x 13 columns]}
# DataFrame customers created successfully
# Cleaning DataFrame for table: customers
# DataFrame customers cleaned
# Saving DataFrame for table: customers
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
# Processed DataFrame added for table: customers
# Processing table: offices
# Creating DataFrame for table: offices
# DataFrame df_offices created
# DataFrames dictionary created: {'df_offices':   officeCode           city             phone              addressLine1  \
# 0          1  San Francisco   +1 650 219 4782         100 Market Street   
# 1          2         Boston   +1 215 837 0825          1550 Court Place   
# 2          3            NYC   +1 212 555 3000      523 East 53rd Street   
# 3          4          Paris   +33 14 723 4404  43 Rue Jouffroy D'abbans   
# 4          5          Tokyo   +81 33 224 5000               4-1 Kioicho   
# 5          6         Sydney   +61 2 9264 2451     5-11 Wentworth Avenue   
# 6          7         London  +44 20 7877 2041       25 Old Broad Street   

#   addressLine2       state    country postalCode territory  
# 0    Suite 300          CA        USA      94080        NA  
# 1    Suite 102          MA        USA      02107        NA  
# 2      apt. 5A          NY        USA      10022        NA  
# 3         None        None     France      75017      EMEA  
# 4         None  Chiyoda-Ku      Japan   102-8578     Japan  
# 5     Floor #2        None  Australia   NSW 2010      APAC  
# 6      Level 7        None         UK   EC2N 1HN      EMEA  }
# DataFrame offices created successfully
# Completing DataFrame for table: offices
# DataFrame offices completed
# Saving DataFrame for table: offices
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
# Processed DataFrame added for table: offices
# Processing table: employees
# Creating DataFrame for table: employees
# DataFrame df_employees created
# DataFrames dictionary created: {'df_employees':     employeeNumber   lastName firstName extension  \
# 0             1002     Murphy     Diane     x5800   
# 1             1056  Patterson      Mary     x4611   
# 2             1076   Firrelli      Jeff     x9273   
# 3             1088  Patterson   William     x4871   
# 4             1102     Bondur    Gerard     x5408   
# 5             1143        Bow   Anthony     x5428   
# 6             1165   Jennings    Leslie     x3291   
# 7             1166   Thompson    Leslie     x4065   
# 8             1188   Firrelli     Julie     x2173   
# 9             1216  Patterson     Steve     x4334   
# 10            1286      Tseng  Foon Yue     x2248   
# 11            1323     Vanauf    George     x4102   
# 12            1337     Bondur      Loui     x6493   
# 13            1370  Hernandez    Gerard     x2028   
# 14            1401   Castillo    Pamela     x2759   
# 15            1501       Bott     Larry     x2311   
# 16            1504      Jones     Barry      x102   
# 17            1611     Fixter      Andy      x101   
# 18            1612      Marsh     Peter      x102   
# 19            1619       King       Tom      x103   
# 20            1621      Nishi      Mami      x101   
# 21            1625       Kato   Yoshimi      x102   
# 22            1702     Gerard    Martin     x2312   

#                               email officeCode  reportsTo  \
# 0      dmurphy@classicmodelcars.com          1        NaN   
# 1    mpatterso@classicmodelcars.com          1     1002.0   
# 2    jfirrelli@classicmodelcars.com          1     1002.0   
# 3   wpatterson@classicmodelcars.com          6     1056.0   
# 4      gbondur@classicmodelcars.com          4     1056.0   
# 5         abow@classicmodelcars.com          1     1056.0   
# 6    ljennings@classicmodelcars.com          1     1143.0   
# 7    lthompson@classicmodelcars.com          1     1143.0   
# 8    jfirrelli@classicmodelcars.com          2     1143.0   
# 9   spatterson@classicmodelcars.com          2     1143.0   
# 10      ftseng@classicmodelcars.com          3     1143.0   
# 11     gvanauf@classicmodelcars.com          3     1143.0   
# 12     lbondur@classicmodelcars.com          4     1102.0   
# 13   ghernande@classicmodelcars.com          4     1102.0   
# 14   pcastillo@classicmodelcars.com          4     1102.0   
# 15       lbott@classicmodelcars.com          7     1102.0   
# 16      bjones@classicmodelcars.com          7     1102.0   
# 17     afixter@classicmodelcars.com          6     1088.0   
# 18      pmarsh@classicmodelcars.com          6     1088.0   
# 19       tking@classicmodelcars.com          6     1088.0   
# 20      mnishi@classicmodelcars.com          5     1056.0   
# 21       ykato@classicmodelcars.com          5     1621.0   
# 22     mgerard@classicmodelcars.com          4     1102.0   

#                 jobTitle  
# 0              President  
# 1               VP Sales  
# 2           VP Marketing  
# 3   Sales Manager (APAC)  
# 4    Sale Manager (EMEA)  
# 5     Sales Manager (NA)  
# 6              Sales Rep  
# 7              Sales Rep  
# 8              Sales Rep  
# 9              Sales Rep  
# 10             Sales Rep  
# 11             Sales Rep  
# 12             Sales Rep  
# 13             Sales Rep  
# 14             Sales Rep  
# 15             Sales Rep  
# 16             Sales Rep  
# 17             Sales Rep  
# 18             Sales Rep  
# 19             Sales Rep  
# 20             Sales Rep  
# 21             Sales Rep  
# 22             Sales Rep  }
# DataFrame employees created successfully
# Saving DataFrame for table: employees
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
# Processed DataFrame added for table: employees
# Processing table: orderdetails
# Creating DataFrame for table: orderdetails
# DataFrame df_orderdetails created
# DataFrames dictionary created: {'df_orderdetails':       orderNumber productCode  quantityOrdered  priceEach  orderLineNumber
# 0           10100    S18_1749               30     136.00                3
# 1           10100    S18_2248               50      55.09                2
# 2           10100    S18_4409               22      75.46                4
# 3           10100    S24_3969               49      35.29                1
# 4           10101    S18_2325               25     108.06                4
# ...           ...         ...              ...        ...              ...
# 2781        10398   S700_2466               22      98.72                8
# 2782        10398   S700_2834               23     102.04                9
# 2783        10398   S700_3167               29      76.80               10
# 2784        10398   S700_4002               36      62.19               12
# 2785        10398    S72_1253               34      41.22                1

# [2786 rows x 5 columns]}
# DataFrame orderdetails created successfully
# Saving DataFrame for table: orderdetails
#    orderNumber productCode  quantityOrdered  priceEach  orderLineNumber
# 0        10100    S18_1749               30     136.00                3
# 1        10100    S18_2248               50      55.09                2
# 2        10100    S18_4409               22      75.46                4
# 3        10100    S24_3969               49      35.29                1
# 4        10101    S18_2325               25     108.06                4
# Le fichier 'df_orderdetails_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_orderdetails_cleaned.csv
# Processed DataFrame added for table: orderdetails
# Processing table: orders
# Creating DataFrame for table: orders
# DataFrame df_orders created
# DataFrames dictionary created: {'df_orders':      orderNumber  orderDate requiredDate shippedDate   status  \
# 0          10100 2021-07-23   2021-07-30  2021-07-27  Shipped   
# 1          10101 2021-07-26   2021-08-04  2021-07-28  Shipped   
# 2          10102 2021-07-27   2021-08-04  2021-07-31  Shipped   
# 3          10103 2021-08-15   2021-08-24  2021-08-19  Shipped   
# 4          10104 2021-08-17   2021-08-26  2021-08-18  Shipped   
# ..           ...        ...          ...         ...      ...   
# 294        10394 2023-09-30   2023-10-10  2023-10-04  Shipped   
# 295        10395 2023-10-02   2023-10-09  2023-10-08  Shipped   
# 296        10396 2023-10-08   2023-10-09  2023-10-10  Shipped   
# 297        10397 2023-10-10   2023-10-10  2023-10-10  Shipped   
# 298        10398 2023-10-10   2023-10-10  2023-10-10  Shipped   

#                                               comments  customerNumber  
# 0                                                 None             363  
# 1                               Check on availability.             128  
# 2                                                 None             181  
# 3                                                 None             121  
# 4                                                 None             141  
# ..                                                 ...             ...  
# 294                                               None             141  
# 295  We must be cautions with this customer. Their ...             250  
# 296                                               None             124  
# 297                                               None             242  
# 298                                               None             353  

# [299 rows x 7 columns]}
# DataFrame orders created successfully
# Saving DataFrame for table: orders
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
# Processed DataFrame added for table: orders
# Processing table: payments
# Creating DataFrame for table: payments
# DataFrame df_payments created
# DataFrames dictionary created: {'df_payments':      customerNumber checkNumber paymentDate    amount
# 0               103    HQ336336  2022-10-18   6066.78
# 1               103    JM555205  2021-06-03  14571.44
# 2               103    OM314933  2022-12-17   1676.14
# 3               112    BO864823  2022-12-16  14191.12
# 4               112     HQ55022  2021-06-04  32641.98
# ..              ...         ...         ...       ...
# 252             489    PO860906  2022-01-29   7310.42
# 253             495    BH167026  2021-12-24  59265.14
# 254             495    FN155234  2022-05-13   6276.60
# 255             496    MB342426  2021-07-14  32077.44
# 256             496     MN89921  2022-12-30  52166.00

# [257 rows x 4 columns]}
# DataFrame payments created successfully
# Saving DataFrame for table: payments
#    customerNumber checkNumber paymentDate    amount
# 0             103    HQ336336  2022-10-18   6066.78
# 1             103    JM555205  2021-06-03  14571.44
# 2             103    OM314933  2022-12-17   1676.14
# 3             112    BO864823  2022-12-16  14191.12
# 4             112     HQ55022  2021-06-04  32641.98
# Le fichier 'df_payments_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_payments_cleaned.csv
# Processed DataFrame added for table: payments
# Processing table: productlines
# Creating DataFrame for table: productlines
# DataFrame df_productlines created
# DataFrames dictionary created: {'df_productlines':         productLine                                    textDescription  \
# 0      Classic Cars  Attention car enthusiasts: Make your wildest c...   
# 1       Motorcycles  Our motorcycles are state of the art replicas ...   
# 2            Planes  Unique, diecast airplane and helicopter replic...   
# 3             Ships  The perfect holiday or anniversary gift for ex...   
# 4            Trains  Model trains are a rewarding hobby for enthusi...   
# 5  Trucks and Buses  The Truck and Bus models are realistic replica...   
# 6      Vintage Cars  Our Vintage Car models realistically portray a...   

#   htmlDescription image  
# 0            None  None  
# 1            None  None  
# 2            None  None  
# 3            None  None  
# 4            None  None  
# 5            None  None  
# 6            None  None  }
# DataFrame productlines created successfully
# Saving DataFrame for table: productlines
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
# Le fichier 'df_productlines_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_productlines_cleaned.csv
# Processed DataFrame added for table: productlines
# Processing table: products
# Creating DataFrame for table: products
# DataFrame df_products created
# DataFrames dictionary created: {'df_products':     productCode                            productName   productLine  \
# 0      S10_1678  1969 Harley Davidson Ultimate Chopper   Motorcycles   
# 1      S10_1949               1952 Alpine Renault 1300  Classic Cars   
# 2      S10_2016                  1996 Moto Guzzi 1100i   Motorcycles   
# 3      S10_4698   2003 Harley-Davidson Eagle Drag Bike   Motorcycles   
# 4      S10_4757                    1972 Alfa Romeo GTA  Classic Cars   
# ..          ...                                    ...           ...   
# 105   S700_3505                            The Titanic         Ships   
# 106   S700_3962                         The Queen Mary         Ships   
# 107   S700_4002              American Airlines: MD-11S        Planes   
# 108    S72_1253                       Boeing X-32A JSF        Planes   
# 109    S72_3212                             Pont Yacht         Ships   

#     productScale              productVendor  \
# 0           1:10            Min Lin Diecast   
# 1           1:10    Classic Metal Creations   
# 2           1:10   Highway 66 Mini Classics   
# 3           1:10          Red Start Diecast   
# 4           1:10    Motor City Art Classics   
# ..           ...                        ...   
# 105        1:700   Carousel DieCast Legends   
# 106        1:700  Welly Diecast Productions   
# 107        1:700        Second Gear Diecast   
# 108         1:72    Motor City Art Classics   
# 109         1:72       Unimax Art Galleries   

#                                     productDescription  quantityInStock  \
# 0    This replica features working kickstand, front...              793   
# 1    Turnable front wheels; steering function; deta...              731   
# 2    Official Moto Guzzi logos and insignias, saddl...              663   
# 3    Model features, official Harley Davidson logos...              558   
# 4    Features include: Turnable front wheels; steer...              325   
# ..                                                 ...              ...   
# 105  Completed model measures 19 1/2 inches long, 9...              196   
# 106  Exact replica. Wood and Metal. Many extras inc...              509   
# 107  Polished finish. Exact replia with official lo...              882   
# 108  10" Wingspan with retractable landing gears.Co...              486   
# 109  Measures 38 inches Long x 33 3/4 inches High. ...               41   

#      buyPrice    MSRP  
# 0       48.81   95.70  
# 1       98.58  214.30  
# 2       68.99  118.94  
# 3       91.02  193.66  
# 4       85.68  136.00  
# ..        ...     ...  
# 105     51.09  100.17  
# 106     53.63   99.31  
# 107     36.27   74.03  
# 108     32.77   49.66  
# 109     33.30   54.60  

# [110 rows x 9 columns]}
# DataFrame products created successfully
# Saving DataFrame for table: products
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
# Le fichier 'df_products_cleaned.csv' a été enregistré à l'emplacement : C:\Users\Utilisateur\Documents\Formation_Data\Projets\Projet_1_toys_and_models\Datasets_csv\vs_code\df_cleaned_csv\df_products_cleaned.csv
# Processed DataFrame added for table: products
# Exporting DataFrames to MySQL
# DataFrame df_customers exported to table customers in MySQL
# DataFrame df_offices exported to table offices in MySQL
# DataFrame df_employees exported to table employees in MySQL
# DataFrame df_orderdetails exported to table orderdetails in MySQL
# DataFrame df_orders exported to table orders in MySQL
# DataFrame df_payments exported to table payments in MySQL
# DataFrame df_productlines exported to table productlines in MySQL
# DataFrame df_products exported to table products in MySQL