import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

# Définir le répertoire contenant les fichiers
directory = 'C:\\Users\\Utilisateur\\Documents\\Formation_Data\\Projets\\Projet_1_toys_and_models\\Datasets_csv\\vs_code\\df_cleaned_csv'

# Liste des noms de fichiers
file_names = ['df_customers_cleaned.csv', 'df_employees_cleaned.csv', 'df_offices_cleaned.csv',\
              'df_orderdetails_cleaned.csv', 'df_orders_cleaned.csv', 'df_payments_cleaned.csv',\
                'df_productlines.csv', 'df_products.csv']

# Dictionnaire pour stocker les DataFrames
dataframes = {}

# Itérer sur chaque fichier
for file_name in file_names:
    file_path = os.path.join(directory, file_name)
    
    # Vérifier si le fichier existe
    if os.path.exists(file_path):
        print(f"Le fichier {file_name} existe.")
        df = pd.read_csv(file_path)
        dataframes[file_name] = df  # Stocker le DataFrame dans le dictionnaire
        print(df.head())  # Afficher les premières lignes pour vérifier
    else:
        print(f"Le fichier {file_name} n'existe pas dans le répertoire {directory}.")

# Accéder aux DataFrames
customers_df = dataframes.get('df_customers_cleaned.csv') # récupère la clé du dico
employees_df = dataframes.get('df_employees_cleaned.csv')
orders_df = dataframes.get('df_offices_cleaned.csv')
orderdetails_df = dataframes.get('df_orderdetails_cleaned.csv')
orders_df = dataframes.get('df_orders_cleaned.csv')
payments_df = dataframes.get('df_payments_cleaned.csv')
productlines_df = dataframes.get('df_productlines.csv')
products_df = dataframes.get('df_products.csv')

#_______________________________________________________________________________________________________________
# GRAPHE SIMPLE

# Créer un graphe
G = nx.Graph()

# Ajouter les nœuds (tables)
tables = ['customers', 'employees', 'offices', 'orderdetails', 'orders', 'payments', 'productlines', 'products']
G.add_nodes_from(tables)

# Ajouter les arêtes (relations)
edges = [
    ('customers', 'orders'),        # customers -> orders
    ('customers', 'employees'),     # customers -> employees
    ('employees', 'offices'),       # employees -> offices
    ('orders', 'orderdetails'),     # orders -> orderdetails
    ('orders', 'customers'),        # orders -> customers
    ('orderdetails', 'products'),   # orderdetails -> products
    ('products', 'productlines'),   # products -> productlines
    ('customers', 'payments'),      # customers -> payments
]
G.add_edges_from(edges)

# Dessiner le graphe
plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G) # calcule les positions des noeuds du graphe 'G'
nx.draw(G, pos, with_labels=True, node_color='skyblue', node_size=2000, edge_color='gray', font_size=15, font_weight='bold')
plt.title('Relations entre les tables')
plt.show()

# nx.draw(G, pos, with_labels=True, ...) : dessine le graphe G en 
# utilisant les positions des nœuds calculées précédemment (pos).

#_______________________________________________________________________________________________________________
# GRAPHE DETAILLE

import pandas as pd
from graphviz import Digraph

# Spécification du répertoire où enregistrer le fichier de sortie du diagramme
directory = 'C:\\Users\\Utilisateur\\Documents\\Formation_Data\\Projets\\Projet_1_toys_and_models\\Datasets_csv\\vs_code\\requests'

# Liste des noms de fichiers et leurs relations
# 'fields' : liste des colonnes de la table
# 'primary_key' : clé primaire de la table, s'il y en a
# 'foreign_key' : clé étrangère de la table, s'il y en a 
tables_detailed = {
    'customers': {
        'fields': ['customerNumber', 'customerName', 'contactLastName', 'contactFirstName',
                   'phone', 'addressLine1', 'addressLine2', 'city', 'state', 'postalCode',
                   'country', 'salesRepEmployeeNumber', 'creditLimit'],
        'primary_key': 'customerNumber',
        'foreign_key': 'salesRepEmployeeNumber'
    },
    'employees': {
        'fields': ['employeeNumber', 'lastName', 'firstName', 'extension', 'email',
                   'officeCode', 'reportsTo', 'jobTitle'],
        'primary_key': 'employeeNumber',
        'foreign_key': 'officeCode'
    },
    'offices': {
        'fields': ['officeCode', 'city', 'phone', 'addressLine1', 'addressLine2',
                   'state', 'country', 'postalCode', 'territory'],
        'primary_key': 'officeCode',
        'foreign_key': None
    },
    'orders': {
        'fields': ['orderNumber', 'orderDate', 'requiredDate', 'shippedDate',
                   'status', 'comments', 'customerNumber'],
        'primary_key': 'orderNumber',
        'foreign_key': 'customerNumber'
    },
    'orderdetails': {
        'fields': ['orderNumber', 'productCode', 'quantityOrdered', 'priceEach',
                   'orderLineNumber'],
        'primary_key': None,
        'foreign_key': ('orderNumber', 'productCode')
    },
    'payments': {
        'fields': ['customerNumber', 'checkNumber', 'paymentDate', 'amount'],
        'primary_key': None,
        'foreign_key': ('customerNumber')
    },
    'productlines': {
        'fields': ['productLine', 'textDescription', 'htmlDescription', 'image'],
        'primary_key': 'productLine',
        'foreign_key' : None
    },
    'products': {
        'fields': ['productCode', 'productName', 'productLine', 'productScale',
                   'productVendor', 'productDescription', 'quantityInStock',
                   'buyPrice', 'MSRP'],
        'primary_key': 'productCode',
        'foreign_key': 'productLine'
    }
}

# Visualisation des relations entre les tables
def visualize_tables():
    dot = Digraph()

    # Ajouter des nœuds pour chaque table avec ses champs
    # La fonction crée un graphe et ajoute des noeuds pour chaque table. Elle
    # construit les étiquettes (labels) des noeuds avec les informations des 
    # clés primaires et étrangères.
    for table, data in tables_detailed.items():
        fields = "\n".join(data['fields'])
        if data.get('primary_key') and data.get('foreign_key'):
            label = f"{table}\nPK: {data['primary_key']}\nFK: {data.get('foreign_key', '')}\n{fields}"
        elif data.get('primary_key'):
            label = f"{table}\nPK: {data['primary_key']}\n{fields}"
        elif data.get('foreign_key'):
            label = f"{table}\nFK: {data['foreign_key']}\n{fields}"
        else:
            label = f"{table}\n{fields}"
        dot.node(table, label=label)

    # Ajouter des arêtes pour les relations entre les tables
    relations = [
        ('customers', 'orders', 'customerNumber'),
        ('customers', 'payments', 'customerNumber'),
        ('customers', 'employees', 'salesRepEmployeeNumber'), 
        ('orders', 'orderdetails', 'orderNumber'),
        ('orderdetails', 'products', 'productCode'),
        ('products', 'productlines', 'productLine'),
        ('employees', 'offices', 'officeCode'),
        ('employees', 'employees', 'reportsTo')  # auto-référence pour le management
    ]

    # Les relations sont spécifiées comme des tuples contenant la table source,
    # la table cible et la clé étrangère utilisée pour la relation
    for source, target, key in relations:
        if isinstance(key, tuple):
            label = ', '.join(key)
        else:
            label = key
        dot.edge(source, target, label=label)

    return dot # retourne l'objet "Digraph" qui représente le graph

# Générer et enregistrer le graphique
dot = visualize_tables()
dot.render('tables_relations_détaillées', format='png', directory=directory, cleanup=True)
# Génère le fichier de sortie du diagramme (au format PNG dans ce cas) et 
# l'enregistre dans le répertoire spécifié
