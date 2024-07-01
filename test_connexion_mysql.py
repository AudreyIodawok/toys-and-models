import pymysql

# Informations de connexion
host = 'localhost'
user = 'Audreyk'
password = '***' # (à remplacer)
database = 'toys_and_models'

# Connexion à MySQL
try:
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
    print("Connexion réussie à MySQL!")

    # Utilisez la connexion ici...

    # Fermez la connexion
    connection.close()

except pymysql.MySQLError as e:
    print(f"Erreur de connexion à MySQL: {e}")