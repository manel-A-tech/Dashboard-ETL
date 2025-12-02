import pandas as pd
import sqlalchemy

#import absolu au lieu de relatif
try:
    from .ETLconfig import SQL_CONN_STRING
except ImportError:
    from ETLconfig import SQL_CONN_STRING


def get_sql_engine():
    """Crée le moteur SQL à partir de la configuration"""
    conn_str = f"mssql+pyodbc:///?odbc_connect={SQL_CONN_STRING.replace(' ', '%20')}"
    return sqlalchemy.create_engine(conn_str)


def load_data(df):
    """
    CHARGE les données transformées directement dans une table SQL Server.
    Cette table servira de Data Warehouse.
    """
    print("\n--- 3. CHARGEMENT (LOAD VERS SQL SERVER) ---")

    # Nom de la table finale dans SQL Server
    table_name = "DWH_Global_Analysis"

    try:
        engine = get_sql_engine()

        print(f"   -> Connexion au serveur : .\\SQLEXPRESS")
        print(f"   -> Base de données : Northwind")
        print(f"   -> Écriture dans la table : {table_name}")

        # if_exists='replace' : Supprime et recrée la table à chaque refresh
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"   ✅ SUCCÈS : {len(df)} lignes insérées dans la table '{table_name}'.")
        
        # VÉRIFICATION CORRIGÉE
        try:
            with engine.begin() as conn:
                result = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn)
                count = result['count'].iloc[0]
                print(f"   ✅ VÉRIFICATION : {count} lignes dans la table '{table_name}'")
        except Exception as verify_error:
            print(f"   ⚠️ Vérification échouée : {verify_error}")
        
        return True

    except Exception as e:
        print(f"   ❌ Erreur lors de l'écriture SQL : {e}")
        return False


# Version alternative sans vérification (plus simple)
def load_data_simple(df):
    """
    Version simplifiée sans vérification
    """
    print("\n--- 3. CHARGEMENT (LOAD VERS SQL SERVER) ---")

    table_name = "DWH_Global_Analysis"

    try:
        engine = get_sql_engine()

        print(f"   -> Connexion au serveur : .\\SQLEXPRESS")
        print(f"   -> Base de données : Northwind")
        print(f"   -> Écriture dans la table : {table_name}")

        # Chargement des données
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"   ✅ SUCCÈS : {len(df)} lignes insérées dans la table '{table_name}'.")
        return True

    except Exception as e:
        print(f"   ❌ Erreur lors de l'écriture SQL : {e}")
        return False


# Exemple d'utilisation
if __name__ == "__main__":
    # Test avec un DataFrame exemple
    sample_data = pd.DataFrame({
        'OrderID': [1, 2, 3],
        'OrderDate': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'ShipCity': ['Paris', 'Lyon', 'Marseille'],
        'Source': ['SQL_Server', 'Access', 'SQL_Server']
    })
    
    success = load_data_simple(sample_data)
    if success:
        print("Chargement réussi !")
    else:
        print("Échec du chargement.")