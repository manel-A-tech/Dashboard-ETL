import pandas as pd
import sqlalchemy

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
    CHARGE les données transformées et la dimension Date dans SQL Server.
    """
    print("\n--- 3. CHARGEMENT (LOAD VERS SQL SERVER) ---")

    table_name = "DWH_Global_Analysis"
    dim_date_table = "DIM_Date"

    try:
        engine = get_sql_engine()

        print(f"-> Connexion au serveur : .\\SQLEXPRESS")
        print(f"-> Base de données : Northwind")
        
        # 1. Charger la table de faits principale
        print(f"   -> Écriture dans la table : {table_name}")
        df_to_load = df.copy()
        
        # Retirer la dimension Date des attributs avant le chargement
        if 'dim_date' in df.attrs:
            dim_date = df.attrs['dim_date']
        else:
            dim_date = None
            
        df_to_load.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"SUCCÈS : {len(df_to_load)} lignes insérées dans '{table_name}'.")
        
        # 2. Charger la dimension Date si elle existe
        if dim_date is not None:
            print(f"\n   -> Écriture de la dimension Date : {dim_date_table}")
            dim_date.to_sql(dim_date_table, engine, if_exists='replace', index=False)
            print(f"SUCCÈS : {len(dim_date)} jours insérés dans '{dim_date_table}'.")
            
            # Vérification de la dimension Date
            try:
                with engine.begin() as conn:
                    result = pd.read_sql(f"SELECT COUNT(*) as count FROM {dim_date_table}", conn)
                    count = result['count'].iloc[0]
                    print(f"VÉRIFICATION : {count} jours dans '{dim_date_table}'")
            except Exception as verify_error:
                print(f"Vérification de {dim_date_table} échouée : {verify_error}")
        
        # 3. Vérification de la table principale
        try:
            with engine.begin() as conn:
                result = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn)
                count = result['count'].iloc[0]
                print(f"VÉRIFICATION : {count} lignes dans '{table_name}'")
        except Exception as verify_error:
            print(f"Vérification échouée : {verify_error}")
        
        print("\nCHARGEMENT COMPLET TERMINÉ")
        return True

    except Exception as e:
        print(f" Erreur lors de l'écriture SQL : {e}")
        return False 