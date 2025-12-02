import pandas as pd
import sqlalchemy
import pyodbc
import os
import traceback

try:
    from .ETLconfig import ACCESS_CONN_STRING, SQL_CONN_STRING, ACCESS_DB_PATH
except ImportError:
    from ETLconfig import ACCESS_CONN_STRING, SQL_CONN_STRING, ACCESS_DB_PATH

# Fonctions de connexion
def get_sql_engine():
    """Connexion à SQL Server"""
    conn_str = f"mssql+pyodbc:///?odbc_connect={SQL_CONN_STRING.replace(' ', '%20')}"
    return sqlalchemy.create_engine(conn_str)

def get_access_connection():
    """Connexion directe à Access via pyodbc"""
    return pyodbc.connect(ACCESS_CONN_STRING)

def extract_data():
    print("\n--- 1. EXTRACTION DES DONNÉES ---")

    # ---------------------------------------------------------
    # PARTIE A : SQL SERVER (On transforme les IDs en NOMS)
    # ---------------------------------------------------------
    print("   -> Connexion à SQL Server...")
    try:
        engine = get_sql_engine()
        # REQUÊTE SQL SERVER : On joint Orders, Customers et Employees
        query_sql = """
        SELECT 
            o.OrderID,
            o.OrderDate,
            o.ShippedDate,
            o.ShipCity,
            o.ShipCountry,
            c.CompanyName,
            e.FirstName + ' ' + e.LastName as EmployeeName
        FROM Orders o
        LEFT JOIN Customers c ON o.CustomerID = c.CustomerID
        LEFT JOIN Employees e ON o.EmployeeID = e.EmployeeID
        """
        df_sql = pd.read_sql(query_sql, engine)
        df_sql['Source'] = 'SQL_Server'
        print(f"    SQL Server : {len(df_sql)} commandes récupérées.")

    except Exception as e:
        print(f"    Erreur SQL Server : {e}")
        df_sql = pd.DataFrame()

    # PARTIE B : ACCESS (Northwind 2012.accdb)
     
    print("   -> Connexion à Access...")
    print(f"   -> Chemin: {ACCESS_DB_PATH}")
    
    try:
        # Test d'existence du fichier
        if not os.path.exists(ACCESS_DB_PATH):
            print(f"    ❌ ERREUR: Fichier introuvable à {ACCESS_DB_PATH}")
            df_access = pd.DataFrame()
        else:
            print(f"    ✅ Fichier trouvé ({os.path.getsize(ACCESS_DB_PATH) / (1024*1024):.2f} MB)")
            
            # Tentative de connexion
            access_conn = get_access_connection()
            print("    ✅ Connexion établie")
            
            # VÃ©rifier les tables disponibles
            cursor = access_conn.cursor()
            tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
            print(f"    Tables trouvées: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}")
            
            # REQUÊTE ACCESS CORRIGÉE avec les vrais noms de colonnes
            # Note: Les colonnes avec espaces doivent être entre crochets []
            query_access = """
            SELECT 
                o.[Order ID] AS OrderID,
                o.[Order Date] AS OrderDate,
                o.[Shipped Date] AS ShippedDate,
                o.[Ship City] AS ShipCity,
                o.[Ship Country/Region] AS ShipCountry,
                c.Company AS CompanyName,
                e.[First Name] & ' ' & e.[Last Name] AS EmployeeName
            FROM (Orders o
            LEFT JOIN Customers c ON o.[Customer ID] = c.ID)
            LEFT JOIN Employees e ON o.[Employee ID] = e.ID
            """
            
            print("    -> Exécution de la requête...")
            df_access = pd.read_sql(query_access, access_conn)
            df_access['Source'] = 'Access'
            print(f"    ✅ Access : {len(df_access)} commandes récupérées.")
            
            # Afficher un aperçu
            if len(df_access) > 0:
                print(f"    Aperçu des colonnes: {df_access.columns.tolist()}")
                print(f"    Première ligne:")
                print(f"    {df_access.iloc[0].to_dict()}")
            
            access_conn.close()

    except Exception as e:
        print(f"    ❌ ERREUR Access : {e}")
        print("\n    === DÉTAILS COMPLETS DE L'ERREUR ===")
        traceback.print_exc()
        print("    =====================================\n")
        df_access = pd.DataFrame()

    # ---------------------------------------------------------
    # FUSION
    # ---------------------------------------------------------
    if df_sql.empty and df_access.empty:
        print("    ❌ AUCUNE DONNÉE RÉCUPÉRÉE !")
        return pd.DataFrame()

    # Concaténation des données
    df_final = pd.concat([df_sql, df_access], ignore_index=True)
    print(f"\n    ✅ TOTAL CONSOLIDÉ : {len(df_final)} lignes.")
    print(f"        - SQL Server : {len(df_sql)} lignes")
    print(f"        - Access : {len(df_access)} lignes")
    
    # Afficher les colonnes finales
    print(f"\n    Colonnes dans le DataFrame final:")
    for col in df_final.columns:
        non_null_count = df_final[col].notna().sum()
        print(f"        - {col}: {non_null_count} valeurs non-nulles")

    return df_final

# Exemple d'utilisation
if __name__ == "__main__":
    df = extract_data()
    if not df.empty:
        print("\n" + "="*70)
        print("APERÇU DES DONNÉES CONSOLIDÉES")
        print("="*70)
        print(df.head(10))
        print(f"\nShape: {df.shape}")
        print(f"Colonnes : {df.columns.tolist()}")
        
        # Statistiques par source
        print("\n" + "="*70)
        print("STATISTIQUES PAR SOURCE")
        print("="*70)
        print(df['Source'].value_counts())
    else:
        print("\n❌ Aucune donnée à afficher")