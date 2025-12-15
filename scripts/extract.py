import pandas as pd
import sqlalchemy
import pyodbc
import os
import traceback

try:
    from .ETLconfig import ACCESS_CONN_STRING, SQL_CONN_STRING, ACCESS_DB_PATH
except ImportError:
    from ETLconfig import ACCESS_CONN_STRING, SQL_CONN_STRING, ACCESS_DB_PATH


# =========================================================
# CONNEXIONS
# =========================================================
def get_sql_engine():
    conn_str = f"mssql+pyodbc:///?odbc_connect={SQL_CONN_STRING.replace(' ', '%20')}"
    return sqlalchemy.create_engine(conn_str)


def get_access_connection():
    return pyodbc.connect(ACCESS_CONN_STRING)


# =========================================================
# EXTRACTION
# =========================================================
def extract_data():
    print("\n--- 1. EXTRACTION DES DONNÉES ---")

    # =====================================================
    # A. SQL SERVER
    # =====================================================
    print("-> Connexion à SQL Server...")
    try:
        engine = get_sql_engine()

        query_sql = """
        SELECT 
            o.OrderID,
            o.OrderDate,
            o.ShippedDate,
            o.CustomerID,
            c.CompanyName,
            o.EmployeeID,
            e.FirstName + ' ' + e.LastName AS EmployeeName,
            o.ShipCity,
            o.ShipCountry
        FROM Orders o
        LEFT JOIN Customers c ON o.CustomerID = c.CustomerID
        LEFT JOIN Employees e ON o.EmployeeID = e.EmployeeID
        """

        df_sql = pd.read_sql(query_sql, engine)
        df_sql['Source'] = 'SQL_Server'
        print(f"SQL Server : {len(df_sql)} commandes récupérées.")

    except Exception as e:
        print(f" Erreur SQL Server : {e}")
        df_sql = pd.DataFrame()

    # =====================================================
    # B. ACCESS
    # =====================================================
    print("\n-> Connexion à Access...")
    print(f"-> Chemin: {ACCESS_DB_PATH}")

    try:
        if not os.path.exists(ACCESS_DB_PATH):
            print("Fichier Access introuvable")
            df_access = pd.DataFrame()
        else:
            access_conn = get_access_connection()
            print("Connexion Access établie")

            query_access = """
            SELECT 
                o.[Order ID] AS OrderID,
                o.[Order Date] AS OrderDate,
                o.[Shipped Date] AS ShippedDate,
                o.[Customer ID] AS CustomerID,
                c.Company AS CompanyName,
                o.[Employee ID] AS EmployeeID,
                e.[First Name] & ' ' & e.[Last Name] AS EmployeeName,
                o.[Ship City] AS ShipCity,
                o.[Ship Country/Region] AS ShipCountry
            FROM (Orders o
            LEFT JOIN Customers c ON o.[Customer ID] = c.ID)
            LEFT JOIN Employees e ON o.[Employee ID] = e.ID
            """

            df_access = pd.read_sql(query_access, access_conn)
            df_access['Source'] = 'Access'
            print(f"Access : {len(df_access)} commandes récupérées.")

            access_conn.close()

    except Exception as e:
        print(f"Erreur Access : {e}")
        traceback.print_exc()
        df_access = pd.DataFrame()

    # =====================================================
    # C. FUSION
    # =====================================================
    if df_sql.empty and df_access.empty:
        print(" AUCUNE DONNÉE EXTRAITE")
        return pd.DataFrame()

    df_final = pd.concat([df_sql, df_access], ignore_index=True)

    print("\n--- RÉSUMÉ EXTRACTION ---")
    print(f"Total lignes : {len(df_final)}")
    print(f"- SQL Server : {len(df_sql)}")
    print(f"- Access     : {len(df_access)}")

    print("\nColonnes finales :")
    for col in df_final.columns:
        print(f"- {col}")

    return df_final

    






