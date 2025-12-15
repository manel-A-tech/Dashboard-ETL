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
    CHARGE :
    - Table de faits
    - DIM_Date
    - DIM_Employee
    - DIM_Customer
    """
    print("\n--- 3. CHARGEMENT (LOAD VERS SQL SERVER) ---")

    FACT_TABLE = "FACT_Orders"
    DIM_DATE_TABLE = "DIM_Date"
    DIM_EMPLOYEE_TABLE = "DIM_Employee"
    DIM_CUSTOMER_TABLE = "DIM_Customer"

    try:
        engine = get_sql_engine()

        print("-> Connexion SQL Server réussie")

        # =========================================================
        # 1. RÉCUPÉRATION DES DIMENSIONS
        # =========================================================
        dim_date = df.attrs.get('dim_date')
        dim_employee = df.attrs.get('dim_employee')
        dim_customer = df.attrs.get('dim_customer')

        # =========================================================
        # 2. CHARGEMENT TABLE DE FAITS
        # =========================================================
        print(f"\n-> Chargement table de faits : {FACT_TABLE}")
        df_fact = df.copy()

        df_fact.to_sql(
            FACT_TABLE,
            engine,
            if_exists='replace',
            index=False
        )

        print(f"SUCCÈS : {len(df_fact)} lignes insérées dans {FACT_TABLE}")

        # =========================================================
        # 3. CHARGEMENT DIM_DATE
        # =========================================================
        if dim_date is not None:
            print(f"\n-> Chargement dimension Date : {DIM_DATE_TABLE}")
            dim_date.to_sql(
                DIM_DATE_TABLE,
                engine,
                if_exists='replace',
                index=False
            )
            print(f"SUCCÈS : {len(dim_date)} lignes dans {DIM_DATE_TABLE}")

        # =========================================================
        # 4. CHARGEMENT DIM_EMPLOYEE
        # =========================================================
        if dim_employee is not None:
            print(f"\n-> Chargement dimension Employee : {DIM_EMPLOYEE_TABLE}")
            dim_employee.to_sql(
                DIM_EMPLOYEE_TABLE,
                engine,
                if_exists='replace',
                index=False
            )
            print(f"SUCCÈS : {len(dim_employee)} lignes dans {DIM_EMPLOYEE_TABLE}")

        # =========================================================
        # 5. CHARGEMENT DIM_CUSTOMER
        # =========================================================
        if dim_customer is not None:
            print(f"\n-> Chargement dimension Customer : {DIM_CUSTOMER_TABLE}")
            dim_customer.to_sql(
                DIM_CUSTOMER_TABLE,
                engine,
                if_exists='replace',
                index=False
            )
            print(f"SUCCÈS : {len(dim_customer)} lignes dans {DIM_CUSTOMER_TABLE}")

        # =========================================================
        # 6. VÉRIFICATIONS
        # =========================================================
        with engine.begin() as conn:
            for table in [
                FACT_TABLE,
                DIM_DATE_TABLE,
                DIM_EMPLOYEE_TABLE,
                DIM_CUSTOMER_TABLE
            ]:
                try:
                    result = pd.read_sql(
                        f"SELECT COUNT(*) AS count FROM {table}",
                        conn
                    )
                    print(f"VÉRIFICATION : {table} → {result['count'].iloc[0]} lignes")
                except Exception:
                    print(f"Table {table} non trouvée (normal si dimension absente)")

        print("\nCHARGEMENT COMPLET TERMINÉ ")
        return True

    except Exception as e:
        print(f" Erreur lors du chargement SQL : {e}")
        return False


