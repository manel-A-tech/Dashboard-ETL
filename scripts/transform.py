import pandas as pd


def transform_data(df):
    """
    Transformation des données :
    - Dimension Date
    - Dimension Employee
    - Dimension Customer
    - KPI Livraison
    """
    print("\n--- 2. TRANSFORMATION DES DONNÉES ---")

    if df.empty:
        print("DataFrame vide, rien à transformer.")
        return df

    # =========================================================
    # 1. CONVERSION DES DATES
    # =========================================================
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')

    # =========================================================
    # 2. DIMENSION DATE
    # =========================================================
    all_dates = pd.concat([
        df['OrderDate'].dropna(),
        df['ShippedDate'].dropna()
    ])

    if not all_dates.empty:
        date_min = all_dates.min()
        date_max = all_dates.max()

        date_range = pd.date_range(start=date_min, end=date_max, freq='D')

        dim_date = pd.DataFrame({
            'Date': date_range,
            'Annee': date_range.year,
            'Mois': date_range.month,
            'Jour': date_range.day,
            'Trimestre': date_range.quarter,
            'Mois_Annee': date_range.strftime('%Y-%m'),
            'Nom_Mois': date_range.strftime('%B'),
            'Nom_Jour': date_range.strftime('%A'),
            'Semaine': date_range.isocalendar().week,
            'Jour_Annee': date_range.dayofyear
        })

        # Clé Date dans la table de faits
        df['Date'] = df['OrderDate'].dt.date

        df.attrs['dim_date'] = dim_date
        print(f"Dimension Date créée : {len(dim_date)} lignes")

    # =========================================================
    # 3. DIMENSION EMPLOYEE
    # =========================================================
    if {'EmployeeID', 'EmployeeName'}.issubset(df.columns):
        dim_employee = (
            df[['EmployeeID', 'EmployeeName']]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Nettoyage
        dim_employee['EmployeeName'] = (
            dim_employee['EmployeeName']
            .astype(str)
            .str.strip()
            .str.upper()
        )

        df.attrs['dim_employee'] = dim_employee
        print(f"Dimension Employee créée : {len(dim_employee)} employés")

    # =========================================================
    # 4. DIMENSION CUSTOMER
    # =========================================================
    if {'CustomerID', 'CompanyName'}.issubset(df.columns):
        dim_customer = (
            df[['CustomerID', 'CompanyName', 'ShipCity', 'ShipCountry']]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Nettoyage
        for col in ['CompanyName', 'ShipCity', 'ShipCountry']:
            dim_customer[col] = (
                dim_customer[col]
                .astype(str)
                .str.strip()
                .str.upper()
            )

        df.attrs['dim_customer'] = dim_customer
        print(f"Dimension Customer créée : {len(dim_customer)} clients")

    # =========================================================
    # 5. KPI LIVRAISON
    # =========================================================
    df['Status_Livraison'] = df['ShippedDate'].apply(
        lambda x: 'Non Livrée' if pd.isna(x) else 'Livrée'
    )

    print("Transformation terminée.")

    return df


