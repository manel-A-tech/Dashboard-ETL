import pandas as pd


def transform_data(df):
    """
    Fonction principale de transformation :
    - Nettoie les dates
    - Calcule le KPI (Livrée / Non Livrée)
    - Nettoie les textes
    - Crée une dimension Date 
    """
    print("\n--- 2. TRANSFORMATION DES DONNÉES ---")

    if df.empty:
        print("DataFrame vide, rien à transformer.")
        return df

    # 1. Conversion des Dates (Standardisation)
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')

    # 2. Création de la Dimension Date 
    # Trouver la date minimale et maximale dans toutes les dates
    all_dates = pd.concat([
        df['OrderDate'].dropna(),
        df['ShippedDate'].dropna()
    ])
    
    if len(all_dates) > 0:
        date_min = all_dates.min()
        date_max = all_dates.max()
        
        print(f"Plage de dates : {date_min.date()} → {date_max.date()}")
        
        # Créer une série continue de dates (toutes les dates entre min et max)
        date_range = pd.date_range(start=date_min, end=date_max, freq='D')
        
        # Créer la table de dimension Date
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
        
        print(f"Dimension Date créée : {len(dim_date)} jours")
        
        # Ajouter la dimension Date au DataFrame principal
        # On garde OrderDate pour la jointure, mais on aura accès à toute la dimension
        df['Date'] = df['OrderDate'].dt.date
        df['Annee'] = df['OrderDate'].dt.year
        df['Mois'] = df['OrderDate'].dt.month
        df['Trimestre'] = df['OrderDate'].dt.quarter
        df['Mois_Annee'] = df['OrderDate'].dt.strftime('%Y-%m')
        
        # Stocker la dimension Date séparément (elle sera exportée)
        df.attrs['dim_date'] = dim_date
    else:
        print("Aucune date valide trouvée")

    # 3. Calcul du KPI Principal : Status de Livraison
    df['Status_Livraison'] = df['ShippedDate'].apply(
        lambda x: 'Non Livrée' if pd.isna(x) else 'Livrée'
    )

    # 4. Nettoyage des textes
    cols_texte = ['CompanyName', 'EmployeeName', 'ShipCity', 'ShipCountry']
    for col in cols_texte:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    print("Dates standardisées.")
    print("KPI 'Status_Livraison' calculé.")
    print("Dimensions temporelles ajoutées.")

    return df