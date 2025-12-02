import pandas as pd


def transform_data(df):
    """
    Fonction principale de transformation :
    - Nettoie les dates
    - Calcule le KPI (Livrée / Non Livrée)
    - Nettoie les textes
    """
    print("\n--- 2. TRANSFORMATION DES DONNÉES ---")

    if df.empty:
        print("⚠️ DataFrame vide, rien à transformer.")
        return df

    # 1. Conversion des Dates (Standardisation)
    # 'errors=coerce' transforme les erreurs en NaT (Not a Time) sans planter
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')

    # 2. Ajout des Dimensions Temporelles (Pour les graphiques)
    df['Mois_Annee'] = df['OrderDate'].dt.strftime('%Y-%m')  # Ex: 2024-11
    df['Annee'] = df['OrderDate'].dt.year

    # 3. Calcul du KPI Principal : Status de Livraison
    # Logique : Si ShippedDate est vide (NaT) => 'Non Livrée', sinon 'Livrée'
    df['Status_Livraison'] = df['ShippedDate'].apply(
        lambda x: 'Non Livrée' if pd.isna(x) else 'Livrée'
    )

    # 4. Nettoyage des textes (Enlever les espaces inutiles et mettre en majuscule)
    cols_texte = ['CompanyName', 'EmployeeName', 'ShipCity', 'ShipCountry']
    for col in cols_texte:
        if col in df.columns:
            # On convertit en chaîne, on enlève les espaces (strip) et on met en majuscules (upper)
            df[col] = df[col].astype(str).str.strip().str.upper()

    print("   ✅ Dates standardisées.")
    print("   ✅ KPI 'Status_Livraison' calculé.")

    return df