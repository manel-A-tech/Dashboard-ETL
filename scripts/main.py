
from extract import extract_data
from transform import transform_data
from load import load_data

def main():
    

    # -----------------------------
    # 1. EXTRACTION
    # -----------------------------
    df = extract_data()
    if df.empty:
        print(" Aucune donnée extraite. Fin du script.")
        return

    # -----------------------------
    # 2. TRANSFORMATION
    # -----------------------------
    df_transformed = transform_data(df)

    # -----------------------------
    # 3. CHARGEMENT
    # -----------------------------
    success = load_data(df_transformed)
    if success:
        print("\n ETL terminé avec succès !")
    else:
        print("\n ETL terminé avec erreurs.")


if __name__ == "__main__":
    main()
