# Dashboard Northwind ETL

## 📋 Description

Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour analyser les données de commandes provenant de deux sources Northwind :
- **SQL Server** : Base de données Northwind classique
- **Microsoft Access** : Base de données Northwind 2012 (.accdb)

Le dashboard interactif développé avec Streamlit permet de visualiser et d'analyser les commandes livrées et non livrées selon plusieurs dimensions (clients, employés, mois, années).

## 🎯 Fonctionnalités

- **Extraction** : Récupération des données depuis SQL Server et Access
- **Transformation** : Nettoyage, standardisation et calcul des KPIs
- **Chargement** : Insertion dans une table Data Warehouse SQL Server
- **Visualisation** : Dashboard interactif avec graphiques Plotly
- **Analyse multi-dimensionnelle** : Par client, employé, mois et année
- **KPIs principaux** : Total commandes, taux de livraison, répartition par statut

## 🛠️ Technologies utilisées

- **Python 3.x**
- **Streamlit** : Interface web interactive
- **Pandas** : Manipulation des données
- **Plotly** : Visualisations graphiques
- **SQLAlchemy** : Connexion SQL Server
- **PyODBC** : Connexion Access et SQL Server
- **SQL Server** : Stockage et Data Warehouse
- **Microsoft Access** : Source de données secondaire

## 📁 Structure du projet

```
projet/
│
├── app.py                      # Application Streamlit principale
├── README.md                 
│
└── scripts/
    ├── ETLconfig.py           # Configuration des connexions
    ├── extract.py             # Extraction des données
    ├── transform.py           # Transformation des données
    └── load.py                # Chargement dans SQL Server
    └── main.py 
```

## ⚙️ Configuration

### Prérequis

1. **SQL Server** installé avec la base Northwind
2. **Microsoft Access Database Engine** pour lire les fichiers .accdb
3. **ODBC Driver 17 for SQL Server**

### Installation

1. Cloner le projet :
```bash
git clone <https://github.com/manel-A-tech/Dashboard-ETL.git>
cd northwind-etl
```

2. Installer les dépendances :
```bash
pip install streamlit pandas plotly sqlalchemy pyodbc
```

3. Configurer les connexions dans `scripts/ETLconfig.py` :
```python
# Chemin vers votre base Access
ACCESS_DB_PATH = r"C:\Users\VotreNom\Downloads\Northwind 2012.accdb"

# Serveur SQL Server
SQL_SERVER = r".\SQLEXPRESS"
SQL_DATABASE = "Northwind"
```

## 🚀 Utilisation

### Lancer le dashboard

```bash
streamlit run app.py
```

### Fonctionnement

1. **Chargement initial** : Les données sont extraites et transformées automatiquement au démarrage
2. **Rafraîchissement** : Cliquez sur "Rafraîchir les données" pour relancer l'ETL complet
3. **Navigation** : Utilisez les onglets pour explorer les analyses par dimension
4. **Tableaux détaillés** : Dépliez les sections "Voir le tableau détaillé" pour les données complètes

## 📊 Pipeline ETL détaillé

### 1. Extract (Extraction)

**Source SQL Server :**
- Jointure des tables Orders, Customers et Employees
- Récupération des informations complètes de commande
- Colonnes : OrderID, OrderDate, ShippedDate, ShipCity, ShipCountry, CompanyName, EmployeeName

**Source Access :**
- Connexion via PyODBC
- Lecture de la base Northwind 2012.accdb
- Même structure de données que SQL Server
- Consolidation avec un marqueur 'Source'

### 2. Transform (Transformation)

**Nettoyage des données :**
- Conversion des dates au format datetime
- Gestion des valeurs nulles (NaT pour les dates)

**Enrichissement :**
- Ajout de dimensions temporelles (Mois_Annee, Annee)
- Calcul du statut de livraison (Livrée/Non Livrée)
- Nettoyage des textes (strip, upper)

**KPI principal :**
```python
Status_Livraison = 'Livrée' si ShippedDate existe
                 = 'Non Livrée' si ShippedDate est NULL
```

### 3. Load (Chargement)

- Insertion dans la table `DWH_Global_Analysis` sur SQL Server
- Mode `replace` : La table est recréée à chaque refresh
- Validation du nombre de lignes insérées

## 📈 Visualisations disponibles

### KPIs principaux
- Total des commandes
- Commandes livrées
- Commandes non livrées
- Taux de livraison (%)

### Analyses graphiques

**Par Client :**
- Top 15 clients par volume de commandes
- Graphique en barres empilées
- Répartition Livrée/Non Livrée

**Par Employé :**
- Performance de chaque employé
- Graphique en barres empilées
- Identification des employés les plus actifs

**Par Mois :**
- Évolution temporelle des commandes
- Graphique en courbes
- Tendances de livraison sur le temps

**Par Année :**
- Vue d'ensemble annuelle
- Graphique en barres groupées
- Comparaison interannuelle

## 📝 Table Data Warehouse

La table `DWH_Global_Analysis` créée dans SQL Server contient :

| Colonne | Type | Description |
|---------|------|-------------|
| OrderID | int | Identifiant de commande |
| OrderDate | datetime | Date de commande |
| ShippedDate | datetime | Date d'expédition |
| ShipCity | varchar | Ville de livraison |
| ShipCountry | varchar | Pays de livraison |
| CompanyName | varchar | Nom du client |
| EmployeeName | varchar | Nom de l'employé |
| Source | varchar | SQL_Server ou Access |
| Mois_Annee | varchar | Format YYYY-MM |
| Annee | int | Année |
| Status_Livraison | varchar | Livrée ou Non Livrée |

##  Contribution

Les contributions sont les bienvenues ! N'hésitez pas à :
- Signaler des bugs
- Proposer des améliorations
- Ajouter de nouvelles fonctionnalités


##  Auteur : Ameziane Manel Fatma

---

**Note :** Assurez-vous d'avoir les droits d'accès nécessaires aux bases de données avant d'exécuter le pipeline ETL.
