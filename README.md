# Northwind ETL Dashboard 

A comprehensive ETL (Extract, Transform, Load) pipeline with an interactive dashboard for analyzing order data from Northwind databases across SQL Server and Microsoft Access sources.

## Overview

This project implements a complete data warehouse solution that consolidates order information from two Northwind database sources, transforms it into analytical dimensions and facts, and presents insights through an interactive Streamlit dashboard.

**Key Capabilities:**
- Dual-source data extraction (SQL Server + Access)
- Dimensional modeling (Date, Employee, Customer dimensions)
- Real-time ETL pipeline execution
- Interactive visualizations with delivery status tracking
- Multi-dimensional analysis (temporal, customer, employee)

## Features

### ETL Pipeline
- **Extract**: Retrieves data from SQL Server Northwind and Access Northwind 2012
- **Transform**: Creates dimension tables, calculates KPIs, and cleanses data
- **Load**: Populates fact and dimension tables in SQL Server data warehouse

### Dashboard Analytics
- **KPI Metrics**: Total orders, delivered/undelivered counts, delivery rate
- **Temporal Analysis**: Complete timeline visualization with date dimension statistics
- **Customer Analysis**: Order distribution across all customers with delivery status
- **Employee Analysis**: Performance metrics by employee
- **Interactive Filters**: Real-time data exploration

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Frontend | Streamlit |
| Data Processing | Pandas |
| Visualizations | Plotly |
| Database Connectivity | SQLAlchemy, PyODBC |
| Data Warehouse | SQL Server |
| Secondary Source | Microsoft Access |

## Project Structure

```
project/
│
├── app.py                    # Main Streamlit application
├── README.md                 # This file
│
└── scripts/
    ├── ETLconfig.py          # Database connection configuration
    ├── extract.py            # Data extraction from sources
    ├── transform.py          # Data transformation & dimension creation
    ├── load.py               # Load to data warehouse
    └── main.py               # Standalone ETL execution script
```

## Prerequisites

### Software Requirements
- Python 3.8 or higher
- SQL Server (with Northwind database)
- Microsoft Access Database Engine (for .accdb files)
- ODBC Driver 17 for SQL Server

### Python Dependencies
```bash
pip install streamlit pandas plotly sqlalchemy pyodbc
```

## Installation & Setup

### 1. Clone Repository
```bash
git clone https://github.com/manel-A-tech/Dashboard-ETL.git
cd Dashboard-ETL
```

### 2. Configure Database Connections

Edit `scripts/ETLconfig.py`:

```python
# Access Database Path
ACCESS_DB_PATH = r"C:\path\to\your\Northwind 2012.accdb"

# SQL Server Configuration
SQL_SERVER = r".\SQLEXPRESS"    # Your SQL Server instance
SQL_DATABASE = "Northwind"       # Your database name
```

### 3. Verify Database Setup
- Ensure Northwind database exists in SQL Server
- Confirm Access file path is correct
- Test database connectivity

## Usage

### Launch Dashboard
```bash
streamlit run app.py
```

The dashboard will automatically:
1. Extract data from both sources
2. Transform and create dimension tables
3. Load into SQL Server data warehouse
4. Display interactive visualizations

### Refresh Data
Click the **"Rafraîchir les données"** button to re-run the complete ETL pipeline with latest data.

### Navigate Analysis
Use the tabs to explore different analytical views:
- **Par Date**: Temporal trends and date dimension insights
- **Par Client**: Customer order distribution
- **Par Employé**: Employee performance metrics

## Data Warehouse Schema

### Fact Table: FACT_Orders

| Column | Type | Description |
|--------|------|-------------|
| OrderID | int | Order identifier |
| OrderDate | datetime | Order placement date |
| ShippedDate | datetime | Shipment date (null if undelivered) |
| CustomerID | varchar | Customer identifier |
| CompanyName | varchar | Customer company name |
| EmployeeID | int | Employee identifier |
| EmployeeName | varchar | Employee full name |
| ShipCity | varchar | Delivery city |
| ShipCountry | varchar | Delivery country |
| Source | varchar | Data source (SQL_Server/Access) |
| Date | date | Date key for dimension join |
| Status_Livraison | varchar | Delivery status (Livrée/Non Livrée) |

### Dimension Tables

**DIM_Date**: Complete date dimension with year, month, quarter, week attributes

**DIM_Employee**: Employee master data (EmployeeID, EmployeeName)

**DIM_Customer**: Customer master data (CustomerID, CompanyName, ShipCity, ShipCountry)

## ETL Pipeline Details

### Extract Phase
**SQL Server Source:**
```sql
-- Joins Orders, Customers, and Employees tables
-- Retrieves order and delivery information
-- Marks source as 'SQL_Server'
```

**Access Source:**
```sql
-- Connects to Northwind 2012.accdb
-- Similar structure to SQL Server
-- Marks source as 'Access'
```

Both sources are consolidated into a unified dataset.

### Transform Phase

1. **Date Conversion**: Standardizes OrderDate and ShippedDate to datetime
2. **Date Dimension Creation**: Generates complete date range with temporal attributes
3. **Employee Dimension**: Extracts unique employees with cleansed names
4. **Customer Dimension**: Extracts unique customers with location data
5. **Delivery Status Calculation**:
   ```python
   Status = 'Livrée' if ShippedDate exists
          = 'Non Livrée' if ShippedDate is NULL
   ```

### Load Phase

Tables created in SQL Server:
- `FACT_Orders`: Main fact table
- `DIM_Date`: Date dimension
- `DIM_Employee`: Employee dimension  
- `DIM_Customer`: Customer dimension

All tables use `replace` mode for fresh loads.

## Dashboard Features

### KPI Cards
- **Total Commandes**: Complete order count
- **Commandes Livrées**: Delivered order count
- **Commandes Non Livrées**: Undelivered order count
- **Taux de Livraison**: Delivery rate percentage

### Visualizations

**Temporal Analysis:**
- Dual-line chart showing delivered vs undelivered trends
- Complete date dimension statistics
- Period metrics (average orders/day, max orders/day)

**Customer Analysis:**
- Stacked bar chart for all customers
- Delivery status breakdown per customer
- Complete customer table with totals

**Employee Analysis:**
- Stacked bar chart by employee
- Performance comparison across team
- Detailed employee metrics table

## Running Standalone ETL

Execute the ETL pipeline without the dashboard:

```bash
cd scripts
python main.py
```

This runs the complete Extract → Transform → Load sequence and populates the data warehouse.

## Troubleshooting

**Access Database Connection Issues:**
- Verify Microsoft Access Database Engine is installed
- Check file path uses raw string (r"path")
- Ensure .accdb file is not open in Access

**SQL Server Connection Issues:**
- Confirm ODBC Driver 17 is installed
- Verify SQL Server instance name
- Check Windows Authentication permissions

**Empty Data Issues:**
- Verify both databases contain data
- Check database connection strings
- Review console output for specific errors

## Contributing

Contributions are welcome! Feel free to:
- Report bugs via issues
- Suggest enhancements
- Submit pull requests

## Author

**Ameziane Manel Fatma**



**Note**: Ensure you have appropriate access permissions to both databases before running the ETL pipeline.
