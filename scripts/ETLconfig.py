# Connexion Access 
ACCESS_DB_PATH = r"C:\Users\Acer\Downloads\Northwind 2012.accdb"
ACCESS_CONN_STRING = (
  r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
  f"DBQ={ACCESS_DB_PATH};"
)
    
# Connexion SQL Server 
SQL_SERVER = r".\SQLEXPRESS" 
SQL_DATABASE = "Northwind"  
SQL_CONN_STRING = (
  f"DRIVER={{ODBC Driver 17 for SQL Server}};"
  f"SERVER={SQL_SERVER};"
  f"DATABASE={SQL_DATABASE};"
  f"Trusted_Connection=yes;"
)