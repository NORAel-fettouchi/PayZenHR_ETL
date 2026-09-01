import pyodbc

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=PayZenHR_Source;"
    "UID=sa;"
    "PWD=noraNORA123;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

try:
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()
    cursor.execute("SELECT DB_NAME()")

    result = cursor.fetchone()

    print("✅ Connexion réussie")
    print("Base connectée :", result[0])

    conn.close()

except Exception as e:
    print("❌ Erreur de connexion")
    print(e)