from pyspark.sql import SparkSession

# ==========================
# Création de la session Spark
# ==========================
spark = (
    SparkSession.builder
    .appName("Extraction_Azure_SQL")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar"
    )
    .getOrCreate()
)

# ==========================
# Connexion Azure SQL Database
# ==========================
url = (
    "jdbc:sqlserver://payzenhr-test-sqlserver-01.database.windows.net:1433;"
    "databaseName=test_db;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;" 
)

properties = {
    "user": "sqladmin",
    "password": "ncarfe4s7PJ4sc",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# ==========================
# Lecture de la table Cities
# ==========================
df = spark.read.jdbc(
    url=url,
    table="dbo.Cities",
    properties=properties
)

# ==========================
# Afficher les 10 premières lignes
# ==========================
df.show(10, truncate=False)

# ==========================
# Arrêter Spark
# ==========================
spark.stop()