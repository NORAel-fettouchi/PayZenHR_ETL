from pyspark.sql import SparkSession

# ==========================
# Création de la session Spark
# ==========================
spark = (
    SparkSession.builder
    .appName("PayZenHR_ETL")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar"
    )
    .getOrCreate()
)

# ==========================
# URL JDBC
# ==========================
url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

# ==========================
# Informations de connexion
# ==========================
properties = {
    "user": "nora",
    "password": "nora",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# ==========================
# Lecture de la table Employees
# ==========================
try:
    df = spark.read.jdbc(
        url=url,
        table="dbo.Employees",
        properties=properties
    )

    print("Connexion réussie !")
    print(f"Nombre de lignes : {df.count()}")

    df.printSchema()

    df.show(10, truncate=False)
    

except Exception as e:
    print("\nErreur pendant la connexion :")
    print(e)

finally:
    spark.stop()
