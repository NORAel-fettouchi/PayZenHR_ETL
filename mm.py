from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit

# ======================================
# Création de la session Spark
# ======================================
spark = (
    SparkSession.builder
    .appName("PayZenHR_ETL")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar"
    )
    .getOrCreate()
)

# ======================================
# Connexion SQL Server
# ======================================
url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

properties = {
    "user": "nora",
    "password": "nora",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

try:

    # ======================================
    # ETAPE 1 : EXTRACTION
    # ======================================
    print("Lecture de la table Employees...")

    df = spark.read.jdbc(
        url=url,
        table="dbo.Employees",
        properties=properties
    )

    print("Table originale :")
    df.show()

    # ======================================
    # ETAPE 2 : TRANSFORMATION
    # ======================================

    df_transforme = (
        df
        # Garder uniquement les employés dont le salaire >= 15000
        .filter(col("Salary") >= 15000)

        # Créer une colonne NomComplet
        .withColumn(
            "NomComplet",
            concat_ws(" ", col("FirstName"), col("LastName"))
        )

        # Ajouter une prime fixe
        .withColumn(
            "Prime",
            lit(1000)
        )

        # Calculer le nouveau salaire
        .withColumn(
            "SalaireTotal",
            col("Salary") + col("Prime")
        )

        # Supprimer une colonne inutile
        .drop("DepartmentID")
    )

    print("Table transformée :")
    df_transforme.show(truncate=False)

    # ======================================
    # ETAPE 3 : CHARGEMENT
    # ======================================

    df_transforme.write.jdbc(
        url=url,
        table="Employees_ETL",
        mode="overwrite",
        properties=properties
    )

    print("Nouvelle table Employees_ETL créée avec succès.")

except Exception as e:
    print("Erreur :")
    print(e)

finally:
    spark.stop()