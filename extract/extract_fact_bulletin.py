from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import (
    SQLSERVER_URL,
    SQLSERVER_PROPERTIES,
    MSSQL_JDBC_JAR
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_FactBulletinLigne")
    .config("spark.jars", MSSQL_JDBC_JAR)
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def read_sql(table):

    return (
        spark.read
        .jdbc(
            url=SQLSERVER_URL,
            table=table,
            properties=SQLSERVER_PROPERTIES
        )
    )


# ============================================================
# INSPECTION DES TABLES DE PAIE
# ============================================================

tables = [
    "dbo.PayrollResults",
    "dbo.PayrollResultPrimes",
    "dbo.EmployeeSalaries",
    "dbo.EmployeeSalaryComponents",
    "dbo.PayComponents",
    "dbo.SalaryPackageItems",
    "dbo.ReferentielElements"
]


for table in tables:

    print("\n" + "=" * 70)
    print(table)
    print("=" * 70)

    try:

        df = read_sql(table)

        print("Colonnes :")
        print(df.columns)

        print("\nSchema :")
        df.printSchema()

        print("\nExemple :")
        df.show(5, truncate=False)

    except Exception as e:

        print(
            f"Impossible de lire {table}"
        )

        print(e)


spark.stop()