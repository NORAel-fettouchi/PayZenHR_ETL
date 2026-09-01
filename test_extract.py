import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

JAR_SQLSERVER = r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar"

spark = (
    SparkSession.builder
    .appName("TEST_SQLSERVER")
    .master("local[2]")
    .config("spark.jars", JAR_SQLSERVER)
    .config("spark.driver.extraClassPath", JAR_SQLSERVER)
    .config("spark.executor.extraClassPath", JAR_SQLSERVER)
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("PYTHON")
print("=" * 80)
print(sys.executable)

url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true"
)

df = (
    spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "dbo.Companies")
    .option("user", "sa")
    .option("password", "noraNORA123")
    .option(
        "driver",
        "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    )
    .load()
)

print("=" * 80)
print("SCHEMA")
print("=" * 80)

df.printSchema()

print("=" * 80)
print("COUNT")
print("=" * 80)

print(df.count())

df.show(5, truncate=False)

spark.stop()