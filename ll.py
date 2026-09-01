from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestClickHouse")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\pipline_test\payzenhr_pyspark\jars\clickhouse-jdbc-0.9.8-all.jar"
    )
    .getOrCreate()
)

url = "jdbc:clickhouse://localhost:8124/payzen"

properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "1234"
}

print("===== TEST CLICKHOUSE JDBC =====")

df = spark.read.jdbc(
    url=url,
    table="system.one",
    properties=properties
)

df.show()

print("Connexion JDBC OK")

spark.stop()