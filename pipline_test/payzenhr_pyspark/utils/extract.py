from pyspark.sql import SparkSession, DataFrame

from config import (
    SQLSERVER_HOST,
    SQLSERVER_PORT,
    SQLSERVER_DATABASE,
    SQLSERVER_USER,
    SQLSERVER_PASSWORD,
)


def get_sqlserver_url():

    return (
        f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
        f"databaseName={SQLSERVER_DATABASE};"
        "encrypt=true;"
        "trustServerCertificate=true;"
    )


def read_table(
    spark: SparkSession,
    table_name: str
) -> DataFrame:

    url = get_sqlserver_url()

    return (
        spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", f"dbo.{table_name}")
        .option("user", SQLSERVER_USER)
        .option("password", SQLSERVER_PASSWORD)
        .option(
            "driver",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        .load()
    )