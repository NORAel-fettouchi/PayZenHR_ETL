from pyspark.sql import SparkSession


def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("PayZenHR_ETL_Test")
        .config(
            "spark.jars",
            r"E:\etape_1\PayZenHR_ETL\pipline_test\payzenhr_pyspark\jars\mssql-jdbc-13.4.0.jre8.jar"
        )
        .getOrCreate()
    )

    return spark