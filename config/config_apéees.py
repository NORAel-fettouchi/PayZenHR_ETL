"""
Configuration centrale du projet PayZenHR ETL.
"""

from pathlib import Path


# ============================================================
# SQL SERVER
# ============================================================

SQLSERVER_JDBC_URL = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)


SQLSERVER_CONNECTION_PROPERTIES = {
    "user": "sa",
    "password": "noraNORA123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# ============================================================
# JDBC DRIVER SQL SERVER
# ============================================================

PROJECT_ROOT = Path(r"E:\etape_1\PayZenHR_ETL")

SQLSERVER_JDBC_JAR = str(
    PROJECT_ROOT
    / "jars"
    / "mssql-jdbc-13.4.0.jre8.jar"
)


# ============================================================
# CLICKHOUSE
# ============================================================

CLICKHOUSE_JDBC_URL = (
    "jdbc:clickhouse://localhost:8123/payzen_dw"
)


CLICKHOUSE_CONNECTION_PROPERTIES = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


CLICKHOUSE_JDBC_JAR = str(
    PROJECT_ROOT
    / "jars"
    / "clickhouse-jdbc-0.9.0-all.jar"
)


# ============================================================
# SPARK
# ============================================================

SPARK_APP_NAME = "PayZenHR_ETL"

SPARK_SHUFFLE_PARTITIONS = "4"


# ============================================================
# TABLES SOURCES
# ============================================================

ALL_SOURCE_TABLES = [
    "Employees",
    "Departments",
    "Cities",
]