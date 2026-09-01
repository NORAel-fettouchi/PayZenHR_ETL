import os


# ============================================================
# SQL SERVER AZURE
# ============================================================

SQLSERVER_HOST = os.getenv(
    "PAYZEN_SQL_HOST",
    "payzenhr-test-sqlserver-01.database.windows.net"
)

SQLSERVER_DATABASE = os.getenv(
    "PAYZEN_SQL_DATABASE",
    "test_db"
)

SQLSERVER_USER = os.getenv(
    "PAYZEN_SQL_USER",
    "sqladmin"
)

SQLSERVER_PASSWORD = os.getenv(
    "PAYZEN_SQL_PASSWORD"
)


SQLSERVER_URL = (
    f"jdbc:sqlserver://{SQLSERVER_HOST}:1433;"
    f"databaseName={SQLSERVER_DATABASE};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)


SQLSERVER_PROPERTIES = {
    "user": SQLSERVER_USER,
    "password": SQLSERVER_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# ============================================================
# JDBC DRIVER
# ============================================================

MSSQL_JDBC_JAR = (
    r"E:\etape_1\PayZenHR_ETL\jars"
    r"\mssql-jdbc-13.4.0.jre8.jar"
)


# ============================================================
# CLICKHOUSE
# ============================================================

CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST",
    "localhost"
)

CLICKHOUSE_PORT = os.getenv(
    "CLICKHOUSE_PORT",
    "8123"
)

CLICKHOUSE_DATABASE = "payzen_bi"

CLICKHOUSE_URL = (
    f"jdbc:clickhouse://"
    f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    f"{CLICKHOUSE_DATABASE}"
)

CLICKHOUSE_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}