from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws

# =====================================================
# 1. SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("ETL_Cabinet_Comptable")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\pipline_test\payzenhr_pyspark\jars\mssql-jdbc-13.4.0.jre8.jar,"
        r"E:\etape_1\PayZenHR_ETL\pipline_test\payzenhr_pyspark\jars\clickhouse-jdbc-0.9.8-all.jar"
    )
    .getOrCreate()
)

# =====================================================
# 2. SQL SERVER
# =====================================================

sqlserver_url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

sqlserver_properties = {
    "user": "sa",
    "password": "noraNORA123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# =====================================================
# 3. CLICKHOUSE
# =====================================================

clickhouse_url = (
    "jdbc:clickhouse://localhost:8124/payzen"
)

clickhouse_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "1234"
}

# =====================================================
# 4. EXTRACTION SQL SERVER
# =====================================================

companies_df = spark.read.jdbc(
    url=sqlserver_url,
    table="dbo.Companies",
    properties=sqlserver_properties
)

employees_df = spark.read.jdbc(
    url=sqlserver_url,
    table="dbo.Employees",
    properties=sqlserver_properties
)

departements_df = spark.read.jdbc(
    url=sqlserver_url,
    table="dbo.Departements",
    properties=sqlserver_properties
)

payroll_df = spark.read.jdbc(
    url=sqlserver_url,
    table="dbo.PayrollResults",
    properties=sqlserver_properties
)

# =====================================================
# 5. CLIENTS DU CABINET BEFEC
# =====================================================

clients_df = (
    companies_df
    .filter(col("ManagedByCompanyId") == 30)
)

# =====================================================
# 6. DIM_CLIENTS
# =====================================================

dim_clients_df = (
    clients_df
    .select(
        col("Id").alias("company_id"),
        col("CompanyName").alias("company_name"),
        col("BusinessSector").alias("sector")
    )
)

# =====================================================
# 7. BULLETINS RETENUS
# =====================================================

payroll_valid_df = (
    payroll_df
    .filter(col("Status").isin(2, 5, 6))
)

# =====================================================
# 8. FACT_BULLETINS
# =====================================================

fact_bulletins_df = (
    payroll_valid_df

    .join(
        clients_df,
        payroll_valid_df.CompanyId == clients_df.Id,
        "inner"
    )

    .join(
        employees_df,
        payroll_valid_df.EmployeeId == employees_df.Id,
        "left"
    )

    .join(
        departements_df,
        employees_df.DepartementId == departements_df.Id,
        "left"
    )

    .select(
    payroll_valid_df.EmployeeId.alias("employee_id"),

    concat_ws(
        " ",
        employees_df.FirstName,
        employees_df.LastName
    ).alias("employee_name"),

    clients_df.CompanyName.alias("company_name"),

    payroll_valid_df.CompanyId.alias("company_id"),

    payroll_valid_df.Month.alias("month"),
    payroll_valid_df.Year.alias("year"),

    payroll_valid_df.Status.alias("status"),

    departements_df.DepartementName.alias("service"),

    payroll_valid_df.TotalBrut.alias("brut"),
    payroll_valid_df.TotalCotisationsPatronales.alias("charges_patronales"),
    payroll_valid_df.TotalCotisationsSalariales.alias("charges_salariales"),
    payroll_valid_df.ImpotRevenu.alias("ir"),
    payroll_valid_df.NetAPayer.alias("net")
)
)

# =====================================================
# 9. DIM_CALENDAR
# =====================================================

dim_calendar_df = (
    fact_bulletins_df
    .select(
        col("month"),
        col("year")
    )
    .distinct()
)

# =====================================================
# 10. CONTROLES
# =====================================================

print("===== DIM CLIENTS =====")
dim_clients_df.show(truncate=False)

print("===== DIM CALENDAR =====")
dim_calendar_df.show(truncate=False)

print("===== FACT BULLETINS =====")
fact_bulletins_df.show(50, truncate=False)

print("Nombre clients :", dim_clients_df.count())
print("Nombre périodes :", dim_calendar_df.count())
print("Nombre bulletins :", fact_bulletins_df.count())

print("Répartition par statut :")

fact_bulletins_df.groupBy("status") \
    .count() \
    .orderBy("status") \
    .show()

# =====================================================
# 11. LOAD CLICKHOUSE
# =====================================================

dim_clients_df.write \
    .mode("overwrite") \
    .jdbc(
        url=clickhouse_url,
        table="dim_clients",
        properties=clickhouse_properties
    )

print("dim_clients chargée")

dim_calendar_df.write \
    .mode("overwrite") \
    .jdbc(
        url=clickhouse_url,
        table="dim_calendar",
        properties=clickhouse_properties
    )

print("dim_calendar chargée")

fact_bulletins_df.write \
    .mode("overwrite") \
    .jdbc(
        url=clickhouse_url,
        table="fact_bulletins",
        properties=clickhouse_properties
    )

print("fact_bulletins chargée")

# =====================================================
# 12. FIN
# =====================================================

spark.stop()