from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    lit,
    to_date
)

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
    .appName("PayZenHR_Dimensions")
    .config("spark.jars", MSSQL_JDBC_JAR)
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# FONCTION LECTURE SQL SERVER
# ============================================================

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
# DIM CLIENTS
# ============================================================

print("\n========== DIM CLIENTS ==========")

companies = read_sql("dbo.Companies")

companies.printSchema()

dim_clients = companies.select(
    col("Id").cast("long").alias("client_id"),
    col("Name").alias("nom")
)

if "Sector" in companies.columns:
    dim_clients = dim_clients.withColumn(
        "secteur",
        col("Sector")
    )
else:
    dim_clients = dim_clients.withColumn(
        "secteur",
        lit(None).cast("string")
    )

dim_clients = (
    dim_clients
    .withColumn(
        "taille",
        lit(None).cast("string")
    )
    .withColumn(
        "cabinet_id",
        lit(None).cast("long")
    )
)

dim_clients.show(10, truncate=False)


# ============================================================
# DIM EMPLOYEES
# ============================================================

print("\n========== DIM EMPLOYEES ==========")

employees = read_sql("dbo.Employees")

employees.printSchema()

dim_employees = employees.select(
    col("Id").cast("long").alias("employee_id")
)

# Construction simple du nom
if "FirstName" in employees.columns:
    dim_employees = dim_employees.withColumn(
        "nom",
        concat_ws(
            " ",
            coalesce(col("FirstName"), lit("")),
            coalesce(col("LastName"), lit(""))
        )
    )
else:
    dim_employees = dim_employees.withColumn(
        "nom",
        lit("")
    )

dim_employees = (
    dim_employees
    .withColumn("poste", lit(None).cast("string"))
    .withColumn("niveau", lit(None).cast("string"))
    .withColumn("service", lit(None).cast("string"))
    .withColumn("date_embauche", lit(None).cast("date"))
)

dim_employees.show(10, truncate=False)


# ============================================================
# DIM CALENDAR
# ============================================================

print("\n========== DIM CALENDAR ==========")

from pyspark.sql.functions import (
    sequence,
    explode,
    year,
    month,
    dayofmonth,
    quarter
)

calendar = (
    spark.range(1)
    .select(
        explode(
            sequence(
                lit("2020-01-01").cast("date"),
                lit("2035-12-31").cast("date")
            )
        ).alias("date")
    )
    .select(
        col("date"),
        dayofmonth("date").alias("jour"),
        month("date").alias("mois"),
        quarter("date").alias("trimestre"),
        year("date").alias("annee")
    )
)

calendar.show(10)


# ============================================================
# EXPORT LOCAL POUR VERIFICATION
# ============================================================

dim_clients.write.mode("overwrite").parquet(
    "output/dim_clients"
)

dim_employees.write.mode("overwrite").parquet(
    "output/dim_employees"
)

calendar.write.mode("overwrite").parquet(
    "output/dim_calendar"
)


spark.stop()