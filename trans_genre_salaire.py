from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, row_number,when
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id

# =====================================================
# 1. Création de SparkSession
# =====================================================

spark = (
    SparkSession.builder
    .appName("Transformation Genre Salaire")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\pipline_test\payzenhr_pyspark\jars\mssql-jdbc-13.4.0.jre8.jar,"
        r"E:\etape_1\PayZenHR_ETL\pipline_test\payzenhr_pyspark\jars\clickhouse-jdbc-0.9.8-all.jar"
    )
    .getOrCreate()
)

# =====================================================
# 2. Configuration SQL Server
# =====================================================

jdbc_url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

properties = {
    "user": "sa",
    "password": "noraNORA123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# =====================================================
# 3. EXTRACTION
# =====================================================

employees_df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.Employees",
    properties=properties
)

genders_df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.Genders",
    properties=properties
)

salaries_df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.EmployeeSalaries",
    properties=properties
)

# =====================================================
# 4. GARDER LE SALAIRE ACTUEL
# =====================================================

# On conserve uniquement les salaires actifs
active_salaries = salaries_df.filter(col("EndDate").isNull())

# Si plusieurs salaires actifs existent pour un employé :
# 1- EffectiveDate la plus récente
# 2- Id le plus grand

window_spec = (
    Window.partitionBy("EmployeeId")
    .orderBy(
        col("EffectiveDate").desc(),
        col("Id").desc()
    )
)

current_salary_df = (
    active_salaries
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# =====================================================
# 5. JOINTURES
# =====================================================

employee_salary_df = employees_df.join(
    current_salary_df,
    employees_df.Id == current_salary_df.EmployeeId,
    "left"
)

employee_salary_gender_df = employee_salary_df.join(
    genders_df,
    employees_df.GenderId == genders_df.Id,
    "left"
)

# =====================================================
# 6. SÉLECTION DES COLONNES
# =====================================================

result_df = employee_salary_gender_df.select(
    employees_df.Id.alias("employee_id"),
    employees_df.FirstName.alias("employee_name"),
    employees_df.LastName.alias("employee_last_name"),
    genders_df.NameFr.alias("gender_fr"),
    genders_df.NameAr.alias("gender_ar"),
    current_salary_df.BaseSalary.alias("salary")
)

# Les employés sans salaire actif auront 0
result_df = result_df.fillna({"salary": 0})

# =====================================================
# 7. CONTRÔLES
# =====================================================

print("Nombre total de lignes :", result_df.count())

print("Nombre d'employés distincts :")
print(result_df.select("employee_id").distinct().count())

print("Employés dupliqués :")

result_df.groupBy("employee_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

print("Résultat")

result_df.show(100, truncate=False)

# =====================================================
# 8. CALCUL DU SALAIRE PAR GENRE
# =====================================================

salary_by_gender_df = (
    result_df
    .groupBy("gender_fr")
    .agg(
        count("employee_id").alias("number_of_employees"),
        avg("salary").alias("average_salary"),
        sum("salary").alias("total_salary")
    )
)

print("Salaire par genre")

salary_by_gender_df.show()
# =====================================================
# 9. CREATION DE LA DIMENSION GENDER
# =====================================================

dim_gender_df = (
    result_df
    .select(
        "gender_fr",
        "gender_ar"
    )
    .filter(col("gender_fr").isNotNull())
    .distinct()
    .withColumn(
        "gender_key",
        monotonically_increasing_id() + 1
    )
)

print("Dimension Gender")

dim_gender_df.show()
# =====================================================
# 10. CREATION DE LA TABLE DE FAITS
# =====================================================

fact_salary_by_gender_df = (
    salary_by_gender_df
    .join(
        dim_gender_df,
        "gender_fr",
        "left"
    )
    .select(
        "gender_key",
        "gender_fr",
        "number_of_employees",
        "average_salary",
        "total_salary"
    )
)

print("Fact Salary By Gender")
fact_salary_df = (
    result_df
    .withColumn(
        "gender_key",
        when(col("gender_fr") == "Homme", 1)
        .when(col("gender_fr") == "Femme", 2)
        .otherwise(0)
    )
    .select(
        "employee_id",
        "gender_key",
        "salary"
    )
)

fact_salary_by_gender_df.show()


# =====================================================
# 11. CONNEXION CLICKHOUSE
# =====================================================

clickhouse_url = (
    "jdbc:clickhouse://localhost:8124/payzen_dw"
)

clickhouse_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "1234"
}
# =====================================================
# 9. EXPORT
# =====================================================


# =====================================================
# 10. FIN
# =====================================================
# =====================================================
# 12. CHARGEMENT DIM_GENDER
# =====================================================

dim_gender_df.write \
    .mode("overwrite") \
    .jdbc(
        url=clickhouse_url,
        table="dim_gender",
        properties=clickhouse_properties
    )

print("dim_gender chargée.")
# =====================================================
# 13. CHARGEMENT FACT
# =====================================================

fact_salary_by_gender_df.write \
    .mode("overwrite") \
    .jdbc(
        url=clickhouse_url,
        table="fact_salary_by_gender",
        properties=clickhouse_properties
    )

print("fact_salary_by_gender chargée.")
fact_salary_df.write \
    .mode("overwrite") \
    .jdbc(
        url=clickhouse_url,
        table="fact_salary",
        properties=clickhouse_properties
    )

print("fact_salary chargée.")

spark.stop()