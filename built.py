import os
import sys
import logging
from functools import reduce

# ============================================================
# PYTHON UTILISE PAR PYSPARK
# ============================================================

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE

print("=" * 80)
print("PYTHON EXECUTABLE :", PYTHON_EXECUTABLE)
print("PYSPARK_PYTHON   :", os.environ["PYSPARK_PYTHON"])
print("PYSPARK_DRIVER   :", os.environ["PYSPARK_DRIVER_PYTHON"])
print("=" * 80)


# ============================================================
# PYSPARK
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)


# ============================================================
# PAYZEN HR ETL / BI
#
# SQL SERVER
#       ↓
# PYSPARK
#       ↓
# TRANSFORMATIONS
#       ↓
# CLICKHOUSE
#
# MODE :
#
# SQL SERVER :
#   READ ONLY
#
# CLICKHOUSE :
#   INSERT ONLY
#
# Pas de :
#   DROP
#   TRUNCATE
#   DELETE
#
# Anti-doublons avant INSERT
# ============================================================


# ============================================================
# CONFIGURATION SQL SERVER
# ============================================================

SQLSERVER_HOST = "localhost"
SQLSERVER_PORT = "1433"
SQLSERVER_DATABASE = "PayZenHR_Source"

SQLSERVER_USER = os.environ.get(
    "PAYZENHR_SQL_USER",
    "sa"
)

SQLSERVER_PASSWORD = os.environ.get(
    "PAYZENHR_SQL_PASSWORD",
    "noraNORA123"
)


# ============================================================
# CONFIGURATION CLICKHOUSE
# ============================================================

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_JDBC_PORT = "9001"

CLICKHOUSE_DATABASE = "payzen_bi"

CLICKHOUSE_USER = os.environ.get(
    "PAYZENHR_CLICKHOUSE_USER",
    "default"
)

CLICKHOUSE_PASSWORD = os.environ.get(
    "PAYZENHR_CLICKHOUSE_PASSWORD",
    "1234"
)


# ============================================================
# JDBC DRIVERS
# ============================================================

MSSQL_JDBC_DRIVER = (
    "com.microsoft.sqlserver.jdbc.SQLServerDriver"
)

CLICKHOUSE_JDBC_DRIVER = (
    "com.clickhouse.jdbc.ClickHouseDriver"
)


# ============================================================
# JDBC URLS
# ============================================================

MSSQL_JDBC_URL = (
    f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
    f"databaseName={SQLSERVER_DATABASE};"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://"
    f"{CLICKHOUSE_HOST}:"
    f"{CLICKHOUSE_JDBC_PORT}/"
    f"{CLICKHOUSE_DATABASE}"
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "PAYZENHR_ETL"
)


# ============================================================
# PATH JARS
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

JARS_DIR = os.path.join(
    BASE_DIR,
    "jars"
)

MSSQL_JAR = os.path.join(
    JARS_DIR,
    "mssql-jdbc-13.4.0.jre8.jar"
)

CLICKHOUSE_JAR = os.path.join(
    JARS_DIR,
    "clickhouse-jdbc-0.9.8-all.jar"
)


# ============================================================
# VERIFICATION DES JARS
# ============================================================

if not os.path.isfile(MSSQL_JAR):
    raise FileNotFoundError(
        f"JAR SQL Server introuvable : {MSSQL_JAR}"
    )

if not os.path.isfile(CLICKHOUSE_JAR):
    raise FileNotFoundError(
        f"JAR ClickHouse introuvable : {CLICKHOUSE_JAR}"
    )


# ============================================================
# SPARK SESSION
#
# CORRECTIONS DE STABILITE WINDOWS :
#
# - local[1] au lieu de local[2]
# - Python du venv explicitement utilisé
# - suppression de spark.python.worker.reuse=false
# - suppression de spark.local.dir personnalisé
#
# La logique ETL ne change pas.
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_ETL_V4")

    .master("local[1]")

    .config(
        "spark.jars",
        ",".join([
            MSSQL_JAR,
            CLICKHOUSE_JAR
        ])
    )

    .config(
        "spark.pyspark.python",
        PYTHON_EXECUTABLE
    )

    .config(
        "spark.pyspark.driver.python",
        PYTHON_EXECUTABLE
    )

    .config(
        "spark.sql.shuffle.partitions",
        "1"
    )

    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )

    .config(
        "spark.sql.legacy.timeParserPolicy",
        "LEGACY"
    )

    .getOrCreate()
)


# ============================================================
# UTILITAIRES
# ============================================================

def ensure_sqlserver_credentials():

    if not SQLSERVER_USER:

        raise ValueError(
            "PAYZENHR_SQL_USER est vide."
        )

    if not SQLSERVER_PASSWORD:

        raise ValueError(
            "PAYZENHR_SQL_PASSWORD est vide.\n"
            "Dans PowerShell :\n"
            '$env:PAYZENHR_SQL_PASSWORD = "ton_mot_de_passe"'
        )


# ============================================================
# DATE MENSUELLE
# ============================================================

def sql_month_date(
    year_column,
    month_column
):

    return F.to_date(
        F.concat_ws(
            "-",

            F.col(year_column)
            .cast("string"),

            F.lpad(
                F.col(month_column)
                .cast("string"),
                2,
                "0"
            ),

            F.lit("01")
        )
    )


# ============================================================
# SOFT DELETE
# ============================================================

def clean_soft_deleted(df):

    if "DeletedAt" in df.columns:

        return df.filter(
            F.col("DeletedAt").isNull()
        )

    return df


# ============================================================
# HASH POSITIF
#
# Compatible UInt64 ClickHouse
# ============================================================

def positive_hash(*columns):

    return (
        F.pmod(
            F.xxhash64(*columns),
            F.lit(9223372036854775807)
        )
        .cast("long")
    )


# ============================================================
# VERIFIER COLONNES
# ============================================================

def get_existing_column(
    df,
    candidates
):

    for column in candidates:

        if column in df.columns:

            return column

    return None


# ============================================================
# LECTURE SQL SERVER
# ============================================================

def read_sqlserver_table(
    table_name,
    columns="*"
):

    logger.info(
        f"Lecture SQL Server : {table_name}"
    )

    query = f"""
    (
        SELECT {columns}
        FROM dbo.{table_name}
    ) AS source_table
    """

    df = (
        spark.read
        .format("jdbc")

        .option(
            "url",
            MSSQL_JDBC_URL
        )

        .option(
            "dbtable",
            query
        )

        .option(
            "user",
            SQLSERVER_USER
        )

        .option(
            "password",
            SQLSERVER_PASSWORD
        )

        .option(
            "driver",
            MSSQL_JDBC_DRIVER
        )

        .option(
            "fetchsize",
            "10000"
        )

        .load()
    )

    df = clean_soft_deleted(df)

    count = df.count()

    logger.info(
        f"{table_name} : {count} lignes"
    )

    return df


# ============================================================
# LECTURE CLICKHOUSE
# ============================================================

def read_clickhouse_table(
    table_name
):

    logger.info(
        f"Lecture ClickHouse : {table_name}"
    )

    return (
        spark.read
        .format("jdbc")

        .option(
            "url",
            CLICKHOUSE_JDBC_URL
        )

        .option(
            "dbtable",
            f"{CLICKHOUSE_DATABASE}.{table_name}"
        )

        .option(
            "driver",
            CLICKHOUSE_JDBC_DRIVER
        )

        .option(
            "user",
            CLICKHOUSE_USER
        )

        .option(
            "password",
            CLICKHOUSE_PASSWORD
        )

        .load()
    )


# ============================================================
# ANTI-DOUBLONS
# ============================================================

def remove_existing_records(
    source_df,
    table_name,
    key_columns
):

    source_count = source_df.count()

    if source_count == 0:

        logger.warning(
            f"{table_name} : source vide."
        )

        return source_df

    logger.info(
        f"{table_name} : anti-doublons..."
    )

    target_df = (
        read_clickhouse_table(
            table_name
        )

        .select(
            *key_columns
        )

        .dropDuplicates(
            key_columns
        )
    )

    new_df = (
        source_df

        .dropDuplicates(
            key_columns
        )

        .join(
            target_df,
            on=key_columns,
            how="left_anti"
        )
    )

    new_count = new_df.count()

    logger.info(
        f"{table_name} : "
        f"{source_count - new_count} doublons ignorés."
    )

    logger.info(
        f"{table_name} : "
        f"{new_count} nouvelles lignes."
    )

    return new_df


# ============================================================
# INSERT CLICKHOUSE
# ============================================================

def write_clickhouse(
    df,
    table_name,
    key_columns
):

    if df is None:

        logger.warning(
            f"{table_name} : DataFrame NULL."
        )

        return

    source_count = df.count()

    if source_count == 0:

        logger.info(
            f"{table_name} : aucune donnée à insérer."
        )

        return

    df_to_insert = (
        remove_existing_records(
            df,
            table_name,
            key_columns
        )
    )

    insert_count = df_to_insert.count()

    if insert_count == 0:

        logger.info(
            f"{table_name} : "
            "aucune nouvelle ligne."
        )

        return

    logger.info(
        f"Insertion {table_name} : "
        f"{insert_count} lignes"
    )

    (
        df_to_insert.write
        .format("jdbc")

        .option(
            "url",
            CLICKHOUSE_JDBC_URL
        )

        .option(
            "dbtable",
            f"{CLICKHOUSE_DATABASE}.{table_name}"
        )

        .option(
            "driver",
            CLICKHOUSE_JDBC_DRIVER
        )

        .option(
            "user",
            CLICKHOUSE_USER
        )

        .option(
            "password",
            CLICKHOUSE_PASSWORD
        )

        .option(
            "batchsize",
            "5000"
        )

        .mode(
            "append"
        )

        .save()
    )

    logger.info(
        f"{table_name} : insertion terminée."
    )


# ============================================================
# DIM CLIENTS
# ============================================================

def build_dim_clients(
    companies
):

    name_column = get_existing_column(
        companies,
        [
            "CompanyName",
            "Name",
            "LegalName"
        ]
    )

    if name_column is None:

        name_expr = F.lit("")

    else:

        name_expr = F.coalesce(
            F.col(name_column),
            F.lit("")
        )

    return (
        companies

        .select(

            F.col("Id")
            .cast("int")
            .alias("client_id"),

            name_expr
            .cast("string")
            .alias("client_name"),

            F.col("Id")
            .cast("string")
            .alias("client_code"),

            F.lit(1)
            .cast("int")
            .alias("is_active")
        )

        .filter(
            F.col("client_id").isNotNull()
        )

        .dropDuplicates(
            ["client_id"]
        )
    )


# ============================================================
# DIM EMPLOYEES
# ============================================================

def build_dim_employees(
    employees
):

    birth_column = get_existing_column(
        employees,
        [
            "DateOfBirth",
            "BirthDate"
        ]
    )

    hire_column = get_existing_column(
        employees,
        [
            "HireDate",
            "HiringDate",
            "StartDate"
        ]
    )

    gender_column = get_existing_column(
        employees,
        [
            "Gender",
            "Sex"
        ]
    )

    department_column = get_existing_column(
        employees,
        [
            "Department",
            "DepartmentName"
        ]
    )

    position_column = get_existing_column(
        employees,
        [
            "JobPosition",
            "Position",
            "JobTitle"
        ]
    )

    contract_column = get_existing_column(
        employees,
        [
            "ContractType",
            "Contract"
        ]
    )

    first_name = (
        F.col("FirstName")
        if "FirstName" in employees.columns
        else F.lit("")
    )

    last_name = (
        F.col("LastName")
        if "LastName" in employees.columns
        else F.lit("")
    )

    return (
        employees

        .select(

            F.col("Id")
            .cast("int")
            .alias("employee_id"),

            F.coalesce(
                F.col("CompanyId"),
                F.lit(0)
            )
            .cast("int")
            .alias("client_id"),

            F.col("Id")
            .cast("string")
            .alias("employee_code"),

            F.coalesce(
                first_name,
                F.lit("")
            )
            .cast("string")
            .alias("first_name"),

            F.coalesce(
                last_name,
                F.lit("")
            )
            .cast("string")
            .alias("last_name"),

            F.concat_ws(
                " ",
                F.coalesce(first_name, F.lit("")),
                F.coalesce(last_name, F.lit(""))
            )
            .cast("string")
            .alias("employee_name"),

            (
                F.coalesce(
                    F.col(gender_column),
                    F.lit("")
                )
                if gender_column
                else F.lit("")
            )
            .cast("string")
            .alias("gender"),

            (
                F.coalesce(
                    F.to_date(
                        F.col(birth_column)
                    ),
                    F.lit("1900-01-01").cast("date")
                )
                if birth_column
                else F.lit("1900-01-01").cast("date")
            )
            .alias("birth_date"),

            (
                F.coalesce(
                    F.to_date(
                        F.col(hire_column)
                    ),
                    F.lit("1900-01-01").cast("date")
                )
                if hire_column
                else F.lit("1900-01-01").cast("date")
            )
            .alias("hire_date"),

            (
                F.coalesce(
                    F.col(contract_column),
                    F.lit("")
                )
                if contract_column
                else F.lit("")
            )
            .cast("string")
            .alias("contract_type"),

            (
                F.coalesce(
                    F.col(department_column),
                    F.lit("")
                )
                if department_column
                else F.lit("")
            )
            .cast("string")
            .alias("department"),

            (
                F.coalesce(
                    F.col(position_column),
                    F.lit("")
                )
                if position_column
                else F.lit("")
            )
            .cast("string")
            .alias("job_position"),

            F.lit(1)
            .cast("int")
            .alias("is_active")
        )

        .filter(
            F.col("employee_id").isNotNull()
        )

        .dropDuplicates(
            ["employee_id"]
        )
    )


# ============================================================
# DIM CALENDAR
# ============================================================

def build_dim_calendar(
    payroll_results
):

    dates = (
        payroll_results

        .select(
            sql_month_date(
                "Year",
                "Month"
            )
            .alias("date_id")
        )

        .filter(
            F.col("date_id").isNotNull()
        )

        .distinct()
    )

    return (
        dates

        .select(

            F.col("date_id"),

            F.year("date_id")
            .cast("int")
            .alias("year"),

            F.month("date_id")
            .cast("int")
            .alias("month"),

            F.date_format(
                "date_id",
                "MMMM"
            )
            .alias("month_name"),

            F.quarter("date_id")
            .cast("int")
            .alias("quarter"),

            F.dayofmonth("date_id")
            .cast("int")
            .alias("day")
        )

        .dropDuplicates(
            ["date_id"]
        )
    )


# ============================================================
# DIM COMPOSANT PAIE
#
# Ajout du composant technique 0 :
#
# TOTAL_BRUT_AGREGE
#
# Ce composant représente PayrollResults.TotalBrut
# quand aucune vraie ligne détaillée liée à PayComponents
# n'est disponible.
# ============================================================

def build_dim_composant_paie(
    pay_components
):

    technical_component = (
        spark.createDataFrame(
            [
                (
                    0,
                    "TOTAL_BRUT_AGREGE",
                    "Total brut agrégé",
                    "AGREGAT",
                    0,
                    0,
                    0,
                    1
                )
            ],
            [
                "composant_id",
                "code",
                "nom",
                "type",
                "is_taxable",
                "is_social",
                "is_cimr",
                "is_active"
            ]
        )
    )

    # ========================================================
    # PROTECTION : PayComponents peut être vide
    #
    # On conserve exactement le même principe :
    # le composant technique 0 reste présent.
    # ========================================================

    if pay_components is None:

        return technical_component

    source_count = pay_components.limit(1).count()

    if source_count == 0:

        logger.warning(
            "PayComponents est vide. "
            "Seul TOTAL_BRUT_AGREGE sera utilisé."
        )

        return technical_component

    source_components = (
        pay_components

        .select(

            F.col("Id")
            .cast("int")
            .alias("composant_id"),

            F.coalesce(
                F.col("Code"),
                F.lit("")
            )
            .cast("string")
            .alias("code"),

            F.coalesce(
                F.col("NameFr"),
                F.col("Name")
                if "Name" in pay_components.columns
                else F.lit(""),
                F.lit("")
            )
            .cast("string")
            .alias("nom"),

            (
                F.coalesce(
                    F.col("Type"),
                    F.lit("")
                )
                if "Type" in pay_components.columns
                else F.lit("")
            )
            .cast("string")
            .alias("type"),

            (
                F.coalesce(
                    F.col("IsTaxable"),
                    F.lit(False)
                )
                if "IsTaxable" in pay_components.columns
                else F.lit(False)
            )
            .cast("int")
            .alias("is_taxable"),

            (
                F.coalesce(
                    F.col("IsSocial"),
                    F.lit(False)
                )
                if "IsSocial" in pay_components.columns
                else F.lit(False)
            )
            .cast("int")
            .alias("is_social"),

            (
                F.coalesce(
                    F.col("IsCIMR"),
                    F.lit(False)
                )
                if "IsCIMR" in pay_components.columns
                else F.lit(False)
            )
            .cast("int")
            .alias("is_cimr"),

            (
                F.coalesce(
                    F.col("IsActive"),
                    F.lit(True)
                )
                if "IsActive" in pay_components.columns
                else F.lit(True)
            )
            .cast("int")
            .alias("is_active")
        )

        .filter(
            F.col("composant_id").isNotNull()
        )
    )

    return (
        technical_component

        .unionByName(
            source_components
        )

        .dropDuplicates(
            ["composant_id"]
        )
    )


# ============================================================
# DIM ECHEANCE LEGALE
#
# Pas de données inventées.
# ============================================================

def build_dim_echeance_legale():

    schema = StructType([

        StructField(
            "echeance_id",
            IntegerType(),
            False
        ),

        StructField(
            "libelle",
            StringType(),
            False
        ),

        StructField(
            "date_echeance",
            DateType(),
            False
        ),

        StructField(
            "type_echeance",
            StringType(),
            False
        )
    ])

    return spark.createDataFrame(
        [],
        schema
    )


# ============================================================
# FACT BULLETIN LIGNE
#
# GRAIN :
#
# 1 employé
# 1 mois
# 1 agrégat TotalBrut
#
# composant_id = 0
# → TOTAL_BRUT_AGREGE
# ============================================================

def build_fact_bulletin_ligne(
    payroll_results
):

    return (
        payroll_results

        .select(

            F.col("CompanyId")
            .cast("int")
            .alias("client_id"),

            F.col("EmployeeId")
            .cast("int")
            .alias("employee_id"),

            sql_month_date(
                "Year",
                "Month"
            )
            .alias("date_paie"),

            F.lit(0)
            .cast("int")
            .alias("composant_id"),

            F.coalesce(
                F.col("TotalBrut"),
                F.lit(0)
            )
            .cast("decimal(18,2)")
            .alias("montant"),

            F.lit(1)
            .cast("decimal(18,4)")
            .alias("quantite"),

            F.lit(0)
            .cast("decimal(18,6)")
            .alias("taux")
        )

        .filter(

            F.col("client_id").isNotNull()

            &

            F.col("employee_id").isNotNull()

            &

            F.col("date_paie").isNotNull()
        )

        .dropDuplicates([
            "client_id",
            "employee_id",
            "date_paie",
            "composant_id"
        ])
    )


# ============================================================
# FACT ABSENCE
# ============================================================

def build_fact_absence(
    employee_absences,
    employees
):

    date_column = get_existing_column(
        employee_absences,
        [
            "AbsenceDate",
            "Date",
            "StartDate"
        ]
    )

    if date_column is None:

        logger.warning(
            "EmployeeAbsences : "
            "aucune colonne date reconnue."
        )

        return None

    employee_client = (
        employees

        .select(

            F.col("Id")
            .cast("int")
            .alias("employee_id"),

            F.col("CompanyId")
            .cast("int")
            .alias("client_id")
        )

        .dropDuplicates(
            ["employee_id"]
        )
    )

    absence_type_column = get_existing_column(
        employee_absences,
        [
            "AbsenceType",
            "Type",
            "Reason"
        ]
    )

    duration_column = get_existing_column(
        employee_absences,
        [
            "Duration",
            "Days",
            "NbDays",
            "NumberOfDays"
        ]
    )

    status_column = get_existing_column(
        employee_absences,
        [
            "Status",
            "Statut"
        ]
    )

    absences = (
        employee_absences

        .select(

            F.col("EmployeeId")
            .cast("int")
            .alias("employee_id"),

            F.to_date(
                F.col(date_column)
            )
            .alias("date_absence"),

            (
                F.coalesce(
                    F.col(absence_type_column),
                    F.lit("ABSENCE")
                )
                if absence_type_column
                else F.lit("ABSENCE")
            )
            .cast("string")
            .alias("absence_type"),

            (
                F.coalesce(
                    F.col(duration_column),
                    F.lit(1)
                )
                if duration_column
                else F.lit(1)
            )
            .cast("decimal(10,2)")
            .alias("duree"),

            (
                F.coalesce(
                    F.col(status_column),
                    F.lit("")
                )
                if status_column
                else F.lit("")
            )
            .cast("string")
            .alias("statut")
        )
    )

    return (
        absences

        .join(
            employee_client,
            on="employee_id",
            how="left"
        )

        .select(

            F.coalesce(
                F.col("client_id"),
                F.lit(0)
            )
            .cast("int")
            .alias("client_id"),

            F.col("employee_id"),

            F.col("date_absence"),

            F.col("absence_type"),

            F.col("duree"),

            F.col("statut")
        )

        .filter(

            F.col("employee_id").isNotNull()

            &

            F.col("date_absence").isNotNull()
        )

        .dropDuplicates([
            "client_id",
            "employee_id",
            "date_absence",
            "absence_type"
        ])
    )


# ============================================================
# FACT VARIABLE COLLECTE
#
# VARIABLES DIRECTES DE PayrollResults
#
# IMPORTANT :
#
# On garde uniquement les valeurs différentes de 0.
# ============================================================

def build_fact_variable_collecte(
    payroll_results
):

    logger.info(
        "Construction fact_variable_collecte "
        "depuis PayrollResults..."
    )

    variable_columns = [

        (
            "HeuresSupp25",
            "HEURES_SUPP_25",
            "Heures supplémentaires 25%",
            "heures"
        ),

        (
            "HeuresSupp50",
            "HEURES_SUPP_50",
            "Heures supplémentaires 50%",
            "heures"
        ),

        (
            "HeuresSupp100",
            "HEURES_SUPP_100",
            "Heures supplémentaires 100%",
            "heures"
        ),

        (
            "Conges",
            "CONGES",
            "Congés",
            "jours"
        ),

        (
            "JoursFeries",
            "JOURS_FERIES",
            "Jours fériés",
            "jours"
        ),

        (
            "PrimeAnciennete",
            "PRIME_ANCIENNETE",
            "Prime ancienneté",
            "montant"
        ),

        (
            "PrimeImposable1",
            "PRIME_IMPOSABLE_1",
            "Prime imposable 1",
            "montant"
        ),

        (
            "PrimeImposable2",
            "PRIME_IMPOSABLE_2",
            "Prime imposable 2",
            "montant"
        ),

        (
            "PrimeImposable3",
            "PRIME_IMPOSABLE_3",
            "Prime imposable 3",
            "montant"
        ),

        (
            "FraisProfessionnels",
            "FRAIS_PROFESSIONNELS",
            "Frais professionnels",
            "montant"
        ),

        (
            "IndemniteRepresentation",
            "INDEMNITE_REPRESENTATION",
            "Indemnité représentation",
            "montant"
        ),

        (
            "PrimeTransport",
            "PRIME_TRANSPORT",
            "Prime transport",
            "montant"
        ),

        (
            "PrimePanier",
            "PRIME_PANIER",
            "Prime panier",
            "montant"
        ),

        (
            "IndemniteDeplacement",
            "INDEMNITE_DEPLACEMENT",
            "Indemnité déplacement",
            "montant"
        ),

        (
            "IndemniteCaisse",
            "INDEMNITE_CAISSE",
            "Indemnité caisse",
            "montant"
        ),

        (
            "PrimeSalissure",
            "PRIME_SALISSURE",
            "Prime salissure",
            "montant"
        ),

        (
            "GratificationsFamilial",
            "GRATIFICATION_FAMILIALE",
            "Gratification familiale",
            "montant"
        ),

        (
            "PrimeVoyageMecque",
            "PRIME_VOYAGE_MECQUE",
            "Prime voyage Mecque",
            "montant"
        ),

        (
            "IndemniteLicenciement",
            "INDEMNITE_LICENCIEMENT",
            "Indemnité licenciement",
            "montant"
        ),

        (
            "IndemniteKilometrique",
            "INDEMNITE_KILOMETRIQUE",
            "Indemnité kilométrique",
            "montant"
        ),

        (
            "PrimeTourne",
            "PRIME_TOURNE",
            "Prime tournée",
            "montant"
        ),

        (
            "PrimeOutillage",
            "PRIME_OUTILLAGE",
            "Prime outillage",
            "montant"
        ),

        (
            "AideMedicale",
            "AIDE_MEDICALE",
            "Aide médicale",
            "montant"
        ),

        (
            "AutresPrimesNonImposable",
            "AUTRES_PRIMES_NON_IMPOSABLES",
            "Autres primes non imposables",
            "montant"
        ),

        (
            "AvanceSurSalaire",
            "AVANCE_SUR_SALAIRE",
            "Avance sur salaire",
            "montant"
        ),

        (
            "InteretSurLogement",
            "INTERET_SUR_LOGEMENT",
            "Intérêt sur logement",
            "montant"
        )
    ]

    existing_variables = [

        variable

        for variable in variable_columns

        if variable[0]
        in payroll_results.columns
    ]

    if not existing_variables:

        logger.warning(
            "Aucune colonne variable "
            "trouvée dans PayrollResults."
        )

        return None

    base = (
        payroll_results

        .select(

            F.col("Id")
            .cast("int")
            .alias("payroll_result_id"),

            F.col("CompanyId")
            .cast("int")
            .alias("client_id"),

            F.col("EmployeeId")
            .cast("int")
            .alias("employee_id"),

            sql_month_date(
                "Year",
                "Month"
            )
            .alias("date_collecte"),

            F.col("Month")
            .cast("int")
            .alias("mois"),

            F.col("Year")
            .cast("int")
            .alias("annee"),

            *[
                F.col(source_column)
                .cast("decimal(18,4)")
                .alias(source_column)

                for (
                    source_column,
                    variable_code,
                    variable_name,
                    unite
                )

                in existing_variables
            ]
        )

        .filter(

            F.col("client_id").isNotNull()

            &

            F.col("employee_id").isNotNull()

            &

            F.col("date_collecte").isNotNull()
        )
    )

    variable_dfs = []

    for index, (

        source_column,
        variable_code,
        variable_name,
        unite

    ) in enumerate(
        existing_variables,
        start=1
    ):

        variable_df = (
            base

            .select(

                F.col("client_id"),

                F.col("employee_id"),

                F.col("date_collecte"),

                F.col("payroll_result_id"),

                F.lit(index)
                .cast("int")
                .alias("variable_id"),

                F.lit(variable_code)
                .alias("variable_code"),

                F.lit(variable_name)
                .alias("variable_name"),

                F.lit("PayrollResults")
                .alias("variable_source"),

                F.coalesce(
                    F.col(source_column),
                    F.lit(0)
                )
                .cast("decimal(18,4)")
                .alias("valeur"),

                F.lit(unite)
                .alias("unite"),

                F.col("mois"),

                F.col("annee")
            )

            .filter(
                F.col("valeur") != F.lit(0)
            )
        )

        variable_dfs.append(
            variable_df
        )

    if not variable_dfs:

        return None

    fact = reduce(
        lambda left, right:
        left.unionByName(right),
        variable_dfs
    )

    return (
        fact

        .withColumn(
            "variable_collecte_id",
            positive_hash(
                "client_id",
                "employee_id",
                "date_collecte",
                "payroll_result_id",
                "variable_id"
            )
        )

        .select(

            "variable_collecte_id",
            "client_id",
            "employee_id",
            "date_collecte",
            "payroll_result_id",
            "variable_id",
            "variable_code",
            "variable_name",
            "variable_source",
            "valeur",
            "unite",
            "mois",
            "annee"
        )

        .dropDuplicates([

            "client_id",
            "employee_id",
            "date_collecte",
            "payroll_result_id",
            "variable_id"
        ])
    )


# ============================================================
# FACT VARIABLE COLLECTE
#
# PRIMES DETAILLEES
#
# SOURCE :
# PayrollResultPrimes
# ============================================================

def build_fact_variable_primes(
    payroll_results,
    payroll_result_primes
):

    logger.info(
        "Construction fact_variable_collecte "
        "depuis PayrollResultPrimes..."
    )

    required_columns = [
        "Id",
        "PayrollResultId"
    ]

    for column in required_columns:

        if column not in payroll_result_primes.columns:

            logger.warning(
                f"PayrollResultPrimes : "
                f"colonne absente {column}"
            )

            return None

    base = (
        payroll_results

        .select(

            F.col("Id")
            .cast("int")
            .alias("payroll_result_id"),

            F.col("CompanyId")
            .cast("int")
            .alias("client_id"),

            F.col("EmployeeId")
            .cast("int")
            .alias("employee_id"),

            sql_month_date(
                "Year",
                "Month"
            )
            .alias("date_collecte"),

            F.col("Month")
            .cast("int")
            .alias("mois"),

            F.col("Year")
            .cast("int")
            .alias("annee")
        )
    )

    label_column = get_existing_column(
        payroll_result_primes,
        [
            "Label",
            "Name",
            "Libelle"
        ]
    )

    amount_column = get_existing_column(
        payroll_result_primes,
        [
            "Montant",
            "Amount",
            "Value"
        ]
    )

    if amount_column is None:

        logger.warning(
            "PayrollResultPrimes : "
            "aucune colonne montant reconnue."
        )

        return None

    primes = (
        payroll_result_primes

        .select(

            F.col("Id")
            .cast("int")
            .alias("prime_id"),

            F.col("PayrollResultId")
            .cast("int")
            .alias("payroll_result_id"),

            (
                F.coalesce(
                    F.col(label_column),
                    F.lit("Prime")
                )

                if label_column

                else F.lit("Prime")
            )
            .cast("string")
            .alias("variable_name"),

            F.coalesce(
                F.col(amount_column),
                F.lit(0)
            )
            .cast("decimal(18,4)")
            .alias("valeur")
        )
    )

    fact = (
        base

        .join(
            primes,
            on="payroll_result_id",
            how="inner"
        )

        .select(

            F.col("client_id"),

            F.col("employee_id"),

            F.col("date_collecte"),

            F.col("payroll_result_id"),

            (
                F.lit(1000000)
                +
                F.col("prime_id")
            )
            .cast("int")
            .alias("variable_id"),

            F.concat(
                F.lit("PRIME_"),
                F.col("prime_id")
                .cast("string")
            )
            .alias("variable_code"),

            F.col("variable_name"),

            F.lit("PayrollResultPrimes")
            .alias("variable_source"),

            F.col("valeur"),

            F.lit("montant")
            .alias("unite"),

            F.col("mois"),

            F.col("annee")
        )

        .filter(

            F.col("client_id").isNotNull()

            &

            F.col("employee_id").isNotNull()

            &

            F.col("date_collecte").isNotNull()

            &

            (F.col("valeur") != F.lit(0))
        )
    )

    return (
        fact

        .withColumn(
            "variable_collecte_id",
            positive_hash(
                "client_id",
                "employee_id",
                "date_collecte",
                "payroll_result_id",
                "variable_id"
            )
        )

        .select(

            "variable_collecte_id",
            "client_id",
            "employee_id",
            "date_collecte",
            "payroll_result_id",
            "variable_id",
            "variable_code",
            "variable_name",
            "variable_source",
            "valeur",
            "unite",
            "mois",
            "annee"
        )

        .dropDuplicates([

            "client_id",
            "employee_id",
            "date_collecte",
            "payroll_result_id",
            "variable_id"
        ])
    )


# ============================================================
# VALIDATION RELATIONS
# ============================================================

def validate_relationships(

    dim_clients,
    dim_employees,
    dim_calendar,
    dim_composant_paie,
    fact_bulletin_ligne,
    fact_absence,
    fact_variable_collecte
):

    logger.info("=" * 80)

    logger.info(
        "CONTROLE DES RELATIONS"
    )

    logger.info("=" * 80)

    # --------------------------------------------------------
    # EMPLOYEES -> CLIENTS
    # --------------------------------------------------------

    invalid_employees = (

        dim_employees

        .join(

            dim_clients.select(
                "client_id"
            ),

            on="client_id",

            how="left_anti"
        )

        .count()
    )

    logger.info(
        f"Employés sans client : "
        f"{invalid_employees}"
    )

    # --------------------------------------------------------
    # BULLETIN -> EMPLOYEES
    # --------------------------------------------------------

    invalid_bulletin_employee = (

        fact_bulletin_ligne

        .join(

            dim_employees.select(
                "employee_id"
            ),

            on="employee_id",

            how="left_anti"
        )

        .count()
    )

    logger.info(
        f"Bulletins sans employé : "
        f"{invalid_bulletin_employee}"
    )

    # --------------------------------------------------------
    # BULLETIN -> COMPOSANTS
    # --------------------------------------------------------

    invalid_bulletin_component = (

        fact_bulletin_ligne

        .join(

            dim_composant_paie.select(
                "composant_id"
            ),

            on="composant_id",

            how="left_anti"
        )

        .count()
    )

    logger.info(
        f"Bulletins sans composant : "
        f"{invalid_bulletin_component}"
    )

    # --------------------------------------------------------
    # BULLETIN -> CALENDAR
    # --------------------------------------------------------

    invalid_bulletin_date = (

        fact_bulletin_ligne

        .join(

            dim_calendar.select(
                F.col("date_id")
                .alias("date_paie")
            ),

            on="date_paie",

            how="left_anti"
        )

        .count()
    )

    logger.info(
        f"Bulletins sans date calendrier : "
        f"{invalid_bulletin_date}"
    )

    # --------------------------------------------------------
    # ABSENCE -> EMPLOYEE
    # --------------------------------------------------------

    if fact_absence is not None:

        invalid_absence = (

            fact_absence

            .join(

                dim_employees.select(
                    "employee_id"
                ),

                on="employee_id",

                how="left_anti"
            )

            .count()
        )

        logger.info(
            f"Absences sans employé : "
            f"{invalid_absence}"
        )

    # --------------------------------------------------------
    # VARIABLES -> EMPLOYEE
    # --------------------------------------------------------

    if fact_variable_collecte is not None:

        invalid_variables_employee = (

            fact_variable_collecte

            .join(

                dim_employees.select(
                    "employee_id"
                ),

                on="employee_id",

                how="left_anti"
            )

            .count()
        )

        logger.info(
            f"Variables sans employé : "
            f"{invalid_variables_employee}"
        )

        # ----------------------------------------------------
        # VARIABLES -> CLIENT
        # ----------------------------------------------------

        invalid_variables_client = (

            fact_variable_collecte

            .join(

                dim_clients.select(
                    "client_id"
                ),

                on="client_id",

                how="left_anti"
            )

            .count()
        )

        logger.info(
            f"Variables sans client : "
            f"{invalid_variables_client}"
        )

    logger.info(
        "CONTROLE DES RELATIONS TERMINE"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info("=" * 80)

    logger.info(
        "PAYZEN HR ETL V4 — DEMARRAGE"
    )

    logger.info("=" * 80)

    ensure_sqlserver_credentials()

    # ========================================================
    # EXTRACTION
    # ========================================================

    logger.info(
        "EXTRACTION SQL SERVER"
    )

    companies = read_sqlserver_table(
        "Companies"
    )

    employees = read_sqlserver_table(
        "Employees"
    )

    pay_components = read_sqlserver_table(
        "PayComponents"
    )

    payroll_results = read_sqlserver_table(
        "PayrollResults"
    )

    payroll_result_primes = read_sqlserver_table(
        "PayrollResultPrimes"
    )

    employee_absences = read_sqlserver_table(
        "EmployeeAbsences"
    )

    # ========================================================
    # DIMENSIONS
    # ========================================================

    logger.info(
        "CONSTRUCTION DIMENSIONS"
    )

    dim_clients = (
        build_dim_clients(
            companies
        )
    )

    dim_employees = (
        build_dim_employees(
            employees
        )
    )

    dim_calendar = (
        build_dim_calendar(
            payroll_results
        )
    )

    dim_composant_paie = (
        build_dim_composant_paie(
            pay_components
        )
    )

    dim_echeance_legale = (
        build_dim_echeance_legale()
    )

    # ========================================================
    # FACT BULLETIN
    # ========================================================

    fact_bulletin_ligne = (
        build_fact_bulletin_ligne(
            payroll_results
        )
    )

    # ========================================================
    # FACT ABSENCE
    # ========================================================

    fact_absence = (
        build_fact_absence(
            employee_absences,
            employees
        )
    )

    # ========================================================
    # FACT VARIABLES DIRECTES
    # ========================================================

    fact_variable_directe = (
        build_fact_variable_collecte(
            payroll_results
        )
    )

    # ========================================================
    # FACT VARIABLES PRIMES
    # ========================================================

    fact_variable_primes = (
        build_fact_variable_primes(
            payroll_results,
            payroll_result_primes
        )
    )

    # ========================================================
    # UNION VARIABLES
    # ========================================================

    if (
        fact_variable_directe is not None
        and
        fact_variable_primes is not None
    ):

        fact_variable_collecte = (
            fact_variable_directe
            .unionByName(
                fact_variable_primes
            )
        )

    elif fact_variable_directe is not None:

        fact_variable_collecte = (
            fact_variable_directe
        )

    elif fact_variable_primes is not None:

        fact_variable_collecte = (
            fact_variable_primes
        )

    else:

        fact_variable_collecte = None

    if fact_variable_collecte is not None:

        fact_variable_collecte = (
            fact_variable_collecte
            .dropDuplicates([
                "client_id",
                "employee_id",
                "date_collecte",
                "payroll_result_id",
                "variable_id"
            ])
        )

    # ========================================================
    # CONTROLE DATASETS
    # ========================================================

    logger.info("=" * 80)

    logger.info(
        "CONTROLE DES DATASETS"
    )

    logger.info("=" * 80)

    datasets_for_control = {

        "dim_clients":
            dim_clients,

        "dim_employees":
            dim_employees,

        "dim_calendar":
            dim_calendar,

        "dim_composant_paie":
            dim_composant_paie,

        "dim_echeance_legale":
            dim_echeance_legale,

        "fact_bulletin_ligne":
            fact_bulletin_ligne,

        "fact_absence":
            fact_absence,

        "fact_variable_collecte":
            fact_variable_collecte
    }

    for name, df in (
        datasets_for_control.items()
    ):

        if df is None:

            logger.warning(
                f"{name} = NULL"
            )

        else:

            logger.info(
                f"{name} = "
                f"{df.count()} lignes"
            )

    # ========================================================
    # APERCU VARIABLES
    # ========================================================

    if fact_variable_collecte is not None:

        logger.info("=" * 80)

        logger.info(
            "APERCU FACT_VARIABLE_COLLECTE"
        )

        logger.info("=" * 80)

        (
            fact_variable_collecte

            .groupBy(

                "variable_source",

                "variable_code",

                "variable_name",

                "unite"
            )

            .agg(

                F.count("*")
                .alias("nb_lignes"),

                F.sum("valeur")
                .alias("total_valeur")
            )

            .orderBy(
                F.desc("nb_lignes")
            )

            .show(
                100,
                truncate=False
            )
        )

    # ========================================================
    # VALIDATION RELATIONS
    # ========================================================

    validate_relationships(

        dim_clients,

        dim_employees,

        dim_calendar,

        dim_composant_paie,

        fact_bulletin_ligne,

        fact_absence,

        fact_variable_collecte
    )

    # ========================================================
    # CHARGEMENT CLICKHOUSE
    # ========================================================

    logger.info("=" * 80)

    logger.info(
        "CHARGEMENT CLICKHOUSE"
    )

    logger.info("=" * 80)

    # --------------------------------------------------------
    # DIM CLIENTS
    # --------------------------------------------------------

    write_clickhouse(

        dim_clients,

        "dim_clients",

        [
            "client_id"
        ]
    )

    # --------------------------------------------------------
    # DIM EMPLOYEES
    # --------------------------------------------------------

    write_clickhouse(

        dim_employees,

        "dim_employees",

        [
            "employee_id"
        ]
    )

    # --------------------------------------------------------
    # DIM CALENDAR
    # --------------------------------------------------------

    write_clickhouse(

        dim_calendar,

        "dim_calendar",

        [
            "date_id"
        ]
    )

    # --------------------------------------------------------
    # DIM COMPOSANT PAIE
    # --------------------------------------------------------

    write_clickhouse(

        dim_composant_paie,

        "dim_composant_paie",

        [
            "composant_id"
        ]
    )

    # --------------------------------------------------------
    # DIM ECHEANCE LEGALE
    # --------------------------------------------------------

    write_clickhouse(

        dim_echeance_legale,

        "dim_echeance_legale",

        [
            "echeance_id"
        ]
    )

    # --------------------------------------------------------
    # FACT BULLETIN
    # --------------------------------------------------------

    write_clickhouse(

        fact_bulletin_ligne,

        "fact_bulletin_ligne",

        [

            "client_id",

            "employee_id",

            "date_paie",

            "composant_id"
        ]
    )

    # --------------------------------------------------------
    # FACT ABSENCE
    # --------------------------------------------------------

    if fact_absence is not None:

        write_clickhouse(

            fact_absence,

            "fact_absence",

            [

                "client_id",

                "employee_id",

                "date_absence",

                "absence_type"
            ]
        )

    # --------------------------------------------------------
    # FACT VARIABLE COLLECTE
    # --------------------------------------------------------

    if fact_variable_collecte is not None:

        write_clickhouse(

            fact_variable_collecte,

            "fact_variable_collecte",

            [

                "client_id",

                "employee_id",

                "date_collecte",

                "payroll_result_id",

                "variable_id"
            ]
        )

    # ========================================================
    # FIN
    # ========================================================

    logger.info("=" * 80)

    logger.info(
        "PAYZEN HR ETL V4 — TERMINE AVEC SUCCES"
    )

    logger.info("=" * 80)


# ============================================================
# EXECUTION
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except Exception:

        logger.exception(
            "ERREUR FATALE ETL"
        )

        sys.exit(1)

    finally:

        try:

            spark.stop()

        except Exception:

            pass