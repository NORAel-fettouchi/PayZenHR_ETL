# ============================================================
# PayZenHR ETL
# SQL Server -> PySpark -> ClickHouse
#
# Version : V3.5 - Corrected / Worker Crash Safe
#
# IMPORTANT :
# - Python 3.11 recommandé avec PySpark 3.5.x
# - Aucun Python UDF
# - Fonctions natives Spark uniquement
# - Mode local Windows
# ============================================================

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_DIR = r"E:\etape_1\PayZenHR_ETL"

JDBC_DIR = os.path.join(PROJECT_DIR, "jars")

MSSQL_JAR = os.path.join(
    JDBC_DIR,
    "mssql-jdbc-13.4.0.jre8.jar"
)

CLICKHOUSE_JAR = os.path.join(
    JDBC_DIR,
    "clickhouse-jdbc-0.9.8-all.jar"
)


# ============================================================
# SQL SERVER
# ============================================================

SQLSERVER_HOST = "localhost"
SQLSERVER_PORT = 1433
SQLSERVER_DATABASE = "PayZenHR_Source"

SQLSERVER_USER = "sa"
SQLSERVER_PASSWORD = "noraNORA123"

SQLSERVER_URL = (
    f"jdbc:sqlserver://"
    f"{SQLSERVER_HOST}:{SQLSERVER_PORT};"
    f"databaseName={SQLSERVER_DATABASE};"
    f"encrypt=false;"
    f"trustServerCertificate=true;"
)


SQLSERVER_PROPERTIES = {
    "user": SQLSERVER_USER,
    "password": SQLSERVER_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
}


# ============================================================
# CLICKHOUSE
# ============================================================

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "payzen_bi"

CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "1234"

CLICKHOUSE_URL = (
    f"jdbc:clickhouse://"
    f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    f"{CLICKHOUSE_DATABASE}"
)


# ============================================================
# ENTREPRISE PAYZENHR
# ============================================================

MANAGED_BY_COMPANY_ID = 30


# ============================================================
# LOGGING
# ============================================================

LOG_DIR = os.path.join(PROJECT_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("PayZenHR_ETL")


# ============================================================
# 2. SPARK SESSION
# ============================================================

def create_spark_session():

    logger.info("Création de la session Spark...")

    # --------------------------------------------------------
    # Même Python pour Driver + Worker
    # --------------------------------------------------------

    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    logger.info(
        f"Python utilisé par Spark : {python_executable}"
    )

    spark = (
        SparkSession.builder
        .appName("PayZenHR_ETL")
        .master("local[*]")

        # ----------------------------------------------------
        # JDBC JAR
        # ----------------------------------------------------

        .config(
            "spark.jars",
            ",".join([
                MSSQL_JAR,
                CLICKHOUSE_JAR
            ])
        )

        # ----------------------------------------------------
        # Mémoire
        # ----------------------------------------------------

        .config(
            "spark.driver.memory",
            "4g"
        )

        .config(
            "spark.executor.memory",
            "4g"
        )

        # ----------------------------------------------------
        # Spark memory
        # ----------------------------------------------------

        .config(
            "spark.memory.fraction",
            "0.6"
        )

        .config(
            "spark.memory.storageFraction",
            "0.3"
        )

        # ----------------------------------------------------
        # Shuffle
        # ----------------------------------------------------

        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        # ----------------------------------------------------
        # AQE
        # ----------------------------------------------------

        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )

        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        # ----------------------------------------------------
        # Codegen
        # ----------------------------------------------------

        .config(
            "spark.sql.codegen.wholeStage",
            "true"
        )

        # ----------------------------------------------------
        # Python
        # ----------------------------------------------------

        .config(
            "spark.pyspark.python",
            python_executable
        )

        .config(
            "spark.pyspark.driver.python",
            python_executable
        )

        # ----------------------------------------------------
        # Stabilité
        # ----------------------------------------------------

        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "false"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Spark version : {spark.version}"
    )

    return spark


# ============================================================
# 3. JDBC SQL SERVER
# ============================================================

def read_sqlserver_table(
    spark: SparkSession,
    table_name: str,
    columns: str = "*"
) -> DataFrame:

    logger.info(
        f"Lecture SQL Server : {table_name}"
    )

    query = f"""
        SELECT {columns}
        FROM dbo.{table_name}
    """

    df = (
        spark.read
        .format("jdbc")
        .option(
            "url",
            SQLSERVER_URL
        )
        .option(
            "query",
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
            "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        .option(
            "fetchsize",
            "10000"
        )
        .load()
    )

    logger.info(
        f"{table_name} chargé. Colonnes : {df.columns}"
    )

    return df


# ============================================================
# 4. CLICKHOUSE WRITE
# ============================================================

def write_clickhouse(
    df: DataFrame,
    table_name: str
):

    logger.info(
        f"Écriture ClickHouse : "
        f"{CLICKHOUSE_DATABASE}.{table_name}"
    )

    (
        df.write
        .format("jdbc")
        .option(
            "url",
            CLICKHOUSE_URL
        )
        .option(
            "dbtable",
            table_name
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
            "driver",
            "com.clickhouse.jdbc.ClickHouseDriver"
        )
        .option(
            "batchsize",
            "5000"
        )
        .option(
            "isolationLevel",
            "NONE"
        )
        .mode("overwrite")
        .save()
    )

    logger.info(
        f"Table ClickHouse chargée : {table_name}"
    )


# ============================================================
# 5. LOG DATAFRAME
# ============================================================

def log_dataframe_count(
    df: DataFrame,
    name: str
):

    try:

        count = df.count()

        logger.info(
            f"{name} : {count} lignes"
        )

        return count

    except Exception as e:

        logger.error(
            f"Erreur count() sur {name}"
        )

        logger.error(str(e))

        raise


# ============================================================
# 6. UTILITAIRES
# ============================================================

def column_exists(
    df: DataFrame,
    column_name: str
) -> bool:

    return column_name in df.columns


def safe_col(
    df: DataFrame,
    column_name: str,
    data_type=None
):

    if column_exists(
        df,
        column_name
    ):

        return F.col(column_name)

    if data_type is None:

        return F.lit(None)

    return F.lit(None).cast(data_type)


# ============================================================
# CLÉ TECHNIQUE
# ============================================================

def add_surrogate_key(
    df: DataFrame,
    key_name: str,
    natural_columns
):

    """
    Génération d'une clé technique stable à partir
    de colonnes naturelles.

    Pas de Window.
    Pas de Python UDF.
    Évite les warnings :

        WindowExec:
        No Partition Defined for Window operation
    """

    if not natural_columns:

        raise ValueError(
            f"{key_name} : natural_columns est vide."
        )

    expressions = []

    for column_name in natural_columns:

        expressions.append(
            F.coalesce(
                F.col(column_name).cast("string"),
                F.lit("")
            )
        )

    return df.withColumn(
        key_name,
        F.abs(
            F.xxhash64(*expressions)
        ).cast("long")
    )


# ============================================================
# 7. DIM CLIENTS
# ============================================================

def build_dim_clients(
    spark: SparkSession
):

    logger.info("=" * 70)
    logger.info("Construction dim_clients")
    logger.info("=" * 70)

    companies = read_sqlserver_table(
        spark,
        "Companies"
    )

    logger.info(
        f"Colonnes Companies : {companies.columns}"
    )

    # --------------------------------------------------------
    # Vérification
    # --------------------------------------------------------

    if "Id" not in companies.columns:

        raise ValueError(
            "Companies.Id est obligatoire."
        )

    # --------------------------------------------------------
    # Filtre ManagedByCompanyId
    # --------------------------------------------------------

    if "ManagedByCompanyId" in companies.columns:

        companies = companies.filter(
            F.col("ManagedByCompanyId")
            == F.lit(MANAGED_BY_COMPANY_ID)
        )

    # --------------------------------------------------------
    # IMPORTANT :
    #
    # Ton vrai schéma contient :
    #
    # CompanyName
    # IceNumber
    # IfNumber
    # RcNumber
    #
    # et non :
    #
    # Name
    # LegalName
    # ICE
    # IF
    # RC
    # --------------------------------------------------------

    dim = companies.select(

        F.col("Id")
        .cast("long")
        .alias("client_source_id"),

        safe_col(
            companies,
            "CompanyName",
            "string"
        )
        .cast("string")
        .alias("client_name"),

        safe_col(
            companies,
            "LegalForm",
            "string"
        )
        .cast("string")
        .alias("legal_name"),

        safe_col(
            companies,
            "IceNumber",
            "string"
        )
        .cast("string")
        .alias("ice"),

        safe_col(
            companies,
            "IfNumber",
            "string"
        )
        .cast("string")
        .alias("identifiant_fiscal"),

        safe_col(
            companies,
            "RcNumber",
            "string"
        )
        .cast("string")
        .alias("registre_commerce"),

        safe_col(
            companies,
            "ManagedByCompanyId",
            "long"
        )
        .cast("long")
        .alias("managed_by_company_id")
    )

    # --------------------------------------------------------
    # Suppression doublons
    # --------------------------------------------------------

    dim = dim.dropDuplicates(
        ["client_source_id"]
    )

    # --------------------------------------------------------
    # Clé technique
    # --------------------------------------------------------

    dim = add_surrogate_key(
        dim,
        "client_id",
        ["client_source_id"]
    )

    # --------------------------------------------------------
    # Ordre final
    # --------------------------------------------------------

    dim = dim.select(
        "client_id",
        "client_source_id",
        "client_name",
        "legal_name",
        "ice",
        "identifiant_fiscal",
        "registre_commerce",
        "managed_by_company_id"
    )

    return dim


# ============================================================
# 8. DIM EMPLOYEES
# ============================================================

def build_dim_employees(
    spark: SparkSession,
    managed_company_id=30
):

    logger.info("=" * 70)
    logger.info("Construction dim_employees")
    logger.info("=" * 70)

    # ========================================================
    # 1. LECTURE
    # ========================================================

    logger.info(
        "Lecture SQL Server : Employees"
    )

    employees = read_sqlserver_table(
        spark,
        "Employees"
    )

    logger.info(
        f"Employees colonnes : {employees.columns}"
    )

    logger.info(
        "Lecture SQL Server : EmployeeContracts"
    )

    contracts = read_sqlserver_table(
        spark,
        "EmployeeContracts"
    )

    logger.info(
        f"EmployeeContracts colonnes : "
        f"{contracts.columns}"
    )

    logger.info(
        "Lecture SQL Server : JobPositions"
    )

    job_positions = read_sqlserver_table(
        spark,
        "JobPositions"
    )

    logger.info(
        f"JobPositions colonnes : "
        f"{job_positions.columns}"
    )

    logger.info(
        "Lecture SQL Server : Departements"
    )

    departments = read_sqlserver_table(
        spark,
        "Departements"
    )

    logger.info(
        f"Departements colonnes : "
        f"{departments.columns}"
    )

    # ========================================================
    # 2. VÉRIFICATIONS
    # ========================================================

    required_employee_columns = [
        "Id",
        "CompanyId"
    ]

    for col_name in required_employee_columns:

        if col_name not in employees.columns:

            raise ValueError(
                f"Employees.{col_name} est obligatoire."
            )

    # ========================================================
    # 3. ALIAS
    # ========================================================

    e = employees.alias("e")
    c = contracts.alias("c")
    p = job_positions.alias("p")
    d = departments.alias("d")

    # ========================================================
    # 4. FILTRE ENTREPRISE
    # ========================================================

    e_filtered = e.filter(
        F.col("e.CompanyId")
        == F.lit(managed_company_id)
    )

    # ========================================================
    # 5. EMPLOYEES -> CONTRACTS
    # ========================================================

    if "EmployeeId" not in contracts.columns:

        logger.warning(
            "EmployeeContracts.EmployeeId "
            "absent : contrat ignoré."
        )

        joined = e_filtered

    else:

        joined = e_filtered.join(
            c,
            F.col("e.Id")
            == F.col("c.EmployeeId"),
            "left"
        )

    # ========================================================
    # 6. JOB POSITIONS
    # ========================================================

    if (
        "JobPositionId" in contracts.columns
        and "Id" in job_positions.columns
    ):

        joined = joined.join(
            p,
            F.col("c.JobPositionId")
            == F.col("p.Id"),
            "left"
        )

    # ========================================================
    # 7. DEPARTEMENTS
    # ========================================================

    if (
        "DepartementId" in employees.columns
        and "Id" in departments.columns
    ):

        joined = joined.join(
            d,
            F.col("e.DepartementId")
            == F.col("d.Id"),
            "left"
        )

    # ========================================================
    # 8. CONSTRUCTION
    # ========================================================

    result = joined.select(

        # ----------------------------------------------------
        # SOURCE ID
        # ----------------------------------------------------

        F.col("e.Id")
        .cast("long")
        .alias("employee_source_id"),

        # ----------------------------------------------------
        # ENTREPRISE
        # ----------------------------------------------------

        F.col("e.CompanyId")
        .cast("long")
        .alias("company_source_id"),

        # ----------------------------------------------------
        # INFORMATIONS
        # ----------------------------------------------------

        safe_col(
            employees,
            "FirstName",
            "string"
        ).alias("first_name"),

        safe_col(
            employees,
            "LastName",
            "string"
        ).alias("last_name"),

        safe_col(
            employees,
            "EmployeeNumber",
            "string"
        ).alias("employee_number"),

        # ----------------------------------------------------
        # DEPARTEMENT
        # ----------------------------------------------------

        safe_col(
            employees,
            "DepartementId",
            "long"
        )
        .cast("long")
        .alias("departement_id"),

        safe_col(
            departments,
            "DepartementName",
            "string"
        )
        .alias("departement_name"),

        # ----------------------------------------------------
        # POSTE
        # ----------------------------------------------------

        safe_col(
            contracts,
            "JobPositionId",
            "long"
        )
        .cast("long")
        .alias("job_position_id"),

        safe_col(
            job_positions,
            "Name",
            "string"
        )
        .alias("job_position_name"),

        # ----------------------------------------------------
        # CONTRAT
        # ----------------------------------------------------

        safe_col(
            contracts,
            "ContractTypeId",
            "long"
        )
        .cast("long")
        .alias("contract_type_id"),

        safe_col(
            contracts,
            "StartDate"
        ).alias("contract_start_date"),

        safe_col(
            contracts,
            "EndDate"
        ).alias("contract_end_date"),

        safe_col(
            contracts,
            "ExonerationEndDate"
        ).alias("exoneration_end_date"),

        # ----------------------------------------------------
        # MANAGER
        # ----------------------------------------------------

        safe_col(
            employees,
            "ManagerId",
            "long"
        )
        .cast("long")
        .alias("manager_id"),

        # ----------------------------------------------------
        # STATUT
        # ----------------------------------------------------

        safe_col(
            employees,
            "StatusId",
            "long"
        )
        .cast("long")
        .alias("status_id"),

        # ----------------------------------------------------
        # GENRE
        # ----------------------------------------------------

        safe_col(
            employees,
            "GenderId",
            "long"
        )
        .cast("long")
        .alias("gender_id"),

        # ----------------------------------------------------
        # NATIONALITE
        # ----------------------------------------------------

        safe_col(
            employees,
            "NationalityId",
            "long"
        )
        .cast("long")
        .alias("nationality_id"),

        # ----------------------------------------------------
        # EDUCATION
        # ----------------------------------------------------

        safe_col(
            employees,
            "EducationLevelId",
            "long"
        )
        .cast("long")
        .alias("education_level_id"),

        # ----------------------------------------------------
        # SITUATION FAMILIALE
        # ----------------------------------------------------

        safe_col(
            employees,
            "MaritalStatusId",
            "long"
        )
        .cast("long")
        .alias("marital_status_id"),

        # ----------------------------------------------------
        # CNSS
        # ----------------------------------------------------

        safe_col(
            employees,
            "CnssNumber",
            "string"
        )
        .alias("cnss_number"),

        # ----------------------------------------------------
        # CIMR
        # ----------------------------------------------------

        safe_col(
            employees,
            "CimrNumber",
            "string"
        )
        .alias("cimr_number"),

        safe_col(
            employees,
            "CimrEmployeeRate"
        )
        .cast("decimal(18,6)")
        .alias("cimr_employee_rate"),

        safe_col(
            employees,
            "CimrCompanyRate"
        )
        .cast("decimal(18,6)")
        .alias("cimr_company_rate"),

        # ----------------------------------------------------
        # ASSURANCE PRIVEE
        # ----------------------------------------------------

        safe_col(
            employees,
            "HasPrivateInsurance",
            "boolean"
        )
        .alias("has_private_insurance"),

        safe_col(
            employees,
            "PrivateInsuranceNumber",
            "string"
        )
        .alias("private_insurance_number"),

        safe_col(
            employees,
            "PrivateInsuranceRate"
        )
        .cast("decimal(18,6)")
        .alias("private_insurance_rate"),

        safe_col(
            employees,
            "DisableAmo",
            "boolean"
        )
        .alias("disable_amo"),

        # ----------------------------------------------------
        # MODE PAIEMENT
        # ----------------------------------------------------

        safe_col(
            employees,
            "PaymentMethod",
            "string"
        )
        .alias("payment_method"),

        # ----------------------------------------------------
        # CATEGORIE
        # ----------------------------------------------------

        safe_col(
            employees,
            "CategoryId",
            "long"
        )
        .cast("long")
        .alias("category_id"),

        safe_col(
            employees,
            "FpCategory",
            "string"
        )
        .alias("fp_category"),

        # ----------------------------------------------------
        # PAYS
        # ----------------------------------------------------

        safe_col(
            employees,
            "CountryId",
            "long"
        )
        .cast("long")
        .alias("country_id"),

        # ----------------------------------------------------
        # BANQUE
        # ----------------------------------------------------

        safe_col(
            employees,
            "RibNumber",
            "string"
        )
        .alias("rib_number"),

        safe_col(
            employees,
            "BankAccountId",
            "long"
        )
        .cast("long")
        .alias("bank_account_id"),

        # ----------------------------------------------------
        # AVANTAGES
        # ----------------------------------------------------

        safe_col(
            employees,
            "HousingDeduction"
        )
        .cast("decimal(18,2)")
        .alias("housing_deduction"),

        # ----------------------------------------------------
        # CONGES
        # ----------------------------------------------------

        safe_col(
            employees,
            "AnnualLeaveOpeningDays"
        )
        .cast("decimal(18,2)")
        .alias("annual_leave_opening_days"),

        safe_col(
            employees,
            "AnnualLeaveOpeningEffectiveFrom"
        )
        .alias(
            "annual_leave_opening_effective_from"
        ),

        # ----------------------------------------------------
        # DATES CHANGEMENT
        # ----------------------------------------------------

        safe_col(
            employees,
            "CategoryChangeDate"
        )
        .alias("category_change_date"),

        safe_col(
            employees,
            "CimrRatesChangeDate"
        )
        .alias("cimr_rates_change_date"),

        safe_col(
            employees,
            "ManagerChangeDate"
        )
        .alias("manager_change_date"),

        safe_col(
            employees,
            "MaritalStatusChangeDate"
        )
        .alias("marital_status_change_date"),

        safe_col(
            employees,
            "PrivateInsuranceChangeDate"
        )
        .alias("private_insurance_change_date"),

        safe_col(
            employees,
            "FpCategoryChangeDate"
        )
        .alias("fp_category_change_date"),

        # ----------------------------------------------------
        # AUDIT
        # ----------------------------------------------------

        safe_col(
            employees,
            "CreatedAt"
        ).alias("created_at"),

        safe_col(
            employees,
            "CreatedBy",
            "long"
        )
        .cast("long")
        .alias("created_by"),

        safe_col(
            employees,
            "UpdatedAt"
        ).alias("updated_at"),

        safe_col(
            employees,
            "UpdatedBy",
            "long"
        )
        .cast("long")
        .alias("updated_by"),

        safe_col(
            employees,
            "DeletedAt"
        ).alias("deleted_at"),

        safe_col(
            employees,
            "DeletedBy",
            "long"
        )
        .cast("long")
        .alias("deleted_by")
    )

    # ========================================================
    # 9. DEDUPLICATION
    # ========================================================

    result = result.dropDuplicates(
        ["employee_source_id"]
    )

    # ========================================================
    # 10. SURROGATE KEY
    # ========================================================

    result = add_surrogate_key(
        result,
        "employee_id",
        [
            "employee_source_id",
            "company_source_id"
        ]
    )

    # ========================================================
    # 11. ORDRE FINAL
    # ========================================================

    result = result.select(
        "employee_id",
        "employee_source_id",
        "company_source_id",
        "first_name",
        "last_name",
        "employee_number",
        "departement_id",
        "departement_name",
        "job_position_id",
        "job_position_name",
        "contract_type_id",
        "contract_start_date",
        "contract_end_date",
        "exoneration_end_date",
        "manager_id",
        "status_id",
        "gender_id",
        "nationality_id",
        "education_level_id",
        "marital_status_id",
        "cnss_number",
        "cimr_number",
        "cimr_employee_rate",
        "cimr_company_rate",
        "has_private_insurance",
        "private_insurance_number",
        "private_insurance_rate",
        "disable_amo",
        "payment_method",
        "category_id",
        "fp_category",
        "country_id",
        "rib_number",
        "bank_account_id",
        "housing_deduction",
        "annual_leave_opening_days",
        "annual_leave_opening_effective_from",
        "category_change_date",
        "cimr_rates_change_date",
        "manager_change_date",
        "marital_status_change_date",
        "private_insurance_change_date",
        "fp_category_change_date",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "deleted_at",
        "deleted_by"
    )

    logger.info(
        "dim_employees construite."
    )

    logger.info(
        f"Colonnes dim_employees : "
        f"{result.columns}"
    )

    return result


# ============================================================
# 9. DIM CALENDAR
# ============================================================

def build_dim_calendar(
    spark: SparkSession
):

    logger.info("=" * 70)
    logger.info("Construction dim_calendar")
    logger.info("=" * 70)

    payroll = read_sqlserver_table(
        spark,
        "PayrollResults"
    )

    # --------------------------------------------------------
    # Vérification
    # --------------------------------------------------------

    if "Year" not in payroll.columns:

        raise ValueError(
            "PayrollResults.Year est obligatoire."
        )

    if "Month" not in payroll.columns:

        raise ValueError(
            "PayrollResults.Month est obligatoire."
        )

    # --------------------------------------------------------
    # Couples Year / Month
    # --------------------------------------------------------

    dates = (
        payroll
        .select(
            F.col("Year")
            .cast("int")
            .alias("annee"),

            F.col("Month")
            .cast("int")
            .alias("mois")
        )
        .dropDuplicates()
        .filter(
            F.col("annee").isNotNull()
        )
        .filter(
            F.col("mois").isNotNull()
        )
        .filter(
            F.col("mois").between(1, 12)
        )
    )

    # --------------------------------------------------------
    # Date premier jour du mois
    # --------------------------------------------------------

    dates = dates.withColumn(
        "date",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("annee"),
                F.lpad(
                    F.col("mois"),
                    2,
                    "0"
                ),
                F.lit("01")
            )
        )
    )

    # --------------------------------------------------------
    # Fin du mois
    # --------------------------------------------------------

    dates = dates.withColumn(
        "date_fin_mois",
        F.last_day(
            F.col("date")
        )
    )

    # --------------------------------------------------------
    # Trimestre
    # --------------------------------------------------------

    dates = dates.withColumn(
        "trimestre",
        F.quarter(
            F.col("date")
        )
    )

    # --------------------------------------------------------
    # Semestre
    # --------------------------------------------------------

    dates = dates.withColumn(
        "semestre",
        F.when(
            F.col("mois") <= 6,
            F.lit(1)
        ).otherwise(
            F.lit(2)
        )
    )

    # --------------------------------------------------------
    # Nom mois
    # --------------------------------------------------------

    dates = dates.withColumn(
        "nom_mois",
        F.date_format(
            F.col("date"),
            "MMMM"
        )
    )

    # --------------------------------------------------------
    # MM-yyyy
    # --------------------------------------------------------

    dates = dates.withColumn(
        "mois_annee",
        F.date_format(
            F.col("date"),
            "MM-yyyy"
        )
    )

    # --------------------------------------------------------
    # YYYYMM
    # --------------------------------------------------------

    dates = dates.withColumn(
        "annee_mois",
        (
            F.col("annee") * 100
            + F.col("mois")
        ).cast("int")
    )

    # --------------------------------------------------------
    # calendar_id
    #
    # Clé stable :
    # 202601 -> 202601
    # --------------------------------------------------------

    dates = dates.withColumn(
        "calendar_id",
        F.col("annee_mois")
        .cast("long")
    )

    return dates.select(
        "calendar_id",
        "date",
        "date_fin_mois",
        "mois",
        "annee",
        "trimestre",
        "semestre",
        "nom_mois",
        "mois_annee",
        "annee_mois"
    )


# ============================================================
# 10. DIM COMPOSANT PAIE
# ============================================================

def build_dim_composant_paie(
    spark: SparkSession
):

    logger.info("=" * 70)
    logger.info("Construction dim_composant_paie")
    logger.info("=" * 70)

    primes = read_sqlserver_table(
        spark,
        "PayrollResultPrimes"
    )

    logger.info(
        f"PayrollResultPrimes : "
        f"{primes.columns}"
    )

    # ========================================================
    # COMPOSANTS PRIME
    # ========================================================

    prime_components = primes.select(

        safe_col(
            primes,
            "Label",
            "string"
        )
        .cast("string")
        .alias("libelle"),

        safe_col(
            primes,
            "IsTaxable",
            "boolean"
        )
        .cast("boolean")
        .alias("est_imposable"),

        F.lit("PRIME")
        .cast("string")
        .alias("type_composant")
    )

    # --------------------------------------------------------
    # Suppression DeletedAt
    # --------------------------------------------------------

    if "DeletedAt" in primes.columns:

        prime_components = (
            prime_components
            .withColumn(
                "_deleted",
                F.col("DeletedAt")
            )
            .filter(
                F.col("_deleted").isNull()
            )
            .drop("_deleted")
        )

    # ========================================================
    # COMPOSANTS GENERIQUES
    # ========================================================

    generic_components = spark.createDataFrame(

        [
            (
                "Salaire de base",
                True,
                "SALAIRE_BASE"
            ),

            (
                "CNSS - part salariale",
                False,
                "RETENUE"
            ),

            (
                "CNSS - part patronale",
                False,
                "CHARGE_PATRONALE"
            ),

            (
                "CIMR - part salariale",
                False,
                "RETENUE"
            ),

            (
                "CIMR - part patronale",
                False,
                "CHARGE_PATRONALE"
            ),

            (
                "AMO - part salariale",
                False,
                "RETENUE"
            ),

            (
                "AMO - part patronale",
                False,
                "CHARGE_PATRONALE"
            ),

            (
                "Mutuelle - part salariale",
                False,
                "RETENUE"
            ),

            (
                "Mutuelle - part patronale",
                False,
                "CHARGE_PATRONALE"
            ),

            (
                "IR",
                True,
                "IMPOT"
            ),

            (
                "Net à payer",
                False,
                "NET"
            )
        ],

        schema=T.StructType([

            T.StructField(
                "libelle",
                T.StringType(),
                True
            ),

            T.StructField(
                "est_imposable",
                T.BooleanType(),
                True
            ),

            T.StructField(
                "type_composant",
                T.StringType(),
                True
            )
        ])
    )

    # ========================================================
    # UNION
    # ========================================================

    result = (
        prime_components
        .unionByName(
            generic_components
        )
    )

    # ========================================================
    # NETTOYAGE
    # ========================================================

    result = (
        result
        .filter(
            F.col("libelle").isNotNull()
        )
        .withColumn(
            "libelle",
            F.trim(
                F.col("libelle")
            )
        )
        .filter(
            F.length(
                F.col("libelle")
            ) > 0
        )
        .dropDuplicates(
            ["libelle"]
        )
    )

    # ========================================================
    # CLÉ TECHNIQUE
    # ========================================================

    result = add_surrogate_key(
        result,
        "composant_paie_id",
        ["libelle"]
    )

    return result.select(
        "composant_paie_id",
        "libelle",
        "est_imposable",
        "type_composant"
    )


# ============================================================
# 11. DIM ECHEANCE LEGALE
# ============================================================

def build_dim_echeance_legale(
    spark: SparkSession
):

    logger.info("=" * 70)
    logger.info("Construction dim_echeance_legale")
    logger.info("=" * 70)

    try:

        rules = read_sqlserver_table(
            spark,
            "LeaveTypeLegalRules"
        )

        logger.info(
            f"LeaveTypeLegalRules : "
            f"{rules.columns}"
        )

        result = rules.select(

            safe_col(
                rules,
                "Id",
                "long"
            )
            .cast("long")
            .alias(
                "echeance_source_id"
            ),

            safe_col(
                rules,
                "Name",
                "string"
            )
            .cast("string")
            .alias(
                "libelle"
            ),

            safe_col(
                rules,
                "Description",
                "string"
            )
            .cast("string")
            .alias(
                "description"
            ),

            safe_col(
                rules,
                "Days"
            )
            .cast("decimal(18,2)")
            .alias(
                "nombre_jours"
            ),

            safe_col(
                rules,
                "IsActive",
                "boolean"
            )
            .cast("boolean")
            .alias(
                "est_active"
            )
        )

    except Exception as e:

        logger.warning(
            "Impossible de lire "
            "LeaveTypeLegalRules."
        )

        logger.warning(
            str(e)
        )

        schema = T.StructType([

            T.StructField(
                "echeance_source_id",
                T.LongType(),
                True
            ),

            T.StructField(
                "libelle",
                T.StringType(),
                True
            ),

            T.StructField(
                "description",
                T.StringType(),
                True
            ),

            T.StructField(
                "nombre_jours",
                T.DecimalType(18, 2),
                True
            ),

            T.StructField(
                "est_active",
                T.BooleanType(),
                True
            )
        ])

        result = spark.createDataFrame(
            [],
            schema
        )

    # ========================================================
    # DEDUPLICATION
    # ========================================================

    result = result.dropDuplicates(
        ["echeance_source_id"]
    )

    # ========================================================
    # CLÉ TECHNIQUE
    # ========================================================

    result = add_surrogate_key(
        result,
        "echeance_legale_id",
        ["echeance_source_id"]
    )

    return result.select(
        "echeance_legale_id",
        "echeance_source_id",
        "libelle",
        "description",
        "nombre_jours",
        "est_active"
    )


# ============================================================
# 12. FACT BULLETIN LIGNE
# ============================================================

def build_fact_bulletin_ligne(
    spark: SparkSession,
    dim_clients: DataFrame,
    dim_employees: DataFrame,
    dim_calendar: DataFrame,
    dim_composant_paie: DataFrame
):

    logger.info("=" * 70)
    logger.info("Construction fact_bulletin_ligne")
    logger.info("=" * 70)

    payroll = read_sqlserver_table(
        spark,
        "PayrollResults"
    )

    primes = read_sqlserver_table(
        spark,
        "PayrollResultPrimes"
    )

    logger.info(
        f"PayrollResults : {payroll.columns}"
    )

    logger.info(
        f"PayrollResultPrimes : {primes.columns}"
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    if "Id" not in payroll.columns:

        raise ValueError(
            "PayrollResults.Id est obligatoire."
        )

    # ========================================================
    # BASE PAYROLL
    # ========================================================

    p = payroll.alias("p")

    # ========================================================
    # PRIME
    # ========================================================

    pr = primes.alias("pr")

    # ========================================================
    # JOIN PAYROLL -> PRIME
    # ========================================================

    if (
        "PayrollResultId" in primes.columns
    ):

        fact = p.join(
            pr,
            F.col("p.Id")
            == F.col("pr.PayrollResultId"),
            "left"
        )

    else:

        logger.warning(
            "PayrollResultPrimes.PayrollResultId "
            "absent."
        )

        fact = p

    # ========================================================
    # ANNEE / MOIS
    # ========================================================

    if (
        "Year" in payroll.columns
        and "Month" in payroll.columns
    ):

        fact = (
            fact
            .withColumn(
                "annee",
                F.col("p.Year")
                .cast("int")
            )
            .withColumn(
                "mois",
                F.col("p.Month")
                .cast("int")
            )
        )

    else:

        fact = (
            fact
            .withColumn(
                "annee",
                F.lit(None)
                .cast("int")
            )
            .withColumn(
                "mois",
                F.lit(None)
                .cast("int")
            )
        )

    # ========================================================
    # EMPLOYEE
    # ========================================================

    if "EmployeeId" in payroll.columns:

        fact = fact.withColumn(
            "employee_source_id",
            F.col("p.EmployeeId")
            .cast("long")
        )

    else:

        fact = fact.withColumn(
            "employee_source_id",
            F.lit(None)
            .cast("long")
        )

    # ========================================================
    # CLIENT
    # ========================================================

    if "CompanyId" in payroll.columns:

        fact = fact.withColumn(
            "client_source_id",
            F.col("p.CompanyId")
            .cast("long")
        )

    else:

        fact = fact.withColumn(
            "client_source_id",
            F.lit(None)
            .cast("long")
        )

    # ========================================================
    # LIBELLE COMPOSANT
    # ========================================================

    if "Label" in primes.columns:

        fact = fact.withColumn(
            "libelle_composant",
            F.trim(
                F.col("pr.Label")
                .cast("string")
            )
        )

    else:

        fact = fact.withColumn(
            "libelle_composant",
            F.lit("Salaire de base")
        )

    # ========================================================
    # MONTANT
    # ========================================================

    amount_candidates = [

        "Amount",
        "Value",
        "GrossAmount",
        "NetAmount",
        "Total",
        "PrimeAmount"
    ]

    amount_column = None

    # --------------------------------------------------------
    # PayrollResults
    # --------------------------------------------------------

    for candidate in amount_candidates:

        if candidate in payroll.columns:

            amount_column = F.col(
                f"p.{candidate}"
            )

            logger.info(
                f"Montant trouvé dans PayrollResults : "
                f"{candidate}"
            )

            break

    # --------------------------------------------------------
    # PayrollResultPrimes
    # --------------------------------------------------------

    if amount_column is None:

        for candidate in amount_candidates:

            if candidate in primes.columns:

                amount_column = F.col(
                    f"pr.{candidate}"
                )

                logger.info(
                    f"Montant trouvé dans "
                    f"PayrollResultPrimes : {candidate}"
                )

                break

    # --------------------------------------------------------
    # Aucun montant
    # --------------------------------------------------------

    if amount_column is None:

        logger.warning(
            "Aucune colonne de montant trouvée. "
            "Montant = 0."
        )

        amount_column = F.lit(0)

    fact = fact.withColumn(
        "montant",
        amount_column
        .cast("decimal(18,2)")
    )

    # ========================================================
    # JOIN DIM EMPLOYEES
    # ========================================================

    fact = fact.join(
        dim_employees.alias("de"),

        F.col("employee_source_id")
        ==
        F.col("de.employee_source_id"),

        "left"
    )

    # ========================================================
    # JOIN DIM CLIENTS
    # ========================================================

    fact = fact.join(
        dim_clients.alias("dc"),

        F.col("client_source_id")
        ==
        F.col("dc.client_source_id"),

        "left"
    )

    # ========================================================
    # JOIN CALENDAR
    # ========================================================

    fact = fact.join(
        dim_calendar.alias("cal"),

        (
            F.col("annee")
            ==
            F.col("cal.annee")
        )
        &
        (
            F.col("mois")
            ==
            F.col("cal.mois")
        ),

        "left"
    )

    # ========================================================
    # JOIN COMPOSANT
    # ========================================================

    fact = fact.join(
        dim_composant_paie.alias("cp"),

        F.col("libelle_composant")
        ==
        F.col("cp.libelle"),

        "left"
    )

    # ========================================================
    # PROJECTION FINALE
    # ========================================================

    result = fact.select(

        F.col("dc.client_id")
        .cast("long")
        .alias("client_id"),

        F.col("de.employee_id")
        .cast("long")
        .alias("employee_id"),

        F.col("cal.calendar_id")
        .cast("long")
        .alias("calendar_id"),

        F.col("cp.composant_paie_id")
        .cast("long")
        .alias("composant_paie_id"),

        F.col("p.Id")
        .cast("long")
        .alias(
            "payroll_result_source_id"
        ),

        F.col("libelle_composant")
        .cast("string")
        .alias(
            "libelle_composant"
        ),

        F.col("montant")
        .cast("decimal(18,2)")
        .alias("montant"),

        F.col("annee")
        .cast("int")
        .alias("annee"),

        F.col("mois")
        .cast("int")
        .alias("mois")
    )

    # ========================================================
    # NETTOYAGE
    # ========================================================

    result = result.filter(
        F.col("employee_id").isNotNull()
    )

    return result


# ============================================================
# 13. VALIDATION
# ============================================================

def validate_dataframe(
    df: DataFrame,
    name: str,
    required_columns: list
):

    logger.info(
        f"Validation : {name}"
    )

    missing = [
        c
        for c in required_columns
        if c not in df.columns
    ]

    if missing:

        raise ValueError(
            f"{name} : colonnes manquantes : "
            f"{missing}"
        )

    logger.info(
        f"{name} : structure OK"
    )


# ============================================================
# 14. CACHE / MATERIALIZATION
# ============================================================

def materialize(
    df: DataFrame,
    name: str
):

    logger.info(
        f"Materialisation : {name}"
    )

    df = df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    count = df.count()

    logger.info(
        f"{name} materialisé : "
        f"{count} lignes"
    )

    return df


# ============================================================
# 15. MAIN
# ============================================================

def main():

    spark = None

    dimensions = []
    facts = []

    try:

        # ====================================================
        # START
        # ====================================================

        logger.info("=" * 80)
        logger.info("PAYZENHR ETL - DEMARRAGE")
        logger.info("=" * 80)

        logger.info(
            f"Python : {sys.version}"
        )

        logger.info(
            f"Executable Python : "
            f"{sys.executable}"
        )

        # ====================================================
        # PYTHON
        # ====================================================

        python_version = sys.version_info

        if (
            python_version.major == 3
            and python_version.minor >= 12
        ):

            logger.warning(
                "ATTENTION : Python 3.12 détecté."
            )

            logger.warning(
                "Pour PySpark 3.5.x, "
                "Python 3.11 est recommandé."
            )

        # ====================================================
        # JARS
        # ====================================================

        logger.info(
            f"MSSQL JAR : {MSSQL_JAR}"
        )

        logger.info(
            f"ClickHouse JAR : "
            f"{CLICKHOUSE_JAR}"
        )

        if not os.path.exists(MSSQL_JAR):

            raise FileNotFoundError(
                "JAR SQL Server introuvable : "
                f"{MSSQL_JAR}"
            )

        if not os.path.exists(CLICKHOUSE_JAR):

            raise FileNotFoundError(
                "JAR ClickHouse introuvable : "
                f"{CLICKHOUSE_JAR}"
            )

        # ====================================================
        # SPARK
        # ====================================================

        spark = create_spark_session()

        # ====================================================
        # DIM CLIENTS
        # ====================================================

        dim_clients = build_dim_clients(
            spark
        )

        validate_dataframe(
            dim_clients,
            "dim_clients",
            [
                "client_id",
                "client_source_id",
                "client_name"
            ]
        )

        dim_clients = materialize(
            dim_clients,
            "dim_clients"
        )

        dimensions.append(
            dim_clients
        )

        # ====================================================
        # DIM EMPLOYEES
        # ====================================================

        dim_employees = build_dim_employees(
            spark,
            managed_company_id=MANAGED_BY_COMPANY_ID
        )

        validate_dataframe(
            dim_employees,
            "dim_employees",
            [
                "employee_id",
                "employee_source_id"
            ]
        )

        dim_employees = materialize(
            dim_employees,
            "dim_employees"
        )

        dimensions.append(
            dim_employees
        )

        # ====================================================
        # DIM CALENDAR
        # ====================================================

        dim_calendar = build_dim_calendar(
            spark
        )

        validate_dataframe(
            dim_calendar,
            "dim_calendar",
            [
                "calendar_id",
                "annee",
                "mois"
            ]
        )

        dim_calendar = materialize(
            dim_calendar,
            "dim_calendar"
        )

        dimensions.append(
            dim_calendar
        )

        # ====================================================
        # DIM COMPOSANT PAIE
        # ====================================================

        dim_composant_paie = (
            build_dim_composant_paie(
                spark
            )
        )

        validate_dataframe(
            dim_composant_paie,
            "dim_composant_paie",
            [
                "composant_paie_id",
                "libelle"
            ]
        )

        dim_composant_paie = materialize(
            dim_composant_paie,
            "dim_composant_paie"
        )

        dimensions.append(
            dim_composant_paie
        )

        # ====================================================
        # DIM ECHEANCE LEGALE
        # ====================================================

        dim_echeance_legale = (
            build_dim_echeance_legale(
                spark
            )
        )

        validate_dataframe(
            dim_echeance_legale,
            "dim_echeance_legale",
            [
                "echeance_legale_id"
            ]
        )

        dim_echeance_legale = materialize(
            dim_echeance_legale,
            "dim_echeance_legale"
        )

        dimensions.append(
            dim_echeance_legale
        )

        # ====================================================
        # FACT BULLETIN
        # ====================================================

        fact_bulletin_ligne = (
            build_fact_bulletin_ligne(
                spark,
                dim_clients,
                dim_employees,
                dim_calendar,
                dim_composant_paie
            )
        )

        validate_dataframe(
            fact_bulletin_ligne,
            "fact_bulletin_ligne",
            [
                "client_id",
                "employee_id",
                "calendar_id",
                "composant_paie_id",
                "montant"
            ]
        )

        fact_bulletin_ligne = materialize(
            fact_bulletin_ligne,
            "fact_bulletin_ligne"
        )

        facts.append(
            fact_bulletin_ligne
        )

        # ====================================================
        # RESULTATS
        # ====================================================

        logger.info("=" * 80)
        logger.info("RESULTATS")
        logger.info("=" * 80)

        log_dataframe_count(
            dim_clients,
            "dim_clients"
        )

        log_dataframe_count(
            dim_employees,
            "dim_employees"
        )

        log_dataframe_count(
            dim_calendar,
            "dim_calendar"
        )

        log_dataframe_count(
            dim_composant_paie,
            "dim_composant_paie"
        )

        log_dataframe_count(
            dim_echeance_legale,
            "dim_echeance_legale"
        )

        log_dataframe_count(
            fact_bulletin_ligne,
            "fact_bulletin_ligne"
        )

        # ====================================================
        # CLICKHOUSE
        # ====================================================

        logger.info("=" * 80)
        logger.info("CHARGEMENT CLICKHOUSE")
        logger.info("=" * 80)

        # ----------------------------------------------------
        # Dimensions
        # ----------------------------------------------------

        write_clickhouse(
            dim_clients,
            "dim_clients"
        )

        write_clickhouse(
            dim_employees,
            "dim_employees"
        )

        write_clickhouse(
            dim_calendar,
            "dim_calendar"
        )

        write_clickhouse(
            dim_composant_paie,
            "dim_composant_paie"
        )

        write_clickhouse(
            dim_echeance_legale,
            "dim_echeance_legale"
        )

        # ----------------------------------------------------
        # Fact
        # ----------------------------------------------------

        write_clickhouse(
            fact_bulletin_ligne,
            "fact_bulletin_ligne"
        )

        # ====================================================
        # SUCCESS
        # ====================================================

        logger.info("=" * 80)
        logger.info("ETL TERMINE AVEC SUCCES")
        logger.info("=" * 80)

    except Exception as e:

        logger.exception(
            "ERREUR ETL : %s",
            e
        )

        raise

    finally:

        # ====================================================
        # UNPERSIST
        # ====================================================

        for df in facts:

            try:
                df.unpersist()
            except Exception:
                pass

        for df in dimensions:

            try:
                df.unpersist()
            except Exception:
                pass

        # ====================================================
        # STOP SPARK
        # ====================================================

        if spark is not None:

            logger.info(
                "Arrêt de Spark..."
            )

            try:

                spark.stop()

            except Exception as e:

                logger.warning(
                    f"Erreur arrêt Spark : {e}"
                )

            logger.info(
                "Spark arrêté."
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()