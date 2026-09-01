# =============================================================================
# PAYZEN HR - ETL / BI V3
# =============================================================================
#
# Architecture cible :
#
# DIMENSIONS
#   dim_clients
#   dim_employees
#   dim_calendar
#   dim_composant_paie
#   dim_echeance_legale
#
# FAITS
#   fact_bulletin_ligne
#   fact_echeance
#   fact_absence
#   fact_variable_collecte
#   fact_notes_frais
#   fact_evaluation
#
# GRAIN fact_bulletin_ligne :
#   1 composant de paie
#   x 1 employé
#   x 1 mois
#
# SOURCE PAIE :
#   SQL Server dbo.PayrollResults
#
# TARGET :
#   ClickHouse payzen_bi
#
# IMPORTANT :
#   - SQL Server = READ ONLY
#   - ClickHouse = INSERT ONLY
#   - Aucun DROP
#   - Aucun TRUNCATE
#   - Aucun UPDATE
#   - Aucun DELETE
#   - Les agrégats PayrollResults ne deviennent PAS des composants.
#
# =============================================================================

import os
import sys
import logging
from datetime import datetime, date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

APP_NAME = "PayZenHR_ETL_V3"

SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "(local)")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "PayZenHR_Source")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8124")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "payzen_bi")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

MSSQL_JAR = os.getenv(
    "MSSQL_JAR",
    r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar"
)

CLICKHOUSE_JAR = os.getenv(
    "CLICKHOUSE_JAR",
    r"E:\etape_1\PayZenHR_ETL\jars\clickhouse-jdbc-0.9.8-all.jar"
)

# JDBC SQL Server
SQLSERVER_JDBC_URL = (
    f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
    f"databaseName={SQLSERVER_DATABASE};"
    f"encrypt=false;"
    f"trustServerCertificate=true;"
)

# JDBC ClickHouse
CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:9001/{CLICKHOUSE_DATABASE}"
)

# Mode volontairement LOAD ONLY / INSERT ONLY
LOAD_ONLY = True


# =============================================================================
# 2. LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(APP_NAME)


# =============================================================================
# 3. SPARK
# =============================================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)

    .config(
        "spark.jars",
        f"{MSSQL_JAR},{CLICKHOUSE_JAR}"
    )

    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )

    .config(
        "spark.sql.legacy.timeParserPolicy",
        "LEGACY"
    )

    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =============================================================================
# 4. UTILITAIRES
# =============================================================================

def fail(message: str):
    logger.error("=" * 80)
    logger.error(message)
    logger.error("=" * 80)
    raise RuntimeError(message)


def require_columns(
    df: DataFrame,
    required_columns,
    dataframe_name: str
):
    """
    Vérifie qu'un DataFrame contient toutes les colonnes nécessaires.
    """

    existing = set(df.columns)

    missing = [
        c for c in required_columns
        if c not in existing
    ]

    if missing:
        fail(
            f"{dataframe_name}: colonnes manquantes : {missing}\n"
            f"Colonnes disponibles : {df.columns}"
        )


def sqlserver_table(table_name: str) -> DataFrame:
    """
    Lecture READ ONLY depuis SQL Server.
    """

    logger.info(
        "Lecture SQL Server : dbo.%s",
        table_name
    )

    return (
        spark.read
        .format("jdbc")
        .option("url", SQLSERVER_JDBC_URL)
        .option(
            "dbtable",
            f"dbo.{table_name}"
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
            "1000"
        )
        .load()
    )


def clickhouse_write(
    df: DataFrame,
    table_name: str
):
    """
    INSERT ONLY dans ClickHouse.

    Aucune opération destructive.
    """

    if df is None:
        logger.warning(
            "DataFrame None : %s non chargé",
            table_name
        )
        return

    count = df.count()

    logger.info(
        "Chargement ClickHouse : %s | lignes=%s",
        table_name,
        count
    )

    if count == 0:
        logger.warning(
            "Table %s : aucune ligne à insérer.",
            table_name
        )
        return

    (
        df.write
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
            "1000"
        )
        .mode("append")
        .save()
    )


def remove_soft_deleted(df: DataFrame) -> DataFrame:

    if "DeletedAt" in df.columns:
        return df.filter(
            F.col("DeletedAt").isNull()
        )

    return df


# =============================================================================
# 5. LECTURE DES SOURCES
# =============================================================================

def load_payroll_results():

    df = sqlserver_table("PayrollResults")

    required = [
        "Id",
        "EmployeeId",
        "CompanyId",
        "Month",
        "Year",

        "SalaireBase",

        "HeuresSupp25",
        "HeuresSupp50",
        "HeuresSupp100",

        "Conges",
        "JoursFeries",

        "PrimeAnciennete",
        "PrimeAnciennteRate",

        "PrimeImposable1",
        "PrimeImposable2",
        "PrimeImposable3",
        "TotalPrimesImposables",

        "FraisProfessionnels",
        "IndemniteRepresentation",
        "PrimeTransport",
        "PrimePanier",
        "IndemniteDeplacement",
        "IndemniteCaisse",
        "PrimeSalissure",
        "GratificationsFamilial",
        "PrimeVoyageMecque",
        "IndemniteLicenciement",
        "IndemniteKilometrique",
        "PrimeTourne",
        "PrimeOutillage",
        "AideMedicale",
        "AutresPrimesNonImposable",

        "TotalIndemnites",
        "TotalNiExcedentImposable",

        "CnssPartSalariale",
        "CnssBase",

        "CimrPartSalariale",
        "CimrBase",

        "AmoPartSalariale",
        "AmoBase",

        "MutuellePartSalariale",
        "MutuelleBase",

        "TotalCotisationsSalariales",

        "CnssPartPatronale",
        "CimrPartPatronale",
        "AmoPartPatronale",
        "MutuellePartPatronale",

        "TotalCotisationsPatronales",

        "ImpotRevenu",
        "IrTaux",

        "AvanceSurSalaire",
        "InteretSurLogement",

        "BrutImposable",
        "NetImposable",

        "NetAPayer",

        "DeletedAt"
    ]

    require_columns(
        df,
        required,
        "PayrollResults"
    )

    df = remove_soft_deleted(df)

    logger.info(
        "PayrollResults actifs : %s",
        df.count()
    )

    return df


# =============================================================================
# 6. DIM_CLIENTS
# =============================================================================

def build_dim_clients():

    logger.info("=" * 80)
    logger.info("CONSTRUCTION dim_clients")
    logger.info("=" * 80)

    companies = sqlserver_table("Companies")

    companies = remove_soft_deleted(companies)

    # Colonnes réellement indispensables pour la clé.
    require_columns(
        companies,
        ["Id"],
        "Companies"
    )

    # On ne fabrique PAS les colonnes inconnues.
    #
    # Les noms exacts doivent être présents dans la source.
    #
    # Le code détecte les colonnes disponibles parmi les candidats.

    def first_existing(candidates):

        for c in candidates:
            if c in companies.columns:
                return F.col(c)

        return F.lit(None)

    dim = companies.select(
        F.col("Id").cast("long").alias("client_id"),

        first_existing(
            ["Name", "CompanyName", "LegalName"]
        ).cast("string").alias("nom"),

        first_existing(
            ["Sector", "ActivitySector", "BusinessSector"]
        ).cast("string").alias("secteur"),

        first_existing(
            ["Size", "CompanySize", "EmployeeCount"]
        ).cast("string").alias("taille"),

        first_existing(
            ["CabinetId", "AccountingFirmId", "FirmId"]
        ).cast("long").alias("cabinet_id")
    )

    dim = (
        dim
        .dropDuplicates(["client_id"])
    )

    return dim


# =============================================================================
# 7. DIM_EMPLOYEES
# =============================================================================

def build_dim_employees():

    logger.info("=" * 80)
    logger.info("CONSTRUCTION dim_employees")
    logger.info("=" * 80)

    employees = sqlserver_table("Employees")
    employees = remove_soft_deleted(employees)

    contracts = sqlserver_table("EmployeeContracts")
    contracts = remove_soft_deleted(contracts)

    positions = sqlserver_table("JobPositions")
    positions = remove_soft_deleted(positions)

    departments = sqlserver_table("Departements")
    departments = remove_soft_deleted(departments)

    require_columns(
        employees,
        ["Id", "CompanyId"],
        "Employees"
    )

    require_columns(
        contracts,
        [
            "EmployeeId",
            "CompanyId",
            "JobPositionId"
        ],
        "EmployeeContracts"
    )

    require_columns(
        positions,
        ["Id"],
        "JobPositions"
    )

    require_columns(
        departments,
        ["Id"],
        "Departements"
    )

    # -------------------------------------------------------------------------
    # Détection des colonnes descriptives.
    # -------------------------------------------------------------------------

    def first_col(df, candidates):

        for c in candidates:
            if c in df.columns:
                return F.col(c)

        return F.lit(None)

    emp = employees.alias("e")
    ctr = contracts.alias("c")
    pos = positions.alias("p")
    dep = departments.alias("d")

    # -------------------------------------------------------------------------
    # Contrats actifs
    #
    # On privilégie les contrats non supprimés.
    # Si plusieurs contrats existent, on garde le plus récent.
    # -------------------------------------------------------------------------

    contract_date_col = None

    for candidate in [
        "StartDate",
        "DateDebut",
        "CreatedAt"
    ]:
        if candidate in contracts.columns:
            contract_date_col = candidate
            break

    if contract_date_col:

        w = Window.partitionBy(
            "EmployeeId"
        ).orderBy(
            F.col(contract_date_col).desc_nulls_last()
        )

        ctr = (
            contracts
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .alias("c")
        )

    joined = (
        emp
        .join(
            ctr,
            F.col("e.Id") == F.col("c.EmployeeId"),
            "left"
        )
        .join(
            pos,
            F.col("c.JobPositionId") == F.col("p.Id"),
            "left"
        )
        .join(
            dep,
            F.col("e.DepartementId") == F.col("d.Id"),
            "left"
        )
    )

    employee_name = first_col(
        employees,
        [
            "Name",
            "FullName",
            "EmployeeName",
            "Nom",
            "FirstName"
        ]
    )

    position_name = first_col(
        positions,
        [
            "Name",
            "Title",
            "Label",
            "JobTitle"
        ]
    )

    level_col = first_col(
        employees,
        [
            "Level",
            "Niveau"
        ]
    )

    department_name = first_col(
        departments,
        [
            "Name",
            "Label",
            "DepartmentName",
            "Nom"
        ]
    )

    hire_date = first_col(
        employees,
        [
            "HireDate",
            "DateEmbauche",
            "StartDate",
            "DateEntree"
        ]
    )

    # Si la date d'embauche est uniquement dans le contrat,
    # on essaie ensuite les colonnes contractuelles.
    if all(
        candidate not in employees.columns
        for candidate in [
            "HireDate",
            "DateEmbauche",
            "StartDate",
            "DateEntree"
        ]
    ):

        hire_date = first_col(
            contracts,
            [
                "StartDate",
                "DateDebut",
                "HireDate",
                "DateEmbauche"
            ]
        )

    dim = joined.select(
        F.col("e.Id")
        .cast("long")
        .alias("employee_id"),

        employee_name
        .cast("string")
        .alias("nom"),

        position_name
        .cast("string")
        .alias("poste"),

        level_col
        .cast("string")
        .alias("niveau"),

        department_name
        .cast("string")
        .alias("service"),

        hire_date
        .cast("date")
        .alias("date_embauche")
    )

    dim = (
        dim
        .dropDuplicates(["employee_id"])
    )

    return dim


# =============================================================================
# 8. DIM_CALENDAR
# =============================================================================

def build_dim_calendar(payroll):

    logger.info("=" * 80)
    logger.info("CONSTRUCTION dim_calendar")
    logger.info("=" * 80)

    min_max = payroll.select(
        F.min(
            F.make_date(
                F.col("Year"),
                F.col("Month"),
                F.lit(1)
            )
        ).alias("min_date"),

        F.max(
            F.make_date(
                F.col("Year"),
                F.col("Month"),
                F.lit(1)
            )
        ).alias("max_date")
    ).collect()[0]

    min_date = min_max["min_date"]
    max_date = min_max["max_date"]

    if min_date is None or max_date is None:
        fail(
            "Impossible de construire dim_calendar : "
            "aucune date Year/Month valide."
        )

    # On crée une vraie date pour chaque jour.
    calendar = (
        spark
        .range(
            0,
            (
                max_date - min_date
            ).days + 1
        )
        .withColumn(
            "date",
            F.date_add(
                F.lit(min_date),
                F.col("id").cast("int")
            )
        )
        .drop("id")
    )

    calendar = calendar.select(
        F.col("date").cast("date"),

        F.dayofmonth("date")
        .alias("jour"),

        F.month("date")
        .alias("mois"),

        F.quarter("date")
        .alias("trimestre"),

        F.year("date")
        .alias("année")
    )

    return calendar


# =============================================================================
# 9. DIM_COMPOSANT_PAIE
# =============================================================================

def build_dim_composant_paie():

    logger.info("=" * 80)
    logger.info("CONSTRUCTION dim_composant_paie")
    logger.info("=" * 80)

    # -------------------------------------------------------------------------
    # IMPORTANT
    #
    # On ne met PAS les champs agrégés :
    #
    #   TotalPrimesImposables
    #   TotalBrut
    #   TotalIndemnites
    #   TotalCotisationsSalariales
    #   TotalCotisationsPatronales
    #   BrutImposable
    #   NetImposable
    #   TotalGains
    #   TotalRetenues
    #   NetAPayer
    #
    # Sinon le fait granulaire contiendrait des doublons conceptuels.
    # -------------------------------------------------------------------------

    components = [

        # ---------------------------------------------------------------------
        # GAINS / BRUT
        # ---------------------------------------------------------------------

        (1, "Salaire de base", "Brut", "SalaireBase"),

        (2, "Heures supplémentaires 25%", "Brut", "HeuresSupp25"),
        (3, "Heures supplémentaires 50%", "Brut", "HeuresSupp50"),
        (4, "Heures supplémentaires 100%", "Brut", "HeuresSupp100"),

        (5, "Congés", "Brut", "Conges"),
        (6, "Jours fériés", "Brut", "JoursFeries"),

        (7, "Prime ancienneté", "Brut", "PrimeAnciennete"),

        (8, "Prime imposable 1", "Brut", "PrimeImposable1"),
        (9, "Prime imposable 2", "Brut", "PrimeImposable2"),
        (10, "Prime imposable 3", "Brut", "PrimeImposable3"),

        # ---------------------------------------------------------------------
        # INDEMNITES / GAINS NON IMPOSABLES
        # ---------------------------------------------------------------------

        (11, "Frais professionnels", "Brut", "FraisProfessionnels"),
        (12, "Indemnité représentation", "Brut", "IndemniteRepresentation"),
        (13, "Prime transport", "Brut", "PrimeTransport"),
        (14, "Prime panier", "Brut", "PrimePanier"),
        (15, "Indemnité déplacement", "Brut", "IndemniteDeplacement"),
        (16, "Indemnité caisse", "Brut", "IndemniteCaisse"),
        (17, "Prime salissure", "Brut", "PrimeSalissure"),
        (18, "Gratifications familiales", "Brut", "GratificationsFamilial"),
        (19, "Prime voyage Mecque", "Brut", "PrimeVoyageMecque"),
        (20, "Indemnité licenciement", "Brut", "IndemniteLicenciement"),
        (21, "Indemnité kilométrique", "Brut", "IndemniteKilometrique"),
        (22, "Prime tournée", "Brut", "PrimeTourne"),
        (23, "Prime outillage", "Brut", "PrimeOutillage"),
        (24, "Aide médicale", "Brut", "AideMedicale"),
        (25, "Autres primes non imposables", "Brut", "AutresPrimesNonImposable"),

        # ---------------------------------------------------------------------
        # CHARGES SALARIALES / DEDUCTIONS
        # ---------------------------------------------------------------------

        (26, "CNSS - part salariale", "Déduction", "CnssPartSalariale"),
        (27, "CIMR - part salariale", "Déduction", "CimrPartSalariale"),
        (28, "AMO - part salariale", "Déduction", "AmoPartSalariale"),
        (29, "Mutuelle - part salariale", "Déduction", "MutuellePartSalariale"),

        (30, "Impôt sur le revenu", "Déduction", "ImpotRevenu"),

        (31, "Avance sur salaire", "Déduction", "AvanceSurSalaire"),
        (32, "Intérêt sur logement", "Déduction", "InteretSurLogement"),

        # ---------------------------------------------------------------------
        # CHARGES PATRONALES
        # ---------------------------------------------------------------------

        (33, "CNSS - part patronale", "Charge", "CnssPartPatronale"),
        (34, "CIMR - part patronale", "Charge", "CimrPartPatronale"),
        (35, "AMO - part patronale", "Charge", "AmoPartPatronale"),
        (36, "Mutuelle - part patronale", "Charge", "MutuellePartPatronale"),
    ]

    schema = T.StructType([
        T.StructField(
            "composant_id",
            T.IntegerType(),
            False
        ),
        T.StructField(
            "libelle",
            T.StringType(),
            False
        ),
        T.StructField(
            "categorie",
            T.StringType(),
            False
        ),
        T.StructField(
            "source_column",
            T.StringType(),
            False
        )
    ])

    return spark.createDataFrame(
        components,
        schema
    )


# =============================================================================
# 10. FACT_BULLETIN_LIGNE
# =============================================================================

def build_fact_bulletin_ligne(
    payroll: DataFrame,
    dim_components: DataFrame
):

    logger.info("=" * 80)
    logger.info("CONSTRUCTION fact_bulletin_ligne")
    logger.info("=" * 80)

    # -------------------------------------------------------------------------
    # Vérification des colonnes.
    # -------------------------------------------------------------------------

    component_columns = [
        row["source_column"]
        for row in dim_components.collect()
    ]

    require_columns(
        payroll,
        [
            "Id",
            "EmployeeId",
            "CompanyId",
            "Month",
            "Year"
        ] + component_columns,
        "PayrollResults / fact_bulletin_ligne"
    )

    # -------------------------------------------------------------------------
    # UNPIVOT
    #
    # On transforme :
    #
    # Employee | SalaireBase | PrimeTransport | CNSS
    #
    # en :
    #
    # Employee | composant | montant
    #
    # -------------------------------------------------------------------------

    stack_parts = []

    for row in dim_components.collect():

        component_id = row["composant_id"]
        source_column = row["source_column"]

        stack_parts.extend([
            F.lit(component_id),
            F.col(source_column).cast("decimal(18,2)")
        ])

    stack_expression = (
        f"stack({len(component_columns)}, "
        + ",".join(
            [
                f"{row['composant_id']}, "
                f"`{row['source_column']}`"
                for row in dim_components.collect()
            ]
        )
        + ") as (composant_id, montant)"
    )

    fact = (
        payroll
        .select(
            F.col("Id").alias("payroll_result_id"),

            F.col("EmployeeId")
            .cast("long")
            .alias("employee_id"),

            F.col("CompanyId")
            .cast("long")
            .alias("client_id"),

            F.col("Month")
            .cast("int")
            .alias("mois"),

            F.col("Year")
            .cast("int")
            .alias("annee"),

            F.expr(stack_expression)
        )
    )

    # -------------------------------------------------------------------------
    # NULL => 0
    # -------------------------------------------------------------------------

    fact = fact.withColumn(
        "montant",
        F.coalesce(
            F.col("montant"),
            F.lit(0).cast("decimal(18,2)")
        )
    )

    # -------------------------------------------------------------------------
    # On conserve uniquement les composants réellement présents.
    #
    # Exemple :
    # PrimeImposable1 = 0
    #
    # => pas besoin de créer une ligne inutile dans le fait.
    # -------------------------------------------------------------------------

    fact = fact.filter(
        F.abs(
            F.col("montant")
        ) > F.lit(0.01)
    )

    # -------------------------------------------------------------------------
    # Date mensuelle.
    #
    # Le grain métier est mensuel.
    # On utilise le premier jour du mois comme date de référence.
    # -------------------------------------------------------------------------

    fact = fact.withColumn(
        "date",
        F.make_date(
            F.col("annee"),
            F.col("mois"),
            F.lit(1)
        )
    )

    # -------------------------------------------------------------------------
    # Jointure dimension composant
    # -------------------------------------------------------------------------

    fact = (
        fact
        .join(
            dim_components.select(
                "composant_id",
                "libelle",
                "categorie"
            ),
            on="composant_id",
            how="inner"
        )
    )

    # -------------------------------------------------------------------------
    # Colonnes finales
    # -------------------------------------------------------------------------

    fact = fact.select(
        "payroll_result_id",
        "employee_id",
        "client_id",
        "date",
        "mois",
        "annee",
        "composant_id",
        "libelle",
        "categorie",
        "montant"
    )

    # -------------------------------------------------------------------------
    # Vérification du grain.
    #
    # Une ligne =
    #   1 payroll result
    #   1 composant
    #
    # Donc aucune duplication.
    # -------------------------------------------------------------------------

    duplicate_count = (
        fact
        .groupBy(
            "employee_id",
            "client_id",
            "date",
            "composant_id"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_count > 0:

        fail(
            f"fact_bulletin_ligne : {duplicate_count} "
            "doublons détectés sur le grain "
            "(employee_id, client_id, date, composant_id)."
        )

    logger.info(
        "fact_bulletin_ligne : %s lignes",
        fact.count()
    )

    return fact


# =============================================================================
# 11. VALIDATION DES PRIMES
# =============================================================================

def validate_primes(payroll):

    logger.info("=" * 80)
    logger.info("VALIDATION TotalPrimesImposables")
    logger.info("=" * 80)

    validation = (
        payroll
        .withColumn(
            "calcule",
            F.coalesce(F.col("PrimeImposable1"), F.lit(0))
            + F.coalesce(F.col("PrimeImposable2"), F.lit(0))
            + F.coalesce(F.col("PrimeImposable3"), F.lit(0))
        )
        .withColumn(
            "ecart",
            F.coalesce(
                F.col("TotalPrimesImposables"),
                F.lit(0)
            ) - F.col("calcule")
        )
    )

    anomalies = (
        validation
        .filter(
            F.abs(F.col("ecart")) > F.lit(0.01)
        )
    )

    count = anomalies.count()

    if count > 0:

        logger.error(
            "ATTENTION : %s anomalies TotalPrimesImposables",
            count
        )

        anomalies.select(
            "Id",
            "EmployeeId",
            "CompanyId",
            "Month",
            "Year",
            "PrimeImposable1",
            "PrimeImposable2",
            "PrimeImposable3",
            "TotalPrimesImposables",
            "calcule",
            "ecart"
        ).show(
            20,
            truncate=False
        )

        fail(
            "Validation TotalPrimesImposables échouée."
        )

    logger.info(
        "OK : aucune anomalie TotalPrimesImposables > 0.01"
    )


# =============================================================================
# 12. DIM ECHEANCE LEGALE
# =============================================================================

def build_dim_echeance_legale():

    logger.info("=" * 80)
    logger.info("CONSTRUCTION dim_echeance_legale")
    logger.info("=" * 80)

    # -------------------------------------------------------------------------
    # Cette dimension doit être alimentée par le référentiel légal existant.
    #
    # IMPORTANT :
    # on ne crée PAS ici de fausses règles légales.
    #
    # La table doit être alimentée depuis la source légale PayZen lorsqu'elle
    # sera identifiée.
    # -------------------------------------------------------------------------

    schema = T.StructType([
        T.StructField(
            "echeance_id",
            T.IntegerType(),
            False
        ),
        T.StructField(
            "type",
            T.StringType(),
            False
        ),
        T.StructField(
            "periodicite",
            T.StringType(),
            True
        ),
        T.StructField(
            "regle_date_limite",
            T.StringType(),
            True
        )
    ])

    # Aucun faux référentiel.
    return spark.createDataFrame(
        [],
        schema
    )


# =============================================================================
# 13. FACTS FUTURS
# =============================================================================

def empty_fact_echeance():

    schema = T.StructType([
        T.StructField("echeance_id", T.IntegerType(), True),
        T.StructField("client_id", T.LongType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("annee", T.IntegerType(), True),
        T.StructField("mois", T.IntegerType(), True),
        T.StructField("date_limite", T.DateType(), True),
        T.StructField("statut", T.StringType(), True),
    ])

    return spark.createDataFrame([], schema)


def empty_fact_absence():

    schema = T.StructType([
        T.StructField("employee_id", T.LongType(), True),
        T.StructField("client_id", T.LongType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("type_absence", T.StringType(), True),
        T.StructField("duree", T.DecimalType(18, 2), True),
    ])

    return spark.createDataFrame([], schema)


def empty_fact_variable_collecte():

    schema = T.StructType([
        T.StructField("client_id", T.LongType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("canal", T.StringType(), True),
        T.StructField("delai", T.DecimalType(18, 2), True),
    ])

    return spark.createDataFrame([], schema)


def empty_fact_notes_frais():

    schema = T.StructType([
        T.StructField("client_id", T.LongType(), True),
        T.StructField("employee_id", T.LongType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("montant", T.DecimalType(18, 2), True),
    ])

    return spark.createDataFrame([], schema)


def empty_fact_evaluation():

    schema = T.StructType([
        T.StructField("employee_id", T.LongType(), True),
        T.StructField("client_id", T.LongType(), True),
        T.StructField("date", T.DateType(), True),
        T.StructField("score", T.DecimalType(18, 2), True),
    ])

    return spark.createDataFrame([], schema)


# =============================================================================
# 14. CONTROLES DE COHERENCE
# =============================================================================

def validate_fact_bulletin(
    fact,
    payroll
):

    logger.info("=" * 80)
    logger.info("CONTROLES fact_bulletin_ligne")
    logger.info("=" * 80)

    # -------------------------------------------------------------------------
    # 1. Aucun employee_id NULL
    # -------------------------------------------------------------------------

    null_employee = (
        fact
        .filter(
            F.col("employee_id").isNull()
        )
        .count()
    )

    if null_employee:
        fail(
            f"fact_bulletin_ligne : {null_employee} "
            "lignes avec employee_id NULL."
        )

    # -------------------------------------------------------------------------
    # 2. Aucun client_id NULL
    # -------------------------------------------------------------------------

    null_client = (
        fact
        .filter(
            F.col("client_id").isNull()
        )
        .count()
    )

    if null_client:
        fail(
            f"fact_bulletin_ligne : {null_client} "
            "lignes avec client_id NULL."
        )

    # -------------------------------------------------------------------------
    # 3. Dates valides
    # -------------------------------------------------------------------------

    invalid_dates = (
        fact
        .filter(
            F.col("date").isNull()
            |
            (F.col("mois") < 1)
            |
            (F.col("mois") > 12)
        )
        .count()
    )

    if invalid_dates:
        fail(
            f"fact_bulletin_ligne : {invalid_dates} "
            "dates/mois invalides."
        )

    # -------------------------------------------------------------------------
    # 4. Les PayrollResults sources doivent être identifiables
    # -------------------------------------------------------------------------

    source_ids = (
        payroll
        .select(
            F.col("Id").alias("payroll_result_id")
        )
        .distinct()
    )

    orphan = (
        fact
        .select("payroll_result_id")
        .distinct()
        .join(
            source_ids,
            on="payroll_result_id",
            how="left_anti"
        )
        .count()
    )

    if orphan:
        fail(
            f"fact_bulletin_ligne : {orphan} "
            "payroll_result_id inconnus."
        )

    logger.info(
        "OK : contrôles fact_bulletin_ligne validés."
    )


# =============================================================================
# 15. MAIN
# =============================================================================

def main():

    start = datetime.now()

    logger.info("=" * 80)
    logger.info("PAYZEN HR ETL V3 - DEMARRAGE")
    logger.info("=" * 80)

    logger.info(
        "SQL Server : %s / %s",
        SQLSERVER_HOST,
        SQLSERVER_DATABASE
    )

    logger.info(
        "ClickHouse : %s / %s",
        CLICKHOUSE_HOST,
        CLICKHOUSE_DATABASE
    )

    logger.info(
        "MODE : LOAD_ONLY / INSERT_ONLY"
    )

    try:

        # ---------------------------------------------------------------------
        # SOURCE PAIE
        # ---------------------------------------------------------------------

        payroll = load_payroll_results()

        # ---------------------------------------------------------------------
        # VALIDATION MÉTIER CONFIRMÉE
        # ---------------------------------------------------------------------

        validate_primes(payroll)

        # ---------------------------------------------------------------------
        # DIMENSIONS
        # ---------------------------------------------------------------------

        dim_clients = build_dim_clients()

        dim_employees = build_dim_employees()

        dim_calendar = build_dim_calendar(
            payroll
        )

        dim_components = build_dim_composant_paie()

        dim_echeance = build_dim_echeance_legale()

        # ---------------------------------------------------------------------
        # FACT PAIE
        # ---------------------------------------------------------------------

        fact_bulletin = build_fact_bulletin_ligne(
            payroll,
            dim_components
        )

        validate_fact_bulletin(
            fact_bulletin,
            payroll
        )

        # ---------------------------------------------------------------------
        # AUTRES FAITS
        #
        # Ils restent volontairement vides tant que les sources exactes
        # n'ont pas été identifiées.
        #
        # ON NE FABRIQUE PAS DE DONNÉES.
        # ---------------------------------------------------------------------

        fact_echeance = empty_fact_echeance()
        fact_absence = empty_fact_absence()
        fact_variable = empty_fact_variable_collecte()
        fact_notes = empty_fact_notes_frais()
        fact_evaluation = empty_fact_evaluation()

        # ---------------------------------------------------------------------
        # RAPPORT
        # ---------------------------------------------------------------------

        logger.info("=" * 80)
        logger.info("RESUME AVANT CHARGEMENT")
        logger.info("=" * 80)

        logger.info(
            "dim_clients          : %s",
            dim_clients.count()
        )

        logger.info(
            "dim_employees        : %s",
            dim_employees.count()
        )

        logger.info(
            "dim_calendar         : %s",
            dim_calendar.count()
        )

        logger.info(
            "dim_composant_paie   : %s",
            dim_components.count()
        )

        logger.info(
            "dim_echeance_legale  : %s",
            dim_echeance.count()
        )

        logger.info(
            "fact_bulletin_ligne   : %s",
            fact_bulletin.count()
        )

        # ---------------------------------------------------------------------
        # CHARGEMENT CLICKHOUSE
        # ---------------------------------------------------------------------

        if not LOAD_ONLY:
            fail(
                "Configuration invalide : "
                "le script doit rester en LOAD_ONLY."
            )

        clickhouse_write(
            dim_clients,
            "dim_clients"
        )

        clickhouse_write(
            dim_employees,
            "dim_employees"
        )

        clickhouse_write(
            dim_calendar,
            "dim_calendar"
        )

        clickhouse_write(
            dim_components,
            "dim_composant_paie"
        )

        # Seulement si le référentiel contient des lignes.
        if dim_echeance.count() > 0:
            clickhouse_write(
                dim_echeance,
                "dim_echeance_legale"
            )
        else:
            logger.warning(
                "dim_echeance_legale non chargée : "
                "référentiel légal source non identifié."
            )

        clickhouse_write(
            fact_bulletin,
            "fact_bulletin_ligne"
        )

        # Les autres faits ne sont pas chargés tant que leurs sources
        # métier réelles ne sont pas identifiées.
        logger.info(
            "fact_echeance : source non identifiée -> non chargée."
        )

        logger.info(
            "fact_absence : source non identifiée -> non chargée."
        )

        logger.info(
            "fact_variable_collecte : source non identifiée -> non chargée."
        )

        logger.info(
            "fact_notes_frais : source non identifiée -> non chargée."
        )

        logger.info(
            "fact_evaluation : source non identifiée -> non chargée."
        )

        # ---------------------------------------------------------------------
        # FIN
        # ---------------------------------------------------------------------

        duration = datetime.now() - start

        logger.info("=" * 80)
        logger.info(
            "PAYZEN HR ETL V3 - TERMINE"
        )
        logger.info(
            "Duree : %s",
            duration
        )
        logger.info("=" * 80)

    except Exception as exc:

        logger.exception(
            "ETL ECHEC : %s",
            exc
        )

        raise

    finally:

        spark.stop()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()