# ============================================================
# PAYZEN HR - DATA WAREHOUSE V3
# SQL SERVER -> PYSPARK -> CLICKHOUSE
#
# Source :
#     PayZenHR_Source
#
# Target :
#     payzen_bi
#
# Grain fact_bulletin_ligne :
#     1 composant x 1 employé x 1 mois
#
# IMPORTANT :
#     Aucun GrossSalary
#     Aucun NetSalary
#     Aucun BaseSalary
# ============================================================

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

SQLSERVER_HOST = "localhost"
SQLSERVER_PORT = "1433"
SQLSERVER_DB = "PayZenHR_Source"

SQLSERVER_USER = os.getenv("PAYZEN_SQL_USER", "sa")
SQLSERVER_PASSWORD = os.getenv("PAYZEN_SQL_PASSWORD", "noraNORA123")

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = "8124"
CLICKHOUSE_DB = "payzen_bi"

MSSQL_JAR = (
    r"E:\etape_1\PayZenHR_ETL\pipline_test"
    r"\payzenhr_pyspark\jars\mssql-jdbc-13.4.0.jre8.jar"
)

CLICKHOUSE_JAR = (
    r"E:\etape_1\PayZenHR_ETL\pipline_test"
    r"\payzenhr_pyspark\jars\clickhouse-jdbc-0.9.8-all.jar"
)

JARS = f"{MSSQL_JAR};{CLICKHOUSE_JAR}"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("PAYZENHR_V3")


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_DW_V3")
    .master("local[*]")
    .config("spark.jars", JARS)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# JDBC SQL SERVER
# ============================================================

SQLSERVER_URL = (
    f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
    f"databaseName={SQLSERVER_DB};"
    f"encrypt=false;"
    f"trustServerCertificate=true;"
)

SQLSERVER_PROPS = {
    "user": SQLSERVER_USER,
    "password": SQLSERVER_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# ============================================================
# JDBC CLICKHOUSE
# ============================================================

CLICKHOUSE_URL = (
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)

CLICKHOUSE_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


# ============================================================
# HELPER SQL SERVER
# ============================================================

def read_sqlserver_table(table_name):

    logger.info(f"Lecture SQL Server : {table_name}")

    return (
        spark.read
        .jdbc(
            url=SQLSERVER_URL,
            table=table_name,
            properties=SQLSERVER_PROPS
        )
    )


# ============================================================
# 1. PAYROLL RESULTS
# ============================================================

logger.info("=" * 80)
logger.info("LECTURE PayrollResults")
logger.info("=" * 80)

payroll = read_sqlserver_table("PayrollResults")


required_columns = [
    "Id",
    "EmployeeId",
    "CompanyId",
    "Month",
    "Year",
    "Status",
    "SalaireBase",
    "TotalBrut",
    "NetAPayer"
]

missing = [
    c for c in required_columns
    if c not in payroll.columns
]

if missing:
    raise RuntimeError(
        "Colonnes manquantes dans PayrollResults : "
        + ", ".join(missing)
    )


logger.info(f"PayrollResults lignes source : {payroll.count()}")


# ============================================================
# 2. GARDER LES BULLETINS VALIDES
# ============================================================

payroll = (
    payroll
    .filter(F.col("Status") == 7)
)

logger.info(
    f"PayrollResults Status=7 : {payroll.count()}"
)


# ============================================================
# 3. SUPPRESSION DES DOUBLONS EVENTUELS
# ============================================================

partition_cols = [
    "EmployeeId",
    "CompanyId",
    "Month",
    "Year"
]

if "PayHalf" in payroll.columns:
    partition_cols.append("PayHalf")


window_latest = (
    Window
    .partitionBy(*partition_cols)
    .orderBy(F.col("Id").desc())
)

payroll = (
    payroll
    .withColumn("_rn", F.row_number().over(window_latest))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

logger.info(
    f"Bulletins uniques après dédoublonnage : {payroll.count()}"
)


# ============================================================
# 4. DIM CALENDAR
# ============================================================

calendar = (
    payroll
    .select(
        F.col("Month").cast("int").alias("mois"),
        F.col("Year").cast("int").alias("annee")
    )
    .distinct()
    .withColumn(
        "date",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("annee"),
                F.lpad(F.col("mois"), 2, "0"),
                F.lit("01")
            )
        )
    )
    .withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "jour",
        F.dayofmonth("date")
    )
    .withColumn(
        "nom_mois",
        F.date_format("date", "MMMM")
    )
    .withColumn(
        "trimestre",
        F.quarter("date")
    )
    .withColumn(
        "annee_mois",
        F.date_format("date", "yyyy-MM")
    )
    .select(
        "date_key",
        "date",
        "jour",
        "mois",
        "nom_mois",
        "trimestre",
        "annee",
        "annee_mois"
    )
)

logger.info(
    f"dim_calendar : {calendar.count()} lignes"
)


# ============================================================
# 5. DIM CLIENTS
# ============================================================

try:

    companies = read_sqlserver_table("Companies")

    company_columns = companies.columns

    logger.info(
        "Colonnes Companies : "
        + ", ".join(company_columns)
    )

    # ID obligatoire
    if "Id" not in company_columns:
        raise RuntimeError(
            "Companies ne contient pas la colonne Id."
        )

    # Nom : on utilise uniquement une colonne réellement présente
    name_col = None

    for candidate in [
        "Name",
        "CompanyName",
        "LegalName",
        "DisplayName"
    ]:
        if candidate in company_columns:
            name_col = candidate
            break

    if name_col is None:
        logger.warning(
            "Aucune colonne nom trouvée dans Companies. "
            "Le nom client sera vide."
        )

        clients = (
            companies
            .select(
                F.col("Id").cast("int").alias("client_id")
            )
            .distinct()
            .withColumn("nom", F.lit(""))
        )

    else:

        clients = (
            companies
            .select(
                F.col("Id").cast("int").alias("client_id"),
                F.col(name_col).cast("string").alias("nom")
            )
            .distinct()
        )

    # Sector uniquement si présent
    if "Sector" in company_columns:
        clients = clients.withColumn(
            "secteur",
            F.col("Sector").cast("string")
        )
    else:
        clients = clients.withColumn(
            "secteur",
            F.lit(None).cast("string")
        )

    # CabinetId uniquement si présent
    if "CabinetId" in company_columns:
        clients = clients.withColumn(
            "cabinet_id",
            F.col("CabinetId").cast("int")
        )
    else:
        clients = clients.withColumn(
            "cabinet_id",
            F.lit(None).cast("int")
        )

    # Taille : uniquement si la source possède réellement une information
    if "Size" in company_columns:
        clients = clients.withColumn(
            "taille",
            F.col("Size").cast("string")
        )
    elif "CompanySize" in company_columns:
        clients = clients.withColumn(
            "taille",
            F.col("CompanySize").cast("string")
        )
    else:
        clients = clients.withColumn(
            "taille",
            F.lit(None).cast("string")
        )

    clients = clients.select(
        "client_id",
        "nom",
        "secteur",
        "taille",
        "cabinet_id"
    )

except Exception as e:

    logger.warning(
        "Impossible de construire dim_clients depuis Companies : "
        + str(e)
    )

    clients = (
        payroll
        .select(
            F.col("CompanyId").cast("int").alias("client_id")
        )
        .distinct()
        .withColumn("nom", F.lit(""))
        .withColumn("secteur", F.lit(None).cast("string"))
        .withColumn("taille", F.lit(None).cast("string"))
        .withColumn("cabinet_id", F.lit(None).cast("int"))
    )


logger.info(
    f"dim_clients : {clients.count()} lignes"
)


# ============================================================
# 6. DIM EMPLOYEES
# ============================================================

try:

    employees_src = read_sqlserver_table("Employees")

    employee_columns = employees_src.columns

    logger.info(
        "Colonnes Employees : "
        + ", ".join(employee_columns)
    )

    if "Id" not in employee_columns:
        raise RuntimeError(
            "Employees ne contient pas Id."
        )

    employees = (
        employees_src
        .select(
            F.col("Id").cast("int").alias("employee_id"),
            (
                F.col("CompanyId").cast("int")
                if "CompanyId" in employee_columns
                else F.lit(None).cast("int")
            ).alias("client_id")
        )
    )

    # --------------------------------------------------------
    # NOM
    # --------------------------------------------------------

    if "Name" in employee_columns:

        employees = employees.withColumn(
            "nom",
            F.col("Name").cast("string")
        )

    elif (
        "FirstName" in employee_columns
        and "LastName" in employee_columns
    ):

        employees = employees.withColumn(
            "nom",
            F.concat_ws(
                " ",
                F.col("FirstName"),
                F.col("LastName")
            )
        )

    elif "FirstName" in employee_columns:

        employees = employees.withColumn(
            "nom",
            F.col("FirstName").cast("string")
        )

    else:

        employees = employees.withColumn(
            "nom",
            F.lit("")
        )

    # --------------------------------------------------------
    # POSTE
    # --------------------------------------------------------

    if "JobPositionId" in employee_columns:

        employees = employees.withColumn(
            "poste",
            F.col("JobPositionId").cast("string")
        )

    elif "JobPosition" in employee_columns:

        employees = employees.withColumn(
            "poste",
            F.col("JobPosition").cast("string")
        )

    else:

        employees = employees.withColumn(
            "poste",
            F.lit(None).cast("string")
        )

    # --------------------------------------------------------
    # SERVICE
    # --------------------------------------------------------

    if "DepartementId" in employee_columns:

        employees = employees.withColumn(
            "service",
            F.col("DepartementId").cast("string")
        )

    elif "DepartmentId" in employee_columns:

        employees = employees.withColumn(
            "service",
            F.col("DepartmentId").cast("string")
        )

    else:

        employees = employees.withColumn(
            "service",
            F.lit(None).cast("string")
        )

    # --------------------------------------------------------
    # NIVEAU
    # --------------------------------------------------------

    if "CategoryId" in employee_columns:

        employees = employees.withColumn(
            "niveau",
            F.col("CategoryId").cast("string")
        )

    else:

        employees = employees.withColumn(
            "niveau",
            F.lit(None).cast("string")
        )

    # --------------------------------------------------------
    # DATE EMBAUCHE
    # --------------------------------------------------------

    if "DateEmbauche" in employee_columns:

        employees = employees.withColumn(
            "date_embauche",
            F.to_date(F.col("DateEmbauche"))
        )

    elif "HireDate" in employee_columns:

        employees = employees.withColumn(
            "date_embauche",
            F.to_date(F.col("HireDate"))
        )

    elif "CreatedAt" in employee_columns:

        employees = employees.withColumn(
            "date_embauche",
            F.to_date(F.col("CreatedAt"))
        )

    else:

        employees = employees.withColumn(
            "date_embauche",
            F.lit(None).cast("date")
        )

    employees = employees.select(
        "employee_id",
        "client_id",
        "nom",
        "poste",
        "niveau",
        "service",
        "date_embauche"
    ).dropDuplicates(
        ["employee_id"]
    )

except Exception as e:

    logger.warning(
        "dim_employees construite avec les données PayrollResults : "
        + str(e)
    )

    employees = (
        payroll
        .select(
            F.col("EmployeeId").cast("int").alias("employee_id"),
            F.col("CompanyId").cast("int").alias("client_id")
        )
        .distinct()
        .withColumn("nom", F.lit(""))
        .withColumn("poste", F.lit(None).cast("string"))
        .withColumn("niveau", F.lit(None).cast("string"))
        .withColumn("service", F.lit(None).cast("string"))
        .withColumn("date_embauche", F.lit(None).cast("date"))
    )


logger.info(
    f"dim_employees : {employees.count()} lignes"
)


# ============================================================
# 7. FACT BULLETIN LIGNE
# ============================================================

logger.info("=" * 80)
logger.info("CONSTRUCTION fact_bulletin_ligne")
logger.info("=" * 80)


component_mapping = [

    (1,  "SalaireBase"),

    (2,  "HeuresSupp25"),
    (3,  "HeuresSupp50"),
    (4,  "HeuresSupp100"),

    (5,  "Conges"),
    (6,  "JoursFeries"),

    (7,  "PrimeAnciennete"),
    (8,  "PrimeImposable1"),
    (9,  "PrimeImposable2"),
    (10, "PrimeImposable3"),
    (11, "TotalPrimesImposables"),

    (12, "TotalBrut"),

    (13, "FraisProfessionnels"),
    (14, "IndemniteRepresentation"),
    (15, "PrimeTransport"),
    (16, "PrimePanier"),
    (17, "IndemniteDeplacement"),
    (18, "IndemniteCaisse"),
    (19, "PrimeSalissure"),
    (20, "GratificationsFamilial"),
    (21, "PrimeVoyageMecque"),
    (22, "IndemniteLicenciement"),
    (23, "IndemniteKilometrique"),
    (24, "PrimeTourne"),
    (25, "PrimeOutillage"),
    (26, "AideMedicale"),
    (27, "AutresPrimesNonImposable"),

    (28, "TotalIndemnites"),
    (29, "TotalNiExcedentImposable"),

    (30, "CnssPartSalariale"),
    (31, "CnssBase"),

    (32, "CimrPartSalariale"),
    (33, "CimrBase"),

    (34, "AmoPartSalariale"),
    (35, "AmoBase"),

    (36, "MutuellePartSalariale"),
    (37, "MutuelleBase"),

    (38, "TotalCotisationsSalariales"),

    (39, "CnssPartPatronale"),
    (40, "CimrPartPatronale"),
    (41, "AmoPartPatronale"),
    (42, "MutuellePartPatronale"),

    (43, "TotalCotisationsPatronales"),

    (44, "ImpotRevenu"),
    (45, "IrTaux"),

    (46, "Arrondi"),
    (47, "AvanceSurSalaire"),
    (48, "InteretSurLogement"),

    (49, "BrutImposable"),
    (50, "NetImposable"),

    (51, "TotalGains"),
    (52, "TotalRetenues"),

    (53, "NetAPayer"),
    (54, "TotalNet"),
    (55, "TotalNet2")
]


fact_parts = []


for component_id, source_column in component_mapping:

    if source_column not in payroll.columns:

        logger.warning(
            f"Colonne absente, ignorée : {source_column}"
        )

        continue

    part = (
        payroll
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

            F.col("Month")
            .cast("int")
            .alias("mois"),

            F.col("Year")
            .cast("int")
            .alias("annee"),

            (
                F.col("PayHalf").cast("int")
                if "PayHalf" in payroll.columns
                else F.lit(None).cast("int")
            ).alias("pay_half"),

            F.col("Status")
            .cast("int")
            .alias("status"),

            F.col(source_column)
            .cast("decimal(18,2)")
            .alias("valeur"),

            F.lit(component_id)
            .cast("int")
            .alias("composant_id"),

            F.lit(source_column)
            .alias("source_column"),

            (
                F.col("ImportSource").cast("string")
                if "ImportSource" in payroll.columns
                else F.lit(None).cast("string")
            ).alias("import_source")
        )
        .filter(
            F.col("valeur").isNotNull()
        )
        .filter(
            F.col("valeur") != 0
        )
    )

    fact_parts.append(part)


if not fact_parts:

    raise RuntimeError(
        "Aucun composant de paie n'a pu être construit."
    )


fact_bulletin = fact_parts[0]

for part in fact_parts[1:]:

    fact_bulletin = fact_bulletin.unionByName(part)


# ============================================================
# DATE KEY
# ============================================================

fact_bulletin = (
    fact_bulletin
    .withColumn(
        "calendar_key",
        F.concat(
            F.col("annee"),
            F.lpad(F.col("mois"), 2, "0"),
            F.lit("01")
        ).cast("int")
    )
    .withColumn(
        "unite",
        F.lit("valeur_source")
    )
    .withColumn(
        "created_at",
        F.current_timestamp()
    )
    .select(
        "payroll_result_id",
        "client_id",
        "employee_id",
        "calendar_key",
        "composant_id",
        "mois",
        "annee",
        "valeur",
        "unite",
        "status",
        "pay_half",
        "source_column",
        "import_source",
        "created_at"
    )
)


logger.info(
    f"fact_bulletin_ligne : {fact_bulletin.count()} lignes"
)


# ============================================================
# 8. TABLES DE FAITS DONT LES SOURCES DOIVENT ETRE VALIDEES
# ============================================================
#
# On crée les tables correctement mais on ne fabrique PAS
# de données fictives.
#
# Cela respecte :
#     "sans inventer de données"
#
# Les modules correspondants seront alimentés lorsque les
# tables sources PayZenHR seront identifiées.
# ============================================================

fact_echeance = spark.createDataFrame(
    [],
    """
    client_id INT,
    calendar_key INT,
    echeance_id INT,
    date_echeance DATE,
    date_limite DATE,
    statut STRING,
    montant DECIMAL(18,2),
    created_at TIMESTAMP
    """
)

fact_absence = spark.createDataFrame(
    [],
    """
    employee_id INT,
    client_id INT,
    calendar_key INT,
    type_absence STRING,
    date_debut DATE,
    date_fin DATE,
    nombre_jours DECIMAL(18,2),
    statut STRING,
    created_at TIMESTAMP
    """
)

fact_variable_collecte = spark.createDataFrame(
    [],
    """
    client_id INT,
    calendar_key INT,
    canal STRING,
    date_envoi TIMESTAMP,
    statut STRING,
    delai_traitement DECIMAL(18,2),
    created_at TIMESTAMP
    """
)

fact_notes_frais = spark.createDataFrame(
    [],
    """
    client_id INT,
    employee_id INT,
    date_soumission DATE,
    calendar_key INT,
    montant DECIMAL(18,2),
    statut STRING,
    categorie STRING,
    created_at TIMESTAMP
    """
)

fact_evaluation = spark.createDataFrame(
    [],
    """
    client_id INT,
    employee_id INT,
    calendar_key INT,
    date_evaluation DATE,
    score DECIMAL(18,2),
    type_evaluation STRING,
    statut STRING,
    created_at TIMESTAMP
    """
)


# ============================================================
# 9. DIM ECHEANCE LEGALE
# ============================================================

echeance = spark.createDataFrame(
    [
        (1, "CNSS", "Mensuelle", "Selon calendrier légal CNSS"),
        (2, "CIMR", "Mensuelle", "Selon calendrier légal CIMR"),
        (3, "IR",   "Mensuelle", "Selon calendrier fiscal"),
        (4, "AMO",  "Mensuelle", "Selon calendrier légal AMO")
    ],
    """
    echeance_id INT,
    type STRING,
    periodicite STRING,
    regle_date_limite STRING
    """
)


# ============================================================
# 10. FONCTION INSERT CLICKHOUSE
# ============================================================

def write_clickhouse(df, table):

    count = df.count()

    logger.info(
        f"INSERT ClickHouse : {table} -> {count} lignes"
    )

    if count == 0:

        logger.info(
            f"{table} vide : aucune insertion."
        )

        return

    (
        df.write
        .mode("append")
        .jdbc(
            url=CLICKHOUSE_URL,
            table=f"payzen_bi.{table}",
            properties=CLICKHOUSE_PROPS
        )
    )


# ============================================================
# 11. CHARGEMENT
# ============================================================

write_clickhouse(
    clients,
    "dim_clients"
)

write_clickhouse(
    employees,
    "dim_employees"
)

write_clickhouse(
    calendar,
    "dim_calendar"
)

write_clickhouse(
    echeance,
    "dim_echeance_legale"
)

write_clickhouse(
    fact_bulletin,
    "fact_bulletin_ligne"
)

write_clickhouse(
    fact_echeance,
    "fact_echeance"
)

write_clickhouse(
    fact_absence,
    "fact_absence"
)

write_clickhouse(
    fact_variable_collecte,
    "fact_variable_collecte"
)

write_clickhouse(
    fact_notes_frais,
    "fact_notes_frais"
)

write_clickhouse(
    fact_evaluation,
    "fact_evaluation"
)


# ============================================================
# 12. CONTROLES
# ============================================================

logger.info("=" * 80)
logger.info("CONTROLES FINAUX")
logger.info("=" * 80)


logger.info(
    f"dim_clients             = {clients.count()}"
)

logger.info(
    f"dim_employees           = {employees.count()}"
)

logger.info(
    f"dim_calendar            = {calendar.count()}"
)

logger.info(
    f"dim_composant_paie     = {len(component_mapping)} composants"
)

logger.info(
    f"dim_echeance_legale     = {echeance.count()}"
)

logger.info(
    f"fact_bulletin_ligne     = {fact_bulletin.count()}"
)


# ============================================================
# CONTROLE GRAIN
# ============================================================

duplicates = (
    fact_bulletin
    .groupBy(
        "payroll_result_id",
        "employee_id",
        "client_id",
        "calendar_key",
        "composant_id"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicates.count()

logger.info(
    f"Doublons grain fact_bulletin_ligne = {duplicate_count}"
)

if duplicate_count > 0:

    logger.error(
        "ERREUR : le grain composant x employé x mois contient des doublons."
    )

else:

    logger.info(
        "OK : aucun doublon sur le grain."
    )


# ============================================================
# CONTROLE EMPLOYES
# ============================================================

missing_employees = (
    fact_bulletin
    .select("employee_id")
    .distinct()
    .join(
        employees.select("employee_id").distinct(),
        "employee_id",
        "left_anti"
    )
    .count()
)

logger.info(
    f"Employés absents de dim_employees = {missing_employees}"
)


# ============================================================
# CONTROLE CLIENTS
# ============================================================

missing_clients = (
    fact_bulletin
    .select("client_id")
    .distinct()
    .join(
        clients.select("client_id").distinct(),
        "client_id",
        "left_anti"
    )
    .count()
)

logger.info(
    f"Clients absents de dim_clients = {missing_clients}"
)


logger.info("=" * 80)
logger.info("ETL PAYZEN HR V3 TERMINE")
logger.info("=" * 80)

spark.stop()