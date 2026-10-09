"""
================================================================================
PAYZENHR ETL V3.3
SQL Server (PayZenHR_Source)
        |
        v
     PySpark
        |
        v
ClickHouse (payzen_bi)
================================================================================

VERSION 3.3

CORRECTIONS PRINCIPALES
-----------------------

1. SQL SERVER = READ ONLY
   -> uniquement spark.read.jdbc()
   -> aucun INSERT / UPDATE / DELETE / DROP / TRUNCATE / ALTER.

2. CLICKHOUSE = INSERT / APPEND UNIQUEMENT
   -> aucune destruction de données.

3. CONTROLE CLICKHOUSE OBLIGATOIRE
   -> si la lecture des clés ClickHouse échoue :
      ARRET IMMEDIAT DE L'ETL.
   -> aucun chargement sans contrôle d'idempotence.

4. IDS STABLES POUR dim_composant_paie
   -> les IDs des composants fixes sont réservés.
   -> les composants variables utilisent un identifiant
      déterministe basé sur leur libellé.
   -> contrôle explicite des collisions.

5. ANCIENNETE
   -> anciennete_mois est calculée à partir de la date d'exécution
      du batch.
   -> la valeur ne dépend donc pas directement de current_date()
      au milieu du traitement.

6. GRAIN fact_bulletin_ligne

      1 employé
      x 1 année
      x 1 mois
      x 1 composant de paie

   IMPORTANT :
   client_id n'est PAS une partie du grain métier.
   source_ligne n'est PAS une partie du grain métier.

7. NULL DANS PAYROLLRESULTS
   -> NULL = aucune ligne.
   -> 0 = vraie valeur et génère une ligne.

8. PayrollResultPrimes
   -> intégré uniquement si PayrollResult parent existe.

9. Total* de PayrollResults
   -> non transformés en composants unitaires.

10. Aucune période artificielle
    -> les mois réellement présents dans PayrollResults sont conservés.
    -> aucun mois manquant n'est fabriqué pour un employé.

11. MOTS DE PASSE
    -> lecture depuis les variables d'environnement.
    -> valeurs par défaut conservées pour faciliter l'exécution locale.

12. TABLES CHARGEES

    DIMENSIONS :
      - dim_clients
      - dim_employees
      - dim_calendar
      - dim_composant_paie

    FAITS :
      - fact_bulletin_ligne
      - fact_absence

    Les autres tables prévues par le cahier des charges mais sans
    source exploitable ne sont PAS inventées.
================================================================================
"""


# ==============================================================================
# 0. IMPORTS
# ==============================================================================

import os
import sys
import logging
import hashlib

from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    DecimalType,
    DateType,
)


# ==============================================================================
# 1. PYTHON UTILISE PAR SPARK
# ==============================================================================

PYTHON_EXE = sys.executable

print("=" * 80)
print("PYTHON UTILISÉ PAR SPARK")
print("=" * 80)
print(PYTHON_EXE)

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE


# ==============================================================================
# 2. LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("payzenhr_etl_v3_3")


# ==============================================================================
# 3. RESUME EXECUTION
# ==============================================================================

SUMMARY = {
    "dimensions": {},
    "faits": {},

    "sources": {},

    "composants_catalogue": 0,

    "payrollresults_transformees": 0,
    "primes_integrees": 0,

    "doublons_batch": {},
    "doublons_cible": {},

    "relations_invalides": {},

    "non_disponibles": [],

    "volumes_sources": {},

    "controle_grain": {},

    "controle_clickhouse": {},

    "erreurs": [],
}


# ==============================================================================
# 4. CONFIGURATION
# ==============================================================================

CONFIG = {

    # --------------------------------------------------------------------------
    # SQL SERVER
    # --------------------------------------------------------------------------

    "sqlserver": {

        "host": os.environ.get(
            "SQLSERVER_HOST",
            "localhost"
        ),

        "port": os.environ.get(
            "SQLSERVER_PORT",
            ""
        ),

        "database": os.environ.get(
            "SQLSERVER_DATABASE",
            "PayZenHR_Source"
        ),

        "user": os.environ.get(
            "SQLSERVER_USER",
            ""
        ),
        

        "password": os.environ.get(
            "SQLSERVER_PASSWORD",
            ""
        ),
    },

    # --------------------------------------------------------------------------
    # CLICKHOUSE
    # --------------------------------------------------------------------------

    "clickhouse": {

        "host": os.environ.get(
            "CLICKHOUSE_HOST",
            "localhost"
        ),

        "port": os.environ.get(
            "CLICKHOUSE_PORT",
            ""
        ),

        "database": os.environ.get(
            "CLICKHOUSE_DATABASE",
            "payzen_bi"
        ),

        "user": os.environ.get(
            "CLICKHOUSE_USER",
            "default"
        ),

        "password": os.environ.get(
            "CLICKHOUSE_PASSWORD",
            ""
        ),
    },
}


# ==============================================================================
# 5. DATE DU BATCH
# ==============================================================================

# Une seule date est utilisée pour tout le batch.
# Cela évite que plusieurs transformations utilisent des dates différentes.

BATCH_DATE = date.today()

logger.info(
    f"Date du batch : {BATCH_DATE}"
)


# ==============================================================================
# 6. CHEMINS DES JARS
# ==============================================================================

PROJECT_ROOT = r"E:\etape_1\PayZenHR_ETL\pipeline_test\payzenhr_pyspark"

JARS_DIR = os.path.join(
    PROJECT_ROOT,
    "jars"
)

SQLSERVER_JDBC_JAR = (
    r"E:\etape_1\PayZenHR_ETL\jars"
    r"\mssql-jdbc-13.4.0.jre8.jar"
)

CLICKHOUSE_JDBC_JAR = (
    r"E:\etape_1\PayZenHR_ETL\jars"
    r"\clickhouse-jdbc-0.9.8-all.jar"
)


# ==============================================================================
# 7. VERIFICATION DES JARS
# ==============================================================================

def check_jars():

    logger.info("=" * 80)
    logger.info("VERIFICATION DES JARS")
    logger.info("=" * 80)

    logger.info(
        f"SQL Server JDBC : {SQLSERVER_JDBC_JAR}"
    )

    logger.info(
        f"ClickHouse JDBC : {CLICKHOUSE_JDBC_JAR}"
    )

    if not os.path.exists(SQLSERVER_JDBC_JAR):

        raise FileNotFoundError(
            "JAR SQL Server introuvable : "
            f"{SQLSERVER_JDBC_JAR}"
        )

    if not os.path.exists(CLICKHOUSE_JDBC_JAR):

        raise FileNotFoundError(
            "JAR ClickHouse introuvable : "
            f"{CLICKHOUSE_JDBC_JAR}"
        )

    logger.info("JAR SQL Server : OK")
    logger.info("JAR ClickHouse : OK")


# ==============================================================================
# 8. JDBC URL SQL SERVER
# ==============================================================================

def sqlserver_jdbc_url() -> str:

    c = CONFIG["sqlserver"]

    return (
        f"jdbc:sqlserver://"
        f"{c['host']}:{c['port']};"
        f"databaseName={c['database']};"
        f"encrypt=false;"
        f"trustServerCertificate=true;"
    )


# ==============================================================================
# 9. JDBC URL CLICKHOUSE
# ==============================================================================

def clickhouse_jdbc_url() -> str:

    c = CONFIG["clickhouse"]

    return (
        f"jdbc:clickhouse://"
        f"{c['host']}:{c['port']}/"
        f"{c['database']}"
    )


# ==============================================================================
# 10. SPARK SESSION
# ==============================================================================

def build_spark_session():

    python_exe = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    logger.info(
        f"Spark Python utilisé : {python_exe}"
    )

    logger.info(
        f"SQL Server JDBC JAR : {SQLSERVER_JDBC_JAR}"
    )

    logger.info(
        f"ClickHouse JDBC JAR : {CLICKHOUSE_JDBC_JAR}"
    )

    spark = (
        SparkSession.builder

        .appName(
            "PayZenHR_ETL_V3.3"
        )

        .master(
            "local[2]"
        )

        .config(
            "spark.jars",
            f"{SQLSERVER_JDBC_JAR},{CLICKHOUSE_JDBC_JDBC_JAR}"
            if False
            else f"{SQLSERVER_JDBC_JAR},{CLICKHOUSE_JDBC_JAR}"
        )

        .config(
            "spark.pyspark.python",
            python_exe
        )

        .config(
            "spark.pyspark.driver.python",
            python_exe
        )

        .config(
            "spark.sql.shuffle.partitions",
            "2"
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

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    return spark


# ==============================================================================
# 11. LECTURE SQL SERVER — READ ONLY
# ==============================================================================

def read_sqlserver_table(
    spark: SparkSession,
    table_name: str
) -> DataFrame:

    logger.info(
        f"[SQL SERVER][READ ONLY] Lecture : {table_name}"
    )

    c = CONFIG["sqlserver"]

    df = (
        spark.read.jdbc(
            url=sqlserver_jdbc_url(),
            table=table_name,
            properties={
                "user": c["user"],
                "password": c["password"],
                "driver": (
                    "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                )
            }
        )
    )

    n = df.count()

    SUMMARY["sources"][table_name] = n
    SUMMARY["volumes_sources"][table_name] = n

    logger.info(
        f"[SQL SERVER] {table_name} = {n} lignes"
    )

    if n == 0:

        logger.warning(
            f"[SQL SERVER] {table_name} est VIDE."
        )

    return df


# ==============================================================================
# 12. TEST DE CONNEXION CLICKHOUSE
# ==============================================================================

def test_clickhouse_connection(
    spark: SparkSession
):

    logger.info("=" * 80)
    logger.info("TEST CONNEXION CLICKHOUSE")
    logger.info("=" * 80)

    c = CONFIG["clickhouse"]

    query = (
        "(SELECT 1 AS connection_test) "
        "AS connection_test"
    )

    try:

        df = (
            spark.read.jdbc(
                url=clickhouse_jdbc_url(),
                table=query,
                properties={
                    "user": c["user"],
                    "password": c["password"],
                    "driver": (
                        "com.clickhouse.jdbc.ClickHouseDriver"
                    )
                }
            )
        )

        result = df.collect()

        if not result:

            raise RuntimeError(
                "ClickHouse a répondu mais aucun résultat n'a été retourné."
            )

        value = result[0]["connection_test"]

        if int(value) != 1:

            raise RuntimeError(
                "Test ClickHouse invalide."
            )

        SUMMARY["controle_clickhouse"][
            "connexion"
        ] = "OK"

        logger.info(
            "[CLICKHOUSE] Connexion VALIDÉE."
        )

    except Exception as e:

        SUMMARY["controle_clickhouse"][
            "connexion"
        ] = "ECHEC"

        logger.error(
            "[CLICKHOUSE] Impossible de se connecter."
        )

        logger.error(
            f"Cause : {e}"
        )

        raise RuntimeError(
            "ARRET ETL : connexion ClickHouse impossible."
        ) from e


# ==============================================================================
# 13. CLICKHOUSE — LECTURE DES CLES EXISTANTES
# ==============================================================================

def read_clickhouse_keys(
    spark: SparkSession,
    table_name: str,
    key_columns: list
) -> DataFrame:

    c = CONFIG["clickhouse"]

    columns_sql = ", ".join(
        [
            f"`{cname}`"
            for cname in key_columns
        ]
    )

    query = (
        f"(SELECT {columns_sql} "
        f"FROM {c['database']}.{table_name}) "
        f"AS existing_keys"
    )

    logger.info(
        "[CLICKHOUSE][READ] "
        f"Lecture des clés : {table_name}"
    )

    try:

        df = (
            spark.read.jdbc(
                url=clickhouse_jdbc_url(),
                table=query,
                properties={
                    "user": c["user"],
                    "password": c["password"],
                    "driver": (
                        "com.clickhouse.jdbc.ClickHouseDriver"
                    )
                }
            )
        )

        n = df.count()

        SUMMARY["controle_clickhouse"][
            f"cles_{table_name}"
        ] = "OK"

        logger.info(
            f"[CLICKHOUSE][READ] {table_name} : "
            f"{n} clés existantes"
        )

        return df

    except Exception as e:

        SUMMARY["controle_clickhouse"][
            f"cles_{table_name}"
        ] = "ECHEC"

        logger.error(
            "[CLICKHOUSE][READ] ECHEC lecture des clés."
        )

        logger.error(
            f"Table : {table_name}"
        )

        logger.error(
            f"Clés : {key_columns}"
        )

        logger.error(
            f"Cause : {e}"
        )

        # IMPORTANT :
        # On ne retourne PAS un DataFrame vide.
        # Cela empêcherait de désactiver silencieusement
        # le contrôle d'idempotence.

        raise RuntimeError(
            f"ARRET ETL : impossible de contrôler les clés "
            f"ClickHouse de {table_name}."
        ) from e


# ==============================================================================
# 14. ECRITURE CLICKHOUSE
# ==============================================================================

def write_clickhouse(
    df: DataFrame,
    table_name: str
) -> int:

    n = df.count()

    if n == 0:

        logger.info(
            f"[CLICKHOUSE] {table_name} : "
            "0 ligne -> écriture ignorée."
        )

        return 0

    c = CONFIG["clickhouse"]

    logger.info(
        "[CLICKHOUSE][INSERT] "
        f"{table_name} : {n} lignes"
    )

    (
        df.write.jdbc(
            url=clickhouse_jdbc_url(),
            table=table_name,
            mode="append",
            properties={
                "user": c["user"],
                "password": c["password"],
                "driver": (
                    "com.clickhouse.jdbc.ClickHouseDriver"
                ),
                "batchsize": "1000"
            }
        )
    )

    logger.info(
        f"[CLICKHOUSE][INSERT] {table_name} : "
        f"{n} lignes insérées."
    )

    return n


# ==============================================================================
# 15. ANTI-DOUBLONS BATCH
# ==============================================================================

def drop_duplicates_logged(
    df: DataFrame,
    subset: list,
    name: str
):

    logger.info(
        f"[DEDUP] {name} — "
        f"suppression des doublons sur {subset}"
    )

    before = df.count()

    result = df.dropDuplicates(
        subset
    )

    after = result.count()

    duplicates = before - after

    SUMMARY["doublons_batch"][name] = duplicates

    logger.info(
        f"[DEDUP] {name} — "
        f"{duplicates} doublon(s) supprimé(s)"
    )

    return result


# ==============================================================================
# 16. ANTI-DOUBLONS CONTRE CLICKHOUSE
# ==============================================================================

def remove_existing_clickhouse_rows(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    key_columns: list
) -> DataFrame:

    if df.count() == 0:

        SUMMARY["doublons_cible"][table_name] = 0

        return df

    # --------------------------------------------------------------------------
    # IMPORTANT :
    # Cette fonction arrête l'ETL si la lecture ClickHouse échoue.
    # --------------------------------------------------------------------------

    existing = read_clickhouse_keys(
        spark,
        table_name,
        key_columns
    )

    existing_count = existing.count()

    if existing_count == 0:

        SUMMARY["doublons_cible"][table_name] = 0

        logger.info(
            f"[IDEMPOTENCE] {table_name} : "
            "aucune clé existante."
        )

        return df

    existing_alias = existing.alias(
        "existing"
    )

    new_alias = df.alias(
        "new"
    )

    condition = None

    for col_name in key_columns:

        current = (
            F.col(
                f"new.{col_name}"
            )
            ==
            F.col(
                f"existing.{col_name}"
            )
        )

        if condition is None:

            condition = current

        else:

            condition = (
                condition & current
            )

    result = (
        new_alias
        .join(
            existing_alias,
            condition,
            "left_anti"
        )
    )

    before = df.count()

    after = result.count()

    ignored = before - after

    SUMMARY["doublons_cible"][table_name] = ignored

    if ignored > 0:

        logger.warning(
            f"[IDEMPOTENCE] {table_name} : "
            f"{ignored} ligne(s) déjà présentes "
            "dans ClickHouse ignorées."
        )

    else:

        logger.info(
            f"[IDEMPOTENCE] {table_name} : "
            "aucune ligne déjà présente."
        )

    return result


# ==============================================================================
# 17. VALIDATION FK
# ==============================================================================

def validate_fk(
    df: DataFrame,
    fk_column: str,
    dim_df: DataFrame,
    dim_key_column: str,
    table_name: str
) -> DataFrame:

    before = df.count()

    valid_keys = (
        dim_df
        .select(
            F.col(
                dim_key_column
            ).alias(
                "_valid_key"
            )
        )
        .filter(
            F.col("_valid_key").isNotNull()
        )
        .distinct()
    )

    result = (
        df.join(
            valid_keys,
            df[fk_column]
            ==
            valid_keys["_valid_key"],
            "left_semi"
        )
    )

    after = result.count()

    invalid = before - after

    key_name = (
        f"{table_name}.{fk_column}"
    )

    SUMMARY["relations_invalides"][
        key_name
    ] = invalid

    if invalid > 0:

        logger.warning(
            f"[FK] {key_name} : "
            f"{invalid} ligne(s) rejetées."
        )

    else:

        logger.info(
            f"[FK] {key_name} : "
            "relations valides."
        )

    return result


# ==============================================================================
# 18. VALIDATION VOLUME
# ==============================================================================

def validate_volume(
    df: DataFrame,
    table_name: str,
    minimum: int = 0
) -> bool:

    n = df.count()

    if n < minimum:

        logger.error(
            f"[VOLUME] {table_name} : "
            f"{n} < {minimum}"
        )

        return False

    logger.info(
        f"[VOLUME] {table_name} : "
        f"{n} lignes OK"
    )

    return True


# ==============================================================================
# 19. NON DISPONIBLE
# ==============================================================================

def mark_non_disponible(
    nom: str,
    raison: str
) -> None:

    message = (
        f"{nom} — {raison}"
    )

    SUMMARY["non_disponibles"].append(
        message
    )

    logger.warning(
        f"[NON DISPONIBLE] {message}"
    )


# ==============================================================================
# 20. DIM CLIENTS
# ==============================================================================

def build_dim_clients(
    companies: DataFrame,
    employees: DataFrame
) -> DataFrame:

    logger.info(
        "[DIM] Construction dim_clients"
    )

    dim = (
        companies
        .select(
            F.col("Id")
            .cast(IntegerType())
            .alias("client_id"),

            F.col("CompanyName")
            .alias("nom"),

            F.col("BusinessSector")
            .alias("secteur"),

            F.col("ManagedByCompanyId")
            .cast(IntegerType())
            .alias("cabinet_id"),
        )
    )

    employee_counts = (
        employees
        .groupBy("CompanyId")
        .agg(
            F.countDistinct("Id")
            .cast(IntegerType())
            .alias("taille")
        )
    )

    dim = (
        dim
        .join(
            employee_counts,
            dim["client_id"]
            ==
            F.col("CompanyId"),
            "left"
        )
        .drop("CompanyId")
    )

    dim = dim.withColumn(
        "taille",
        F.coalesce(
            F.col("taille"),
            F.lit(0)
        )
    )

    dim = drop_duplicates_logged(
        dim,
        ["client_id"],
        "dim_clients"
    )

    return dim


# ==============================================================================
# 21. DIM EMPLOYEES
# ==============================================================================

def build_dim_employees(
    employees: DataFrame,
    departements: DataFrame,
    contracts: DataFrame,
    job_positions: DataFrame,
    categories: DataFrame
) -> DataFrame:

    logger.info(
        "[DIM] Construction dim_employees"
    )

    # --------------------------------------------------------------------------
    # PREMIER CONTRAT = DATE D'EMBAUCHE
    # --------------------------------------------------------------------------

    date_embauche = (
        contracts
        .groupBy("EmployeeId")
        .agg(
            F.min("StartDate")
            .alias("date_embauche")
        )
    )

    # --------------------------------------------------------------------------
    # DERNIER CONTRAT CONNU
    # --------------------------------------------------------------------------

    w = (
        Window
        .partitionBy("EmployeeId")
        .orderBy(
            F.col("StartDate").desc()
        )
    )

    contrat_actuel = (
        contracts
        .withColumn(
            "_rn",
            F.row_number().over(w)
        )
        .filter(
            F.col("_rn") == 1
        )
        .select(
            "EmployeeId",
            "JobPositionId"
        )
    )

    # --------------------------------------------------------------------------
    # EMPLOYES
    # --------------------------------------------------------------------------

    emp = (
        employees
        .select(
            F.col("Id")
            .cast(IntegerType())
            .alias("employee_id"),

            F.concat_ws(
                " ",
                F.col("FirstName"),
                F.col("LastName")
            ).alias("nom"),

            F.col("DepartementId"),
            F.col("CategoryId")
        )
    )

    # --------------------------------------------------------------------------
    # JOINTURES
    # --------------------------------------------------------------------------

    dim = (

        emp

        .join(
            contrat_actuel,
            emp["employee_id"]
            ==
            contrat_actuel["EmployeeId"],
            "left"
        )

        .join(
            job_positions.select(
                F.col("Id")
                .alias("_job_id"),

                F.col("Name")
                .alias("poste")
            ),
            F.col("JobPositionId")
            ==
            F.col("_job_id"),
            "left"
        )

        .join(
            departements.select(
                F.col("Id")
                .alias("_dep_id"),

                F.col("DepartementName")
                .alias("service")
            ),
            F.col("DepartementId")
            ==
            F.col("_dep_id"),
            "left"
        )

        .join(
            categories.select(
                F.col("Id")
                .alias("_cat_id"),

                F.col("Name")
                .alias("categorie")
            ),
            F.col("CategoryId")
            ==
            F.col("_cat_id"),
            "left"
        )

        .join(
            date_embauche,
            emp["employee_id"]
            ==
            date_embauche["EmployeeId"],
            "left"
        )

        .withColumn(
            "niveau",
            F.lit(None)
            .cast(StringType())
        )

        # ----------------------------------------------------------------------
        # ANCIENNETE
        #
        # On utilise BATCH_DATE comme date de référence.
        # ----------------------------------------------------------------------

        .withColumn(
            "_batch_date",
            F.lit(
                BATCH_DATE
            ).cast(DateType())
        )

        .withColumn(
            "anciennete_mois",
            F.when(
                F.col("date_embauche").isNotNull(),

                F.floor(
                    F.months_between(
                        F.col("_batch_date"),
                        F.to_date(
                            F.col("date_embauche")
                        )
                    )
                ).cast(IntegerType())
            )
            .otherwise(
                F.lit(None)
                .cast(IntegerType())
            )
        )

        .select(
            "employee_id",
            "nom",
            "poste",
            "niveau",
            "categorie",
            "service",
            "date_embauche",
            "anciennete_mois"
        )
    )

    dim = drop_duplicates_logged(
        dim,
        ["employee_id"],
        "dim_employees"
    )

    return dim


# ==============================================================================
# 22. DIM CALENDAR
# ==============================================================================

def build_dim_calendar(
    spark: SparkSession,
    min_year: int,
    max_year: int
) -> DataFrame:

    logger.info(
        f"[DIM] Calendar : {min_year} -> {max_year}"
    )

    start = date(
        min_year,
        1,
        1
    )

    end = date(
        max_year,
        12,
        31
    )

    n_days = (
        end - start
    ).days + 1

    df = (
        spark.range(
            0,
            n_days
        )
        .withColumn(
            "date",
            F.date_add(
                F.lit(start.isoformat()),
                F.col("id").cast("int")
            )
        )
        .drop("id")
    )

    return (
        df

        .withColumn(
            "jour",
            F.dayofmonth("date")
        )

        .withColumn(
            "mois",
            F.month("date")
        )

        .withColumn(
            "trimestre",
            F.quarter("date")
        )

        .withColumn(
            "annee",
            F.year("date")
        )

        .withColumn(
            "jour_semaine",
            F.dayofweek("date")
        )
    )


# ==============================================================================
# 23. COMPOSANTS DE PAIE
# ==============================================================================

COMPOSANTS_PAYROLLRESULTS = [

    ("SalaireBase", "Gain"),

    ("HeuresSupp25", "Gain"),
    ("HeuresSupp50", "Gain"),
    ("HeuresSupp100", "Gain"),

    ("PrimeAnciennete", "Gain"),

    ("PrimeImposable1", "Gain"),
    ("PrimeImposable2", "Gain"),
    ("PrimeImposable3", "Gain"),

    ("FraisProfessionnels", "Gain"),

    ("IndemniteRepresentation", "Gain"),
    ("PrimeTransport", "Gain"),
    ("PrimePanier", "Gain"),
    ("IndemniteCaisse", "Gain"),
    ("PrimeSalissure", "Gain"),

    ("GratificationsFamilial", "Gain"),

    ("IndemniteKilometrique", "Gain"),

    ("PrimeTourne", "Gain"),
    ("PrimeOutillage", "Gain"),

    ("AideMedicale", "Gain"),

    ("AutresPrimesNonImposable", "Gain"),

    ("CnssPartSalariale", "Cotisation salariale"),
    ("CnssPartPatronale", "Cotisation patronale"),

    ("CimrPartSalariale", "Cotisation salariale"),
    ("CimrPartPatronale", "Cotisation patronale"),

    ("AmoPartSalariale", "Cotisation salariale"),
    ("AmoPartPatronale", "Cotisation patronale"),

    ("MutuellePartSalariale", "Cotisation salariale"),
    ("MutuellePartPatronale", "Cotisation patronale"),

    ("ImpotRevenu", "Retenue"),

    ("AvanceSurSalaire", "Retenue"),

    ("InteretSurLogement", "Retenue"),
]


# ==============================================================================
# 24. GENERATION ID STABLE
# ==============================================================================

def stable_component_id(code: str) -> int:
    """
    Génère un ID stable à partir du code du composant.

    Même code -> même ID entre deux exécutions.

    On utilise SHA-256 puis les 8 premiers caractères hexadécimaux.
    L'espace d'ID est limité à une plage positive afin de rester
    compatible avec IntegerType / ClickHouse Int32.
    """

    normalized = (
        str(code)
        .strip()
        .lower()
        .encode("utf-8")
    )

    digest = hashlib.sha256(
        normalized
    ).hexdigest()

    number = int(
        digest[:8],
        16
    )

    # Plage positive réservée aux composants variables.
    # Les IDs fixes sont 1..N.
    stable_id = (
        100000
        +
        (number % 1900000000)
    )

    return int(stable_id)


# ==============================================================================
# 25. DIM COMPOSANT PAIE
# ==============================================================================

def build_dim_composant_paie(
    spark: SparkSession,
    payroll_result_primes: DataFrame
) -> DataFrame:

    logger.info(
        "[DIM] Construction dim_composant_paie"
    )

    # ==========================================================================
    # 1. COMPOSANTS FIXES
    # ==========================================================================
    #
    # On construit le catalogue via Spark SQL et non via
    # spark.createDataFrame(liste_python).
    #
    # Cela évite de faire passer ce petit catalogue par le
    # Python Worker lors des actions Spark.
    # ==========================================================================

    fixed_rows = []

    for i, (column_name, category) in enumerate(
        COMPOSANTS_PAYROLLRESULTS,
        start=1
    ):

        fixed_rows.append(
            (
                i,
                column_name,
                column_name,
                category,
                "PayrollResults (colonne)"
            )
        )

    # Création JVM/Spark du catalogue fixe
    fixed_values = ", ".join(
        [
            "("
            f"{int(composant_id)}, "
            f"'{code.replace(chr(39), chr(39) + chr(39))}', "
            f"'{libelle.replace(chr(39), chr(39) + chr(39))}', "
            f"'{categorie.replace(chr(39), chr(39) + chr(39))}', "
            f"'{source.replace(chr(39), chr(39) + chr(39))}'"
            ")"
            for (
                composant_id,
                code,
                libelle,
                categorie,
                source
            ) in fixed_rows
        ]
    )

    fixed_df = spark.sql(
        f"""
        SELECT
            CAST(composant_id AS INT) AS composant_id,
            CAST(code AS STRING) AS code,
            CAST(libelle AS STRING) AS libelle,
            CAST(categorie AS STRING) AS categorie,
            CAST(source AS STRING) AS source
        FROM VALUES
            {fixed_values}
        AS t(
            composant_id,
            code,
            libelle,
            categorie,
            source
        )
        """
    )

    # ==========================================================================
    # 2. PRIMES VARIABLES
    # ==========================================================================

    if payroll_result_primes.limit(1).count() > 0:

        logger.info(
            "[DIM] Recherche des labels PayrollResultPrimes..."
        )

        labels = (
            payroll_result_primes
            .select(
                F.trim(
                    F.col("Label")
                ).alias("libelle")
            )
            .filter(
                F.col("libelle").isNotNull()
            )
            .filter(
                F.col("libelle") != ""
            )
            .distinct()
        )

        label_rows = [
            row["libelle"]
            for row in labels.collect()
        ]

        variable_rows = []

        used_ids = {
            row[0]
            for row in fixed_rows
        }

        for label in sorted(
            label_rows,
            key=lambda x: x.lower()
        ):

            variable_id = stable_component_id(label)

            # ------------------------------------------------------------------
            # Contrôle collision avec les IDs fixes
            # ------------------------------------------------------------------

            if variable_id in used_ids:

                raise ValueError(
                    "Collision ID composant détectée : "
                    f"label='{label}', "
                    f"id={variable_id}"
                )

            used_ids.add(variable_id)

            variable_rows.append(
                (
                    variable_id,
                    label,
                    label,
                    "Prime variable",
                    "PayrollResultPrimes.Label"
                )
            )

        if variable_rows:

            variable_values = ", ".join(
                [
                    "("
                    f"{int(composant_id)}, "
                    f"'{code.replace(chr(39), chr(39) + chr(39))}', "
                    f"'{libelle.replace(chr(39), chr(39) + chr(39))}', "
                    f"'Prime variable', "
                    f"'PayrollResultPrimes.Label'"
                    ")"
                    for (
                        composant_id,
                        code,
                        libelle,
                        _categorie,
                        _source
                    ) in variable_rows
                ]
            )

            variable_df = spark.sql(
                f"""
                SELECT
                    CAST(composant_id AS INT) AS composant_id,
                    CAST(code AS STRING) AS code,
                    CAST(libelle AS STRING) AS libelle,
                    CAST(categorie AS STRING) AS categorie,
                    CAST(source AS STRING) AS source
                FROM VALUES
                    {variable_values}
                AS t(
                    composant_id,
                    code,
                    libelle,
                    categorie,
                    source
                )
                """
            )

            dim = fixed_df.unionByName(
                variable_df
            )

        else:

            dim = fixed_df

    else:

        logger.warning(
            "[DIM] PayrollResultPrimes vide."
        )

        dim = fixed_df

    # ==========================================================================
    # 3. CONTROLE ID
    # ==========================================================================

    n = dim.count()

    SUMMARY["composants_catalogue"] = n

    logger.info(
        f"[DIM] dim_composant_paie : {n} composants"
    )

    duplicate_ids = (
        dim
        .groupBy("composant_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_ids > 0:

        logger.error(
            "[DIM] Collision d'ID détectée : "
            f"{duplicate_ids} ID(s)."
        )

        raise ValueError(
            "Violation d'unicité dans "
            "dim_composant_paie."
        )

    # ==========================================================================
    # 4. CONTROLE CODE
    # ==========================================================================

    duplicate_codes = (
        dim
        .groupBy("code")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_codes > 0:

        raise ValueError(
            "Violation d'unicité sur le code "
            "dans dim_composant_paie."
        )

    logger.info(
        "[DIM] dim_composant_paie : "
        "unicité ID + code VALIDEE."
    )

    return dim


# ==============================================================================
# 26. FACT BULLETIN LIGNE
# ==============================================================================

def build_fact_bulletin_ligne(
    payroll_results: DataFrame,
    payroll_result_primes: DataFrame,
    dim_composant_paie: DataFrame,
    dim_employees: DataFrame,
    dim_clients: DataFrame
) -> DataFrame:

    logger.info(
        "[FACT] Construction fact_bulletin_ligne"
    )

    # --------------------------------------------------------------------------
    # PayrollResults vide
    # --------------------------------------------------------------------------

    if payroll_results.count() == 0:

        mark_non_disponible(
            "fact_bulletin_ligne",
            "PayrollResults est vide."
        )

        return payroll_results.sparkSession.createDataFrame(
            [],
            StructType(
                [
                    StructField(
                        "employee_id",
                        IntegerType()
                    ),

                    StructField(
                        "client_id",
                        IntegerType()
                    ),

                    StructField(
                        "annee",
                        IntegerType()
                    ),

                    StructField(
                        "mois",
                        IntegerType()
                    ),

                    StructField(
                        "composant_id",
                        IntegerType()
                    ),

                    StructField(
                        "composant_code",
                        StringType()
                    ),

                    StructField(
                        "montant",
                        DecimalType(18, 2)
                    ),

                    StructField(
                        "source_ligne",
                        StringType()
                    ),
                ]
            )
        )

    # --------------------------------------------------------------------------
    # BASE PAYROLLRESULTS
    # --------------------------------------------------------------------------

    base = (
        payroll_results
        .select(

            F.col("Id")
            .cast(IntegerType())
            .alias("payroll_result_id"),

            F.col("EmployeeId")
            .cast(IntegerType())
            .alias("employee_id"),

            F.col("CompanyId")
            .cast(IntegerType())
            .alias("client_id"),

            F.col("Year")
            .cast(IntegerType())
            .alias("annee"),

            F.col("Month")
            .cast(IntegerType())
            .alias("mois"),

            *[
                F.col(column_name)
                for column_name, _
                in COMPOSANTS_PAYROLLRESULTS
            ]
        )
    )

    # --------------------------------------------------------------------------
    # UNPIVOT
    # --------------------------------------------------------------------------

    n_components = len(
        COMPOSANTS_PAYROLLRESULTS
    )

    stack_expression = ", ".join(
        [
            f"'{column_name}', `{column_name}`"
            for column_name, _
            in COMPOSANTS_PAYROLLRESULTS
        ]
    )

    unpivot = (
        base

        .selectExpr(
            "payroll_result_id",
            "employee_id",
            "client_id",
            "annee",
            "mois",

            f"""
            stack(
                {n_components},
                {stack_expression}
            ) AS (
                composant_code,
                montant
            )
            """
        )

        # NULL = aucune ligne.
        # 0 = vraie valeur.
        .filter(
            F.col("montant").isNotNull()
        )
    )

    # --------------------------------------------------------------------------
    # LOOKUP COMPOSANTS FIXES
    # --------------------------------------------------------------------------

    lookup_fixes = (
        dim_composant_paie

        .filter(
            F.col("source")
            ==
            "PayrollResults (colonne)"
        )

        .select(
            F.col("code")
            .alias("_fix_code"),

            F.col("composant_id")
            .alias("_fix_composant_id")
        )
    )

    fact_fixe = (
        unpivot

        .join(
            lookup_fixes,
            unpivot["composant_code"]
            ==
            lookup_fixes["_fix_code"],
            "inner"
        )

        .select(

            "employee_id",
            "client_id",
            "annee",
            "mois",

            F.col("_fix_composant_id")
            .alias("composant_id"),

            "composant_code",
            "montant"
        )

        .withColumn(
            "source_ligne",
            F.lit(
                "PayrollResults"
            )
        )
    )

    SUMMARY[
        "payrollresults_transformees"
    ] = fact_fixe.count()

    # --------------------------------------------------------------------------
    # PAYROLLRESULTPRIMES
    # --------------------------------------------------------------------------

    if payroll_result_primes.count() > 0:

        lookup_primes = (
            dim_composant_paie

            .filter(
                F.col("source")
                ==
                "PayrollResultPrimes.Label"
            )

            .select(
                F.col("code")
                .alias("_prime_code"),

                F.col("composant_id")
                .alias("_prime_composant_id")
            )
        )

        # ----------------------------------------------------------------------
        # Parent PayrollResult obligatoire.
        # ----------------------------------------------------------------------

        parent = (
            payroll_results

            .select(

                F.col("Id")
                .cast(IntegerType())
                .alias("_payroll_result_id"),

                F.col("EmployeeId")
                .cast(IntegerType())
                .alias("employee_id"),

                F.col("CompanyId")
                .cast(IntegerType())
                .alias("client_id"),

                F.col("Year")
                .cast(IntegerType())
                .alias("annee"),

                F.col("Month")
                .cast(IntegerType())
                .alias("mois")
            )
        )

        primes = (

            payroll_result_primes

            .select(

                F.col("PayrollResultId")
                .cast(IntegerType())
                .alias("payroll_result_id"),

                F.trim(
                    F.col("Label")
                ).alias("composant_code"),

                F.col("Montant")
                .cast(DecimalType(18, 2))
                .alias("montant")
            )

            .filter(
                F.col("montant").isNotNull()
            )

            .filter(
                F.col("composant_code").isNotNull()
            )

            # ------------------------------------------------------------------
            # Parent obligatoire.
            # ------------------------------------------------------------------

            .join(
                parent,
                F.col("payroll_result_id")
                ==
                F.col("_payroll_result_id"),
                "inner"
            )

            # ------------------------------------------------------------------
            # Lookup composant.
            # ------------------------------------------------------------------

            .join(
                lookup_primes,
                F.col("composant_code")
                ==
                F.col("_prime_code"),
                "inner"
            )

            .select(

                "employee_id",
                "client_id",
                "annee",
                "mois",

                F.col("_prime_composant_id")
                .alias("composant_id"),

                "composant_code",
                "montant"
            )

            .withColumn(
                "source_ligne",
                F.lit(
                    "PayrollResultPrimes"
                )
            )
        )

        SUMMARY[
            "primes_integrees"
        ] = primes.count()

        fact = (
            fact_fixe
            .unionByName(
                primes
            )
        )

    else:

        fact = fact_fixe

        SUMMARY[
            "primes_integrees"
        ] = 0

        logger.warning(
            "[FACT] PayrollResultPrimes vide."
        )

    # --------------------------------------------------------------------------
    # VALIDATION EMPLOYEE
    # --------------------------------------------------------------------------

    fact = validate_fk(
        fact,
        "employee_id",
        dim_employees,
        "employee_id",
        "fact_bulletin_ligne"
    )

    # --------------------------------------------------------------------------
    # VALIDATION CLIENT
    # --------------------------------------------------------------------------

    fact = validate_fk(
        fact,
        "client_id",
        dim_clients,
        "client_id",
        "fact_bulletin_ligne"
    )

    # --------------------------------------------------------------------------
    # GRAIN METIER
    #
    # IMPORTANT :
    # client_id n'est PAS inclus dans le grain.
    #
    # employee_id + annee + mois + composant_id
    # --------------------------------------------------------------------------

    fact = drop_duplicates_logged(
        fact,

        [
            "employee_id",
            "annee",
            "mois",
            "composant_id"
        ],

        "fact_bulletin_ligne"
    )

    # --------------------------------------------------------------------------
    # CONTROLE GRAIN
    # --------------------------------------------------------------------------

    total = fact.count()

    distinct_grain = (
        fact
        .select(
            "employee_id",
            "annee",
            "mois",
            "composant_id"
        )
        .distinct()
        .count()
    )

    SUMMARY[
        "controle_grain"
    ]["fact_bulletin_ligne"] = {
        "total": total,
        "grain_distinct": distinct_grain
    }

    logger.info(
        "[GRAIN] fact_bulletin_ligne : "
        f"total={total}, "
        f"grain_distinct={distinct_grain}"
    )

    if total != distinct_grain:

        raise ValueError(
            "Violation du grain "
            "fact_bulletin_ligne."
        )

    logger.info(
        "[GRAIN] Grain fact_bulletin_ligne "
        "VALIDÉ."
    )

    return fact


# ==============================================================================
# 27. FACT ABSENCE
# ==============================================================================

def build_fact_absence(
    employee_absences: DataFrame,
    dim_employees: DataFrame
) -> DataFrame:

    logger.info(
        "[FACT] Construction fact_absence"
    )

    if employee_absences.count() == 0:

        mark_non_disponible(
            "fact_absence",
            "EmployeeAbsences est vide."
        )

        return employee_absences.sparkSession.createDataFrame(
            [],
            StructType(
                [
                    StructField(
                        "absence_id",
                        IntegerType()
                    ),

                    StructField(
                        "employee_id",
                        IntegerType()
                    ),

                    StructField(
                        "date_absence",
                        DateType()
                    ),

                    StructField(
                        "type_absence",
                        StringType()
                    ),

                    StructField(
                        "duree_type",
                        IntegerType()
                    ),

                    StructField(
                        "statut",
                        IntegerType()
                    ),

                    StructField(
                        "motif",
                        StringType()
                    ),
                ]
            )
        )

    fact = (
        employee_absences

        .select(

            F.col("Id")
            .cast(IntegerType())
            .alias("absence_id"),

            F.col("EmployeeId")
            .cast(IntegerType())
            .alias("employee_id"),

            F.to_date(
                F.col("AbsenceDate")
            ).alias("date_absence"),

            F.col("AbsenceType")
            .alias("type_absence"),

            F.col("DurationType")
            .cast(IntegerType())
            .alias("duree_type"),

            F.col("Status")
            .cast(IntegerType())
            .alias("statut"),

            F.col("Reason")
            .alias("motif")
        )
    )

    fact = validate_fk(
        fact,
        "employee_id",
        dim_employees,
        "employee_id",
        "fact_absence"
    )

    fact = drop_duplicates_logged(
        fact,

        [
            "absence_id"
        ],

        "fact_absence"
    )

    return fact


# ==============================================================================
# 28. CHARGEMENT DIMENSION AVEC IDEMPOTENCE
# ==============================================================================

def load_dimension(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    key_columns: list
) -> int:

    logger.info("")
    logger.info(
        "=" * 80
    )

    logger.info(
        f"CHARGEMENT DIMENSION : {table_name}"
    )

    logger.info(
        f"Clé(s) : {key_columns}"
    )

    logger.info(
        "=" * 80
    )

    if not validate_volume(
        df,
        table_name
    ):

        SUMMARY[
            "dimensions"
        ][table_name] = 0

        return 0

    # --------------------------------------------------------------------------
    # CONTROLE CIBLE OBLIGATOIRE
    # --------------------------------------------------------------------------

    df_to_insert = (
        remove_existing_clickhouse_rows(
            spark,
            df,
            table_name,
            key_columns
        )
    )

    inserted = write_clickhouse(
        df_to_insert,
        table_name
    )

    SUMMARY[
        "dimensions"
    ][table_name] = inserted

    return inserted


# ==============================================================================
# 29. CHARGEMENT FAIT AVEC IDEMPOTENCE
# ==============================================================================

def load_fact(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    key_columns: list
) -> int:

    logger.info("")
    logger.info(
        "=" * 80
    )

    logger.info(
        f"CHARGEMENT FAIT : {table_name}"
    )

    logger.info(
        f"Clé(s) : {key_columns}"
    )

    logger.info(
        "=" * 80
    )

    if not validate_volume(
        df,
        table_name
    ):

        SUMMARY[
            "faits"
        ][table_name] = 0

        return 0

    # --------------------------------------------------------------------------
    # CONTROLE CIBLE OBLIGATOIRE
    # --------------------------------------------------------------------------

    df_to_insert = (
        remove_existing_clickhouse_rows(
            spark,
            df,
            table_name,
            key_columns
        )
    )

    inserted = write_clickhouse(
        df_to_insert,
        table_name
    )

    SUMMARY[
        "faits"
    ][table_name] = inserted

    return inserted


# ==============================================================================
# 30. RESUME FINAL
# ==============================================================================

def print_summary():

    logger.info("")
    logger.info("=" * 100)
    logger.info(
        "RESUME FINAL — PAYZENHR ETL V3.3"
    )
    logger.info("=" * 100)

    logger.info("")
    logger.info(
        "----- CONFIGURATION -----"
    )

    logger.info(
        f"Date batch      : {BATCH_DATE}"
    )

    logger.info(
        f"SQL Server      : {sqlserver_jdbc_url()}"
    )

    logger.info(
        f"ClickHouse      : {clickhouse_jdbc_url()}"
    )

    logger.info("")
    logger.info(
        "----- CONTROLE CLICKHOUSE -----"
    )

    for name, value in SUMMARY[
        "controle_clickhouse"
    ].items():

        logger.info(
            f"{name:<40} : {value}"
        )

    logger.info("")
    logger.info(
        "----- SOURCES SQL SERVER -----"
    )

    for name, count in SUMMARY[
        "sources"
    ].items():

        logger.info(
            f"{name:<40} : {count:>10} lignes"
        )

    logger.info("")
    logger.info(
        "----- DIMENSIONS CHARGEES -----"
    )

    for name, count in SUMMARY[
        "dimensions"
    ].items():

        logger.info(
            f"{name:<40} : {count:>10} lignes"
        )

    logger.info("")
    logger.info(
        "----- FAITS CHARGES -----"
    )

    for name, count in SUMMARY[
        "faits"
    ].items():

        logger.info(
            f"{name:<40} : {count:>10} lignes"
        )

    logger.info("")
    logger.info(
        "----- COMPOSANTS -----"
    )

    logger.info(
        "Catalogue composants              : "
        f"{SUMMARY['composants_catalogue']}"
    )

    logger.info(
        "PayrollResults transformées       : "
        f"{SUMMARY['payrollresults_transformees']}"
    )

    logger.info(
        "Primes intégrées                  : "
        f"{SUMMARY['primes_integrees']}"
    )

    logger.info("")
    logger.info(
        "----- DOUBLONS DANS LE BATCH -----"
    )

    for name, count in SUMMARY[
        "doublons_batch"
    ].items():

        logger.info(
            f"{name:<40} : {count} doublon(s)"
        )

    logger.info("")
    logger.info(
        "----- DOUBLONS DEJA PRESENTS "
        "DANS CLICKHOUSE -----"
    )

    for name, count in SUMMARY[
        "doublons_cible"
    ].items():

        logger.info(
            f"{name:<40} : {count} ligne(s) ignorée(s)"
        )

    logger.info("")
    logger.info(
        "----- RELATIONS INVALIDES -----"
    )

    for name, count in SUMMARY[
        "relations_invalides"
    ].items():

        logger.info(
            f"{name:<40} : {count} rejet(s)"
        )

    logger.info("")
    logger.info(
        "----- CONTROLE DU GRAIN -----"
    )

    for name, values in SUMMARY[
        "controle_grain"
    ].items():

        logger.info(
            f"{name}"
        )

        logger.info(
            f"  total          = {values['total']}"
        )

        logger.info(
            f"  grain distinct = {values['grain_distinct']}"
        )

    logger.info("")
    logger.info(
        "----- NON DISPONIBLE -----"
    )

    if SUMMARY[
        "non_disponibles"
    ]:

        for item in SUMMARY[
            "non_disponibles"
        ]:

            logger.info(
                f"  ❌ {item}"
            )

    else:

        logger.info(
            "  Aucun élément non disponible."
        )

    logger.info("")
    logger.info("=" * 100)


# ==============================================================================
# 31. MAIN
# ==============================================================================

def main():

    spark = None

    try:

        # ======================================================================
        # 1. JARS
        # ======================================================================

        check_jars()

        # ======================================================================
        # 2. SPARK
        # ======================================================================

        spark = build_spark_session()

        # ======================================================================
        # 3. TEST CLICKHOUSE
        #
        # AVANT DE LIRE LES SOURCES / CHARGER.
        # ======================================================================

        test_clickhouse_connection(
            spark
        )

        # ======================================================================
        # ETAPE A — EXTRACTION SQL SERVER
        # ======================================================================

        logger.info("")
        logger.info("=" * 100)
        logger.info(
            "ETAPE A — EXTRACTION SQL SERVER"
        )
        logger.info("=" * 100)

        companies = read_sqlserver_table(
            spark,
            "dbo.Companies"
        )

        employees = read_sqlserver_table(
            spark,
            "dbo.Employees"
        )

        departements = read_sqlserver_table(
            spark,
            "dbo.Departements"
        )

        contracts = read_sqlserver_table(
            spark,
            "dbo.EmployeeContracts"
        )

        job_positions = read_sqlserver_table(
            spark,
            "dbo.JobPositions"
        )

        categories = read_sqlserver_table(
            spark,
            "dbo.EmployeeCategories"
        )

        payroll_results = read_sqlserver_table(
            spark,
            "dbo.PayrollResults"
        )

        payroll_result_primes = read_sqlserver_table(
            spark,
            "dbo.PayrollResultPrimes"
        )

        employee_absences = read_sqlserver_table(
            spark,
            "dbo.EmployeeAbsences"
        )

        # ======================================================================
        # ETAPE B — ELEMENTS NON DISPONIBLES
        # ======================================================================

        logger.info("")
        logger.info("=" * 100)
        logger.info(
            "ETAPE B — CONTROLE DES SOURCES DISPONIBLES"
        )
        logger.info("=" * 100)

        mark_non_disponible(
            "dim_echeance_legale",
            "Aucune source exploitable ne porte directement "
            "le concept d'échéance légale."
        )

        mark_non_disponible(
            "fact_echeance",
            "Dépend de dim_echeance_legale."
        )

        mark_non_disponible(
            "fact_variable_collecte",
            "Aucune table source ne porte le concept de collecte "
            "des variables de paie."
        )

        mark_non_disponible(
            "fact_notes_frais",
            "Aucune table source dédiée aux notes de frais."
        )

        mark_non_disponible(
            "fact_evaluation",
            "Aucune table source dédiée aux évaluations."
        )

        # ======================================================================
        # ETAPE C — CONSTRUCTION DES DIMENSIONS
        # ======================================================================

        logger.info("")
        logger.info("=" * 100)
        logger.info(
            "ETAPE C — CONSTRUCTION DES DIMENSIONS"
        )
        logger.info("=" * 100)

        dim_clients = build_dim_clients(
            companies,
            employees
        )

        dim_employees = build_dim_employees(
            employees,
            departements,
            contracts,
            job_positions,
            categories
        )

        # ======================================================================
        # CALENDRIER
        # ======================================================================

        years = []

        # ----------------------------------------------------------------------
        # Années PayrollResults
        # ----------------------------------------------------------------------

        if payroll_results.count() > 0:

            payroll_years = (
                payroll_results

                .select(
                    F.col("Year")
                    .cast(IntegerType())
                    .alias("year")
                )

                .filter(
                    F.col("year").isNotNull()
                )

                .distinct()
                .collect()
            )

            years.extend(
                [
                    row["year"]
                    for row in payroll_years
                ]
            )

        # ----------------------------------------------------------------------
        # Années absences
        # ----------------------------------------------------------------------

        if employee_absences.count() > 0:

            absence_years = (
                employee_absences

                .select(
                    F.year(
                        F.col("AbsenceDate")
                    ).alias("year")
                )

                .filter(
                    F.col("year").isNotNull()
                )

                .distinct()
                .collect()
            )

            years.extend(
                [
                    row["year"]
                    for row in absence_years
                ]
            )

        # ----------------------------------------------------------------------
        # Construction calendrier
        # ----------------------------------------------------------------------

        if years:

            min_year = min(years)
            max_year = max(years)

            dim_calendar = build_dim_calendar(
                spark,
                min_year,
                max_year
            )

        else:

            mark_non_disponible(
                "dim_calendar",
                "Aucune année exploitable dans les sources."
            )

            dim_calendar = spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField(
                            "date",
                            DateType()
                        ),

                        StructField(
                            "jour",
                            IntegerType()
                        ),

                        StructField(
                            "mois",
                            IntegerType()
                        ),

                        StructField(
                            "trimestre",
                            IntegerType()
                        ),

                        StructField(
                            "annee",
                            IntegerType()
                        ),

                        StructField(
                            "jour_semaine",
                            IntegerType()
                        ),
                    ]
                )
            )

        # ======================================================================
        # COMPOSANTS
        # ======================================================================

        dim_composant_paie = (
            build_dim_composant_paie(
                spark,
                payroll_result_primes
            )
        )

        # ======================================================================
        # ETAPE D — CONSTRUCTION DES FAITS
        # ======================================================================

        logger.info("")
        logger.info("=" * 100)
        logger.info(
            "ETAPE D — CONSTRUCTION DES FAITS"
        )
        logger.info("=" * 100)

        fact_bulletin_ligne = (
            build_fact_bulletin_ligne(
                payroll_results,
                payroll_result_primes,
                dim_composant_paie,
                dim_employees,
                dim_clients
            )
        )

        fact_absence = (
            build_fact_absence(
                employee_absences,
                dim_employees
            )
        )

        # ======================================================================
        # ETAPE E — CHARGEMENT CLICKHOUSE
        # ======================================================================

        logger.info("")
        logger.info("=" * 100)
        logger.info(
            "ETAPE E — CHARGEMENT CLICKHOUSE"
        )
        logger.info("=" * 100)

        # ======================================================================
        # DIMENSIONS
        # ======================================================================

        load_dimension(
            spark,
            dim_clients,
            "dim_clients",
            [
                "client_id"
            ]
        )

        load_dimension(
            spark,
            dim_employees,
            "dim_employees",
            [
                "employee_id"
            ]
        )

        load_dimension(
            spark,
            dim_calendar,
            "dim_calendar",
            [
                "date"
            ]
        )

        load_dimension(
            spark,
            dim_composant_paie,
            "dim_composant_paie",
            [
                "composant_id"
            ]
        )

        # ======================================================================
        # FAITS
        # ======================================================================

        load_fact(
            spark,
            fact_bulletin_ligne,
            "fact_bulletin_ligne",
            [
                "employee_id",
                "annee",
                "mois",
                "composant_id"
            ]
        )

        load_fact(
            spark,
            fact_absence,
            "fact_absence",
            [
                "absence_id"
            ]
        )

        # ======================================================================
        # FIN
        # ======================================================================

        logger.info("")
        logger.info("=" * 100)
        logger.info(
            "ETL PAYZENHR V3.3 TERMINE AVEC SUCCES."
        )
        logger.info("=" * 100)

    except Exception as e:

        SUMMARY["erreurs"].append(
            str(e)
        )

        logger.exception(
            "ERREUR FATALE DANS L'ETL V3.3"
        )

        raise

    finally:

        print_summary()

        if spark is not None:

            spark.stop()

            logger.info(
                "SparkSession arrêtée."
            )


# ==============================================================================
# 32. ENTRY POINT
# ==============================================================================

if __name__ == "__main__":

    main()