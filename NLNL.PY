import os
import sys
import urllib.request
import urllib.parse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# PAYZENHR - ETL SOCLE BI
# REBUILD COMPLET DU SOCLE POUR CLICKHOUSE
#
# Objectif :
#
#   Portefeuille
#       ↓
#   Client
#       ↓
#   Service
#       ↓
#   Catégorie composant
#       ↓
#   Composant
#       ↓
#   Collaborateur
#
# Grain de fact_bulletin_ligne :
#
#   1 ligne = 1 composant de paie
#            1 collaborateur
#            1 mois
#
# ============================================================


# ============================================================
# 0. CONFIGURATION
# ============================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("=" * 80)
print("PAYZENHR - REBUILD COMPLET DU SOCLE BI")
print("=" * 80)


# ------------------------------------------------------------
# SQL SERVER
# ------------------------------------------------------------

SQLSERVER_URL = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

SQLSERVER_PROPERTIES = {
    "user": "sa",
    "password": os.environ.get(
        "PAYZENHR_SQL_PASSWORD",
        "noraNORA123"
    ),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


# ------------------------------------------------------------
# CLICKHOUSE
# ------------------------------------------------------------

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8124
CLICKHOUSE_DATABASE = "payzen"

CLICKHOUSE_HTTP_URL = (
    f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
)

CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
    f"/{CLICKHOUSE_DATABASE}"
)

CLICKHOUSE_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": os.environ.get(
        "PAYZENHR_CLICKHOUSE_PASSWORD",
        "1234"
    ),
}


# ------------------------------------------------------------
# CABINET
# ------------------------------------------------------------

CABINET_ID = 30


# ============================================================
# 1. FONCTION CLICKHOUSE : EXECUTER DU SQL
# ============================================================

def clickhouse_sql(sql):
    """
    Exécute une requête SQL directement via l'API HTTP ClickHouse.
    Permet notamment de DROP / CREATE DATABASE.
    """

    params = {
        "user": CLICKHOUSE_PROPERTIES["user"],
        "password": CLICKHOUSE_PROPERTIES["password"],
    }

    url = CLICKHOUSE_HTTP_URL + "/?" + urllib.parse.urlencode(params)

    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = response.read().decode("utf-8")
            return result

    except Exception as e:
        print("\n❌ ERREUR CLICKHOUSE")
        print(e)
        raise


# ============================================================
# 2. RESET COMPLET DE LA BASE CLICKHOUSE
# ============================================================

print("\n" + "=" * 80)
print("RESET COMPLET DE CLICKHOUSE")
print("=" * 80)

print(f"Suppression de la base : {CLICKHOUSE_DATABASE}")

clickhouse_sql(
    f"DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE}"
)

print("✓ Ancienne base supprimée")

print(f"Création de la base : {CLICKHOUSE_DATABASE}")

clickhouse_sql(
    f"CREATE DATABASE {CLICKHOUSE_DATABASE}"
)

print("✓ Nouvelle base créée")

print("\n⚠️ CLICKHOUSE EST MAINTENANT VIDE.")
print("Le chargement va repartir de zéro.")


# ============================================================
# 3. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_Rebuild_Socle_BI")

    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar,"
        r"E:\etape_1\PayZenHR_ETL\jars\clickhouse-jdbc-0.9.8-all.jar"
    )

    .config("spark.sql.shuffle.partitions", "4")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 4. LECTURE SQL SERVER
# ============================================================

def read_sqlserver(table_name):

    print(f"\nLecture SQL Server : {table_name}")

    df = spark.read.jdbc(
        url=SQLSERVER_URL,
        table=f"dbo.{table_name}",
        properties=SQLSERVER_PROPERTIES
    )

    count = df.count()

    print(f"  ✓ {count} lignes")

    return df


# ============================================================
# 5. EXTRACTION
# ============================================================

print("\n" + "=" * 80)
print("EXTRACTION SQL SERVER")
print("=" * 80)


companies_df = read_sqlserver("Companies")

employees_df = read_sqlserver("Employees")

departements_df = read_sqlserver("Departements")

job_positions_df = read_sqlserver("JobPositions")

employee_contracts_df = read_sqlserver("EmployeeContracts")

payroll_df = read_sqlserver("PayrollResults")


# ============================================================
# 6. DIM_CLIENTS
# ============================================================

print("\n" + "=" * 80)
print("CONSTRUCTION DIM_CLIENTS")
print("=" * 80)


clients_df = (
    companies_df
    .filter(
        F.col("ManagedByCompanyId") == CABINET_ID
    )
)


dim_clients_df = (
    clients_df
    .select(
        F.col("Id").alias("client_id"),

        F.col("CompanyName").alias("nom"),

        F.when(
            F.trim(F.col("BusinessSector")) == "",
            None
        )
        .otherwise(F.col("BusinessSector"))
        .alias("secteur"),

        # Pas de colonne source fiable pour taille
        F.lit(None)
        .cast("string")
        .alias("taille"),

        F.col("ManagedByCompanyId")
        .alias("cabinet_id"),
    )
)


print("\nAperçu DIM_CLIENTS")

dim_clients_df.show(
    50,
    truncate=False
)

print(
    "Nombre clients :",
    dim_clients_df.count()
)


# ============================================================
# 7. DIM_EMPLOYEES
# ============================================================

print("\n" + "=" * 80)
print("CONSTRUCTION DIM_EMPLOYEES")
print("=" * 80)


# ------------------------------------------------------------
# EmployeeContracts -> JobPositions
# ------------------------------------------------------------

contracts_with_position = (
    employee_contracts_df.alias("ec")

    .join(
        job_positions_df
        .select(
            F.col("Id").alias("jp_id"),
            F.col("Name").alias("poste_nom")
        )
        .alias("jp"),

        F.col("ec.JobPositionId")
        == F.col("jp.jp_id"),

        "left"
    )
)


# ------------------------------------------------------------
# Premier contrat = date d'embauche
# ------------------------------------------------------------

window_first = (
    Window
    .partitionBy("ec.EmployeeId")
    .orderBy(
        F.col("ec.StartDate").asc_nulls_last()
    )
)


first_contract_df = (
    contracts_with_position

    .withColumn(
        "rn",
        F.row_number().over(window_first)
    )

    .filter(
        F.col("rn") == 1
    )

    .select(
        F.col("ec.EmployeeId")
        .alias("employee_id_first"),

        F.col("ec.StartDate")
        .alias("date_embauche")
    )
)


# ------------------------------------------------------------
# Contrat actuel = poste actuel
# ------------------------------------------------------------

window_current = (
    Window
    .partitionBy("ec.EmployeeId")
    .orderBy(
        F.when(
            F.col("ec.EndDate").isNull(),
            0
        ).otherwise(1),

        F.col("ec.StartDate").desc_nulls_last()
    )
)


current_contract_df = (
    contracts_with_position

    .withColumn(
        "rn",
        F.row_number().over(window_current)
    )

    .filter(
        F.col("rn") == 1
    )

    .select(
        F.col("ec.EmployeeId")
        .alias("employee_id_current"),

        F.col("poste_nom")
        .alias("poste")
    )
)


# ------------------------------------------------------------
# DIM_EMPLOYEES
# ------------------------------------------------------------

dim_employees_df = (

    employees_df.alias("e")

    # Département
    .join(
        departements_df
        .select(
            F.col("Id").alias("departement_id_ref"),
            F.col("DepartementName").alias("service")
        )
        .alias("d"),

        F.col("e.DepartementId")
        == F.col("d.departement_id_ref"),

        "left"
    )

    # Date embauche
    .join(
        first_contract_df,

        F.col("e.Id")
        == F.col("employee_id_first"),

        "left"
    )

    # Poste
    .join(
        current_contract_df,

        F.col("e.Id")
        == F.col("employee_id_current"),

        "left"
    )

    # Ancienneté
    .withColumn(
        "anciennete_annees",

        F.when(
            F.col("date_embauche").isNull(),
            F.lit(None).cast("int")
        )

        .otherwise(
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("date_embauche")
                ) / 12
            ).cast("int")
        )
    )

    .select(

        F.col("e.Id")
        .alias("employee_id"),

        F.concat_ws(
            " ",
            F.col("e.FirstName"),
            F.col("e.LastName")
        )
        .alias("nom"),

        F.col("e.CompanyId")
        .alias("client_id"),

        F.col("e.DepartementId")
        .alias("departement_id"),

        F.col("service"),

        F.col("e.Matricule")
        .alias("matricule"),

        F.col("poste"),

        # Aucune source identifiée
        F.lit(None)
        .cast("string")
        .alias("niveau"),

        F.col("date_embauche"),

        F.col("anciennete_annees")
    )
)


print("\nAperçu DIM_EMPLOYEES")

dim_employees_df.show(
    50,
    truncate=False
)

print(
    "Nombre employés :",
    dim_employees_df.count()
)


# ============================================================
# 8. DIM_CALENDAR
# ============================================================

print("\n" + "=" * 80)
print("CONSTRUCTION DIM_CALENDAR")
print("=" * 80)


calendar_source_df = (

    payroll_df

    .select(
        F.col("Month").alias("mois"),
        F.col("Year").alias("annee")
    )

    .filter(
        F.col("mois").isNotNull()
        &
        F.col("annee").isNotNull()
    )

    .distinct()
)


dim_calendar_df = (

    calendar_source_df

    .withColumn(
        "date",

        F.to_date(
            F.concat_ws(
                "-",
                F.col("annee"),
                F.lpad(
                    F.col("mois").cast("string"),
                    2,
                    "0"
                ),
                F.lit("01")
            )
        )
    )

    .withColumn(
        "jour",
        F.lit(1)
    )

    .withColumn(
        "trimestre",
        F.quarter(F.col("date"))
    )

    .select(
        "date",
        "jour",
        "mois",
        "trimestre",
        "annee"
    )
)


dim_calendar_df.show(
    100,
    truncate=False
)

print(
    "Nombre périodes :",
    dim_calendar_df.count()
)


# ============================================================
# 9. DIM_COMPOSANT_PAIE
# ============================================================

print("\n" + "=" * 80)
print("CONSTRUCTION DIM_COMPOSANT_PAIE")
print("=" * 80)


# ============================================================
# COMPOSANTS ATOMIQUES
# ============================================================

COMPOSANTS_ATOMIQUES = {

    # --------------------------------------------------------
    # BRUT DE BASE
    # --------------------------------------------------------

    "SalaireBase":
        (
            "Salaire de base",
            "Brut de base"
        ),

    "Conges":
        (
            "Indemnité congés",
            "Brut de base"
        ),

    "JoursFeries":
        (
            "Jours fériés",
            "Brut de base"
        ),


    # --------------------------------------------------------
    # HEURES SUP
    # --------------------------------------------------------

    "HeuresSupp25":
        (
            "Heures supplémentaires 25%",
            "Heures sup"
        ),

    "HeuresSupp50":
        (
            "Heures supplémentaires 50%",
            "Heures sup"
        ),

    "HeuresSupp100":
        (
            "Heures supplémentaires 100%",
            "Heures sup"
        ),


    # --------------------------------------------------------
    # PRIMES
    # --------------------------------------------------------

    "PrimeAnciennete":
        (
            "Prime d'ancienneté",
            "Primes"
        ),

    "PrimeImposable1":
        (
            "Prime imposable 1",
            "Primes"
        ),

    "PrimeImposable2":
        (
            "Prime imposable 2",
            "Primes"
        ),

    "PrimeImposable3":
        (
            "Prime imposable 3",
            "Primes"
        ),

    "IndemniteRepresentation":
        (
            "Indemnité de représentation",
            "Primes"
        ),

    "PrimeTransport":
        (
            "Prime de transport",
            "Primes"
        ),

    "PrimePanier":
        (
            "Prime de panier",
            "Primes"
        ),

    "IndemniteDeplacement":
        (
            "Indemnité de déplacement",
            "Primes"
        ),

    "IndemniteCaisse":
        (
            "Indemnité de caisse",
            "Primes"
        ),

    "PrimeSalissure":
        (
            "Prime de salissure",
            "Primes"
        ),

    "GratificationsFamilial":
        (
            "Gratification familiale",
            "Primes"
        ),

    "PrimeVoyageMecque":
        (
            "Prime voyage Mecque",
            "Primes"
        ),

    "IndemniteLicenciement":
        (
            "Indemnité de licenciement",
            "Primes"
        ),

    "IndemniteKilometrique":
        (
            "Indemnité kilométrique",
            "Primes"
        ),

    "PrimeTourne":
        (
            "Prime de tournée",
            "Primes"
        ),

    "PrimeOutillage":
        (
            "Prime d'outillage",
            "Primes"
        ),

    "AideMedicale":
        (
            "Aide médicale",
            "Primes"
        ),

    "AutresPrimesNonImposable":
        (
            "Autres primes non imposables",
            "Primes"
        ),


    # --------------------------------------------------------
    # CHARGES PATRONALES
    # --------------------------------------------------------

    "CnssPartPatronale":
        (
            "CNSS — part patronale",
            "Charges patronales"
        ),

    "CimrPartPatronale":
        (
            "CIMR — part patronale",
            "Charges patronales"
        ),

    "AmoPartPatronale":
        (
            "AMO — part patronale",
            "Charges patronales"
        ),

    "MutuellePartPatronale":
        (
            "Mutuelle — part patronale",
            "Charges patronales"
        ),


    # --------------------------------------------------------
    # CHARGES SALARIALES
    # --------------------------------------------------------

    "CnssPartSalariale":
        (
            "CNSS — part salariale",
            "Charges salariales"
        ),

    "CimrPartSalariale":
        (
            "CIMR — part salariale",
            "Charges salariales"
        ),

    "AmoPartSalariale":
        (
            "AMO — part salariale",
            "Charges salariales"
        ),

    "MutuellePartSalariale":
        (
            "Mutuelle — part salariale",
            "Charges salariales"
        ),


    # --------------------------------------------------------
    # AUTRES
    # --------------------------------------------------------

    "AvanceSurSalaire":
        (
            "Avance sur salaire",
            "Déduction"
        ),

    "ImpotRevenu":
        (
            "Impôt sur le revenu",
            "Déduction"
        ),

    "FraisProfessionnels":
        (
            "Frais professionnels",
            "Abattement fiscal"
        ),

    "InteretSurLogement":
        (
            "Intérêt sur logement",
            "Avantage en nature"
        )
}


# ============================================================
# Vérification des colonnes réellement présentes
# ============================================================

payroll_columns = set(payroll_df.columns)

COMPOSANTS_VALIDES = {
    col: value
    for col, value in COMPOSANTS_ATOMIQUES.items()
    if col in payroll_columns
}

COMPOSANTS_ABSENTS = [
    col
    for col in COMPOSANTS_ATOMIQUES
    if col not in payroll_columns
]


print("\nComposants présents dans PayrollResults :")

for col, (libelle, categorie) in COMPOSANTS_VALIDES.items():

    print(
        f"  ✓ {col:<35} -> "
        f"{categorie}"
    )


if COMPOSANTS_ABSENTS:

    print("\n⚠️ Composants demandés mais absents de PayrollResults :")

    for col in COMPOSANTS_ABSENTS:
        print(f"  - {col}")


COMPOSANT_COLS = list(
    COMPOSANTS_VALIDES.keys()
)


# ============================================================
# Création DIM_COMPOSANT_PAIE
# ============================================================

values_sql = ", ".join(

    f"("
    f"'{c}', "
    f"'{lib.replace(chr(39), chr(39) + chr(39))}', "
    f"'{cat.replace(chr(39), chr(39) + chr(39))}'"
    f")"

    for c, (lib, cat)
    in COMPOSANTS_VALIDES.items()
)


dim_composant_paie_df = spark.sql(

    f"""
    SELECT *
    FROM VALUES
        {values_sql}
    AS t(
        composant_id,
        libelle,
        categorie
    )
    """
)


dim_composant_paie_df.show(
    100,
    truncate=False
)

print(
    "Nombre composants :",
    dim_composant_paie_df.count()
)


# ============================================================
# 10. DEDOUBLONNAGE PAYROLLRESULTS
# ============================================================

print("\n" + "=" * 80)
print("DEDOUBLONNAGE PAYROLLRESULTS")
print("=" * 80)


payroll_with_ts = (

    payroll_df

    .withColumn(
        "CreatedAt_ts",

        F.coalesce(

            F.to_timestamp(
                "CreatedAt",
                "yyyy-MM-dd HH:mm:ss.SSSSSSS XXX"
            ),

            F.to_timestamp("CreatedAt")
        )
    )
)


window_dedup = (

    Window

    .partitionBy(
        "EmployeeId",
        "Month",
        "Year"
    )

    .orderBy(

        F.col("CreatedAt_ts")
        .desc_nulls_last(),

        F.col("Id")
        .desc()
    )
)


payroll_dedup = (

    payroll_with_ts

    .withColumn(
        "rang",
        F.row_number().over(window_dedup)
    )
)


n_avant = payroll_dedup.count()


n_doublons = (

    payroll_dedup

    .filter(
        F.col("rang") > 1
    )

    .count()
)


payroll_dedup = (

    payroll_dedup

    .filter(
        F.col("rang") == 1
    )

    .drop(
        "rang",
        "CreatedAt_ts"
    )
)


n_apres = payroll_dedup.count()


print(
    f"Lignes avant      : {n_avant}"
)

print(
    f"Doublons supprimés: {n_doublons}"
)

print(
    f"Lignes après      : {n_apres}"
)


# ============================================================
# 11. FACT_BULLETIN_LIGNE
# ============================================================

print("\n" + "=" * 80)
print("CONSTRUCTION FACT_BULLETIN_LIGNE")
print("=" * 80)


# ------------------------------------------------------------
# UNPIVOT
# ------------------------------------------------------------

stack_args = ", ".join(

    f"'{col}', `{col}`"

    for col in COMPOSANT_COLS
)


stack_expr = (

    f"stack("
    f"{len(COMPOSANT_COLS)}, "
    f"{stack_args}"
    f") "
    f"as (composant_id, montant)"
)


fact_base_df = (

    payroll_dedup

    .select(

        "EmployeeId",
        "CompanyId",
        "Month",
        "Year",
        "Id",
        "Status",

        F.expr(stack_expr)
    )

    # On garde les 0
    # On supprime seulement les NULL
    .filter(
        F.col("montant").isNotNull()
    )
)


# ============================================================
# JOINTURE EMPLOYEES
# ============================================================

employees_for_fact = (

    dim_employees_df

    .select(

        F.col("employee_id")
        .alias("employee_id_dim"),

        F.col("nom")
        .alias("employee_name"),

        F.col("poste"),

        F.col("niveau"),

        F.col("service"),

        F.col("anciennete_annees")
    )
)


# ============================================================
# JOINTURE CLIENTS
# ============================================================

clients_for_fact = (

    dim_clients_df

    .select(

        F.col("client_id")
        .alias("client_id_dim"),

        F.col("nom")
        .alias("client_name"),

        F.col("secteur")
        .alias("client_secteur"),

        F.col("taille")
        .alias("client_taille")
    )
)


# ============================================================
# FACT FINAL
# ============================================================

fact_bulletin_ligne_df = (

    fact_base_df.alias("f")

    .join(
        employees_for_fact.alias("e"),

        F.col("f.EmployeeId")
        == F.col("e.employee_id_dim"),

        "left"
    )

    .join(
        clients_for_fact.alias("c"),

        F.col("f.CompanyId")
        == F.col("c.client_id_dim"),

        "inner"
    )

    .join(
        dim_composant_paie_df.alias("cp"),

        F.col("f.composant_id")
        == F.col("cp.composant_id"),

        "left"
    )

    .select(

        # ----------------------------------------------------
        # EMPLOYE
        # ----------------------------------------------------

        F.col("f.EmployeeId")
        .alias("employee_id"),

        F.col("e.employee_name"),

        F.col("e.poste"),

        F.col("e.niveau"),

        F.col("e.service"),

        F.col("e.anciennete_annees"),


        # ----------------------------------------------------
        # CLIENT
        # ----------------------------------------------------

        F.col("f.CompanyId")
        .alias("client_id"),

        F.col("c.client_name"),

        F.col("c.client_secteur"),

        F.col("c.client_taille"),


        # ----------------------------------------------------
        # COMPOSANT
        # ----------------------------------------------------

        F.col("f.composant_id"),

        F.col("cp.libelle")
        .alias("composant_libelle"),

        F.col("cp.categorie")
        .alias("composant_categorie"),


        # ----------------------------------------------------
        # MESURE
        # ----------------------------------------------------

        F.col("f.montant"),


        # ----------------------------------------------------
        # CALENDRIER
        # ----------------------------------------------------

        F.col("f.Month")
        .alias("mois"),

        F.col("f.Year")
        .alias("annee"),


        # ----------------------------------------------------
        # TRACE SOURCE
        # ----------------------------------------------------

        F.col("f.Id")
        .alias("payroll_result_id")
    )
)


print("\nAperçu FACT_BULLETIN_LIGNE")

fact_bulletin_ligne_df.show(
    100,
    truncate=False
)


fact_count = fact_bulletin_ligne_df.count()


print(
    f"\nNombre lignes fact_bulletin_ligne : {fact_count}"
)


# ============================================================
# 12. CONTROLE DU GRAIN
# ============================================================

print("\n" + "=" * 80)
print("CONTROLE DU GRAIN")
print("=" * 80)


duplicate_grain_df = (

    fact_bulletin_ligne_df

    .groupBy(
        "employee_id",
        "client_id",
        "mois",
        "annee",
        "composant_id"
    )

    .count()

    .filter(
        F.col("count") > 1
    )
)


duplicate_count = duplicate_grain_df.count()


if duplicate_count == 0:

    print(
        "✓ Grain valide : "
        "1 composant × 1 collaborateur × 1 mois"
    )

else:

    print(
        f"❌ {duplicate_count} doublons de grain détectés"
    )

    duplicate_grain_df.show(
        20,
        truncate=False
    )


# ============================================================
# 13. CONTROLE DES CATEGORIES
# ============================================================

print("\n" + "=" * 80)
print("CONTROLE CATEGORIES DASHBOARD")
print("=" * 80)


dashboard_categories = [
    "Brut de base",
    "Primes",
    "Heures sup",
    "Charges patronales",
    "Charges salariales"
]


dashboard_df = (

    fact_bulletin_ligne_df

    .filter(
        F.col("composant_categorie")
        .isin(dashboard_categories)
    )

    .groupBy(
        "client_id",
        "client_name",
        "service",
        "composant_categorie"
    )

    .agg(
        F.sum("montant")
        .alias("total_montant")
    )

    .orderBy(
        "client_name",
        "service",
        "composant_categorie"
    )
)


dashboard_df.show(
    200,
    truncate=False
)


# ============================================================
# 14. CONTROLE RECONCILIATION TOTALBRUT
# ============================================================

print("\n" + "=" * 80)
print("CONTROLE RECONCILIATION TOTALBRUT")
print("=" * 80)


brut_components = [

    col

    for col, (_, categorie)
    in COMPOSANTS_VALIDES.items()

    if categorie in [
        "Brut de base",
        "Primes",
        "Heures sup"
    ]
]


brut_recalcule_df = (

    fact_bulletin_ligne_df

    .filter(
        F.col("composant_id")
        .isin(brut_components)
    )

    .groupBy(
        "payroll_result_id"
    )

    .agg(
        F.sum("montant")
        .alias("brut_recalcule")
    )
)


reconciliation_df = (

    payroll_dedup

    .select(
        F.col("Id")
        .alias("payroll_result_id"),

        "TotalBrut"
    )

    .join(
        brut_recalcule_df,
        "payroll_result_id",
        "left"
    )

    .withColumn(
        "ecart",

        F.col("TotalBrut")
        -
        F.coalesce(
            F.col("brut_recalcule"),
            F.lit(0)
        )
    )
)


n_ecarts = (

    reconciliation_df

    .filter(
        F.abs(F.col("ecart")) > 0.01
    )

    .count()
)


n_total = reconciliation_df.count()


print(
    f"Bulletins avec écart : "
    f"{n_ecarts} / {n_total}"
)


if n_ecarts > 0:

    print("\n⚠️ Exemples d'écarts :")

    (
        reconciliation_df

        .filter(
            F.abs(F.col("ecart")) > 0.01
        )

        .show(
            20,
            truncate=False
        )
    )

else:

    print(
        "✓ Réconciliation TotalBrut OK"
    )


# ============================================================
# 15. COMPTAGES FINAUX AVANT CLICKHOUSE
# ============================================================

print("\n" + "=" * 80)
print("COMPTAGES DU SOCLE")
print("=" * 80)


count_clients = dim_clients_df.count()

count_employees = dim_employees_df.count()

count_calendar = dim_calendar_df.count()

count_components = dim_composant_paie_df.count()

count_fact = fact_bulletin_ligne_df.count()


print(
    f"dim_clients          : {count_clients}"
)

print(
    f"dim_employees        : {count_employees}"
)

print(
    f"dim_calendar         : {count_calendar}"
)

print(
    f"dim_composant_paie   : {count_components}"
)

print(
    f"fact_bulletin_ligne  : {count_fact}"
)


# ============================================================
# 16. CHARGEMENT CLICKHOUSE
# ============================================================

print("\n" + "=" * 80)
print("CHARGEMENT CLICKHOUSE")
print("=" * 80)


tables_to_load = [

    (
        dim_clients_df,
        "dim_clients"
    ),

    (
        dim_employees_df,
        "dim_employees"
    ),

    (
        dim_calendar_df,
        "dim_calendar"
    ),

    (
        dim_composant_paie_df,
        "dim_composant_paie"
    ),

    (
        fact_bulletin_ligne_df,
        "fact_bulletin_ligne"
    )
]


for df, table_name in tables_to_load:

    print(
        f"\nChargement : {table_name}"
    )

    (

        df

        .write

        .mode("append")

        .jdbc(
            url=CLICKHOUSE_JDBC_URL,
            table=table_name,
            properties=CLICKHOUSE_PROPERTIES
        )
    )

    print(
        f"✓ {table_name} chargée"
    )


# ============================================================
# 17. CONTROLE CLICKHOUSE
# ============================================================

print("\n" + "=" * 80)
print("CONTROLE CLICKHOUSE")
print("=" * 80)


for _, table_name in tables_to_load:

    result = clickhouse_sql(
        f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{table_name}"
    )

    print(
        f"{table_name:<30} : "
        f"{result.strip()} lignes"
    )


# ============================================================
# 18. FIN
# ============================================================

print("\n" + "=" * 80)
print("✓ ETL PAYZENHR TERMINE")
print("=" * 80)

print(
    """
SOCLE CONSTRUIT :

    dim_clients
        ↓
    dim_employees
        ↓
    dim_calendar
        ↓
    dim_composant_paie
        ↓
    fact_bulletin_ligne


GRAIN :

    1 composant
    × 1 collaborateur
    × 1 mois


HIERARCHIE DASHBOARD :

    Portefeuille
        ↓
    Client
        ↓
    Service
        ↓
    Catégorie de paie
        ↓
    Composant
        ↓
    Collaborateur


CATEGORIES DASHBOARD :

    ✓ Brut de base
    ✓ Primes
    ✓ Heures sup
    ✓ Charges patronales
    ✓ Charges salariales


CLICKHOUSE :

    Base payzen supprimée
    Base payzen recréée
    Tables rechargées depuis zéro
"""
)


spark.stop()