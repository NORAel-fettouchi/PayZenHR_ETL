# -*- coding: utf-8 -*-

"""
====================================================================
PAYZENHR - ETL SOCLE BI V3 - VERSION CORRIGEE
====================================================================

OBJECTIF
--------
Construire le socle BI PayZenHR :

5 DIMENSIONS
------------
1. dim_clients
2. dim_employees
3. dim_calendar
4. dim_composant_paie
5. dim_echeance_legale

6 FAITS
-------
1. fact_bulletin_ligne
2. fact_echeance
3. fact_absence
4. fact_variable_collecte
5. fact_notes_frais
6. fact_evaluation

AUTRE TABLE
-----------
budget_client

IMPORTANT
---------
Aucune donnée fictive n'est générée.

Si une source SQL Server n'est pas identifiée,
la table correspondante est créée avec son schéma
mais reste vide.

CLICKHOUSE DOCKER
-----------------
Windows HTTP 8124 -> ClickHouse 8123
Windows Native 9001 -> ClickHouse 9000

Le script utilise l'API HTTP ClickHouse sur 8124
et le JDBC ClickHouse sur 8124.
====================================================================
"""


# ==================================================================
# 0. IMPORTS
# ==================================================================

import os
import sys
import urllib.request
import urllib.parse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==================================================================
# 1. CONFIGURATION PYTHON
# ==================================================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("=" * 90)
print("PAYZENHR - ETL SOCLE BI V3 - VERSION CORRIGEE")
print("=" * 90)


# ==================================================================
# 2. SQL SERVER
# ==================================================================

SQLSERVER_URL = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

SQLSERVER_PROPERTIES = {
    "user": os.environ.get(
        "PAYZENHR_SQL_USER",
        "sa"
    ),

    "password": os.environ.get(
        "PAYZENHR_SQL_PASSWORD",
        "noraNORA123"
    ),

    "driver":
        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


# ==================================================================
# 3. CLICKHOUSE
# ==================================================================

CLICKHOUSE_HOST = "localhost"

# Docker :
# localhost:8124 -> container:8123
CLICKHOUSE_HTTP_PORT = 8124

CLICKHOUSE_DATABASE = "payzen"

CLICKHOUSE_HTTP_URL = (
    f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}"
)

CLICKHOUSE_HTTP_PORT = 8124
CLICKHOUSE_NATIVE_PORT = 9001

CLICKHOUSE_HTTP_URL = (
    f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}"
)

CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://"
    f"{CLICKHOUSE_HOST}:{CLICKHOUSE_NATIVE_PORT}/"
    f"{CLICKHOUSE_DATABASE}"
)

CLICKHOUSE_PROPERTIES = {
    "driver":
        "com.clickhouse.jdbc.ClickHouseDriver",

    "user":
        os.environ.get(
            "PAYZENHR_CLICKHOUSE_USER",
            "default"
        ),

    "password":
        os.environ.get(
            "PAYZENHR_CLICKHOUSE_PASSWORD",
            "1234"
        ),
}


# ==================================================================
# 4. CABINET
# ==================================================================

CABINET_ID = 30


# ==================================================================
# 5. JDBC JARS
# ==================================================================

MSSQL_JAR = (
    r"E:\etape_1\PayZenHR_ETL\jars"
    r"\mssql-jdbc-13.4.0.jre8.jar"
)

CLICKHOUSE_JAR = (
    r"E:\etape_1\PayZenHR_ETL\jars"
    r"\clickhouse-jdbc-0.9.8-all.jar"
)


# ==================================================================
# 6. CLICKHOUSE HTTP
# ==================================================================

def clickhouse_sql(sql):

    params = {
        "user":
            CLICKHOUSE_PROPERTIES["user"],

        "password":
            CLICKHOUSE_PROPERTIES["password"],
    }

    url = (
        CLICKHOUSE_HTTP_URL
        + "/?"
        + urllib.parse.urlencode(params)
    )

    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        method="POST"
    )

    try:

        with urllib.request.urlopen(
            request,
            timeout=120
        ) as response:

            return response.read().decode("utf-8")

    except Exception as e:

        print("\n❌ ERREUR CLICKHOUSE")
        print(e)

        print("\nSQL :")
        print(sql)

        raise


# ==================================================================
# 7. TEST CLICKHOUSE
# ==================================================================

def check_clickhouse():

    print("\n" + "=" * 90)
    print("TEST CLICKHOUSE")
    print("=" * 90)

    result = clickhouse_sql(
        "SELECT 1"
    )

    if result.strip() != "1":

        raise RuntimeError(
            "ClickHouse ne répond pas correctement."
        )

    print(
        "✓ ClickHouse HTTP accessible sur "
        "localhost:8124"
    )


# ==================================================================
# 8. RESET CLICKHOUSE
# ==================================================================

def reset_clickhouse():

    print("\n" + "=" * 90)
    print("RESET CLICKHOUSE")
    print("=" * 90)

    clickhouse_sql(
        f"DROP DATABASE IF EXISTS "
        f"{CLICKHOUSE_DATABASE}"
    )

    print("✓ Ancienne base supprimée")

    clickhouse_sql(
        f"CREATE DATABASE "
        f"{CLICKHOUSE_DATABASE}"
    )

    print("✓ Base payzen créée")


# ==================================================================
# 9. SPARK
# ==================================================================

def create_spark():

    print("\n" + "=" * 90)
    print("DEMARRAGE SPARK")
    print("=" * 90)

    spark = (

        SparkSession.builder

        .appName(
            "PayZenHR_Socle_BI_V3"
        )

        .config(
            "spark.jars",
            f"{MSSQL_JAR},{CLICKHOUSE_JAR}"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "4"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("✓ Spark démarré")

    return spark


# ==================================================================
# 10. LECTURE SQL SERVER
# ==================================================================

def read_sqlserver(
    spark,
    table_name
):

    print(
        f"\nLecture SQL Server : "
        f"dbo.{table_name}"
    )

    df = spark.read.jdbc(

        url=SQLSERVER_URL,

        table=f"dbo.{table_name}",

        properties=SQLSERVER_PROPERTIES
    )

    count = df.count()

    print(
        f"✓ {table_name} : "
        f"{count} lignes"
    )

    return df


# ==================================================================
# 11. TEST EXISTENCE TABLE
# ==================================================================

def sqlserver_table_exists(
    spark,
    table_name
):

    try:

        spark.read.jdbc(

            url=SQLSERVER_URL,

            table=f"""
                (
                    SELECT TOP 1 *
                    FROM dbo.{table_name}
                ) AS t
            """,

            properties=SQLSERVER_PROPERTIES
        )

        return True

    except Exception:

        return False


# ==================================================================
# 12. DIM_CLIENTS
# ==================================================================

def build_dim_clients(
    companies_df
):

    print("\n" + "=" * 90)
    print("1/5 - DIM_CLIENTS")
    print("=" * 90)

    df = (

        companies_df

        .filter(
            F.col("ManagedByCompanyId")
            == CABINET_ID
        )

        .select(

            F.col("Id")
            .cast("int")
            .alias("client_id"),

            F.col("CompanyName")
            .alias("nom"),

            F.when(

                F.col(
                    "BusinessSector"
                ).isNull()

                |

                (
                    F.trim(
                        F.col("BusinessSector")
                    ) == ""
                ),

                F.lit(None)
            )

            .otherwise(
                F.trim(
                    F.col("BusinessSector")
                )
            )

            .alias("secteur"),

            F.lit(None)
            .cast("string")
            .alias("taille"),

            F.col("ManagedByCompanyId")
            .cast("int")
            .alias("cabinet_id")
        )

        .dropDuplicates(
            ["client_id"]
        )
    )

    print(
        "Nombre clients :",
        df.count()
    )

    df.show(
        20,
        truncate=False
    )

    return df


# ==================================================================
# 13. DIM_EMPLOYEES
# ==================================================================

def build_dim_employees(
    employees_df,
    departements_df,
    job_positions_df,
    employee_contracts_df
):

    print("\n" + "=" * 90)
    print("2/5 - DIM_EMPLOYEES")
    print("=" * 90)


    # --------------------------------------------------------------
    # CONTRATS + POSTES
    # --------------------------------------------------------------

    contracts = (

        employee_contracts_df.alias("ec")

        .join(

            job_positions_df

            .select(

                F.col("Id")
                .alias("jp_id"),

                F.col("Name")
                .alias("poste")

            )

            .alias("jp"),

            F.col("ec.JobPositionId")
            ==
            F.col("jp.jp_id"),

            "left"
        )
    )


    # --------------------------------------------------------------
    # PREMIER CONTRAT
    # --------------------------------------------------------------

    window_first = (

        Window

        .partitionBy(
            F.col("ec.EmployeeId")
        )

        .orderBy(
            F.col(
                "ec.StartDate"
            ).asc_nulls_last()
        )
    )


    first_contract_df = (

        contracts

        .withColumn(
            "rn_first",
            F.row_number().over(
                window_first
            )
        )

        .filter(
            F.col("rn_first") == 1
        )

        .select(

            F.col("ec.EmployeeId")
            .cast("int")
            .alias(
                "employee_id_first"
            ),

            F.col("ec.StartDate")
            .alias(
                "date_embauche"
            )
        )
    )


    # --------------------------------------------------------------
    # CONTRAT ACTUEL
    # --------------------------------------------------------------

    window_current = (

        Window

        .partitionBy(
            F.col("ec.EmployeeId")
        )

        .orderBy(

            F.when(
                F.col("ec.EndDate").isNull(),
                0
            )
            .otherwise(1),

            F.col(
                "ec.StartDate"
            ).desc_nulls_last()
        )
    )


    current_contract_df = (

        contracts

        .withColumn(
            "rn_current",
            F.row_number().over(
                window_current
            )
        )

        .filter(
            F.col("rn_current") == 1
        )

        .select(

            F.col("ec.EmployeeId")
            .cast("int")
            .alias(
                "employee_id_current"
            ),

            F.col("poste")
        )
    )


    # --------------------------------------------------------------
    # DEPARTEMENTS
    # --------------------------------------------------------------

    departments = (

        departements_df

        .select(

            F.col("Id")
            .cast("int")
            .alias(
                "departement_id_ref"
            ),

            F.col(
                "DepartementName"
            ).alias(
                "service"
            )
        )

        .alias("d")
    )


    # --------------------------------------------------------------
    # DIM EMPLOYEES
    # --------------------------------------------------------------

    df = (

        employees_df.alias("e")

        .join(

            departments,

            F.col(
                "e.DepartementId"
            ).cast("int")

            ==

            F.col(
                "d.departement_id_ref"
            ),

            "left"
        )

        .join(

            first_contract_df.alias("fc"),

            F.col(
                "e.Id"
            ).cast("int")

            ==

            F.col(
                "fc.employee_id_first"
            ),

            "left"
        )

        .join(

            current_contract_df.alias("cc"),

            F.col(
                "e.Id"
            ).cast("int")

            ==

            F.col(
                "cc.employee_id_current"
            ),

            "left"
        )

        .withColumn(

            "anciennete_annees",

            F.when(

                F.col(
                    "fc.date_embauche"
                ).isNull(),

                F.lit(None)
                .cast("int")
            )

            .otherwise(

                F.floor(

                    F.months_between(

                        F.current_date(),

                        F.to_date(
                            F.col(
                                "fc.date_embauche"
                            )
                        )
                    )

                    / 12

                ).cast("int")
            )
        )

        .select(

            F.col("e.Id")
            .cast("int")
            .alias(
                "employee_id"
            ),

            F.concat_ws(

                " ",

                F.col("e.FirstName"),

                F.col("e.LastName")

            ).alias(
                "nom"
            ),

            F.col("cc.poste")
            .alias("poste"),

            F.lit(None)
            .cast("string")
            .alias("niveau"),

            F.col("d.service")
            .alias("service"),

            F.col(
                "anciennete_annees"
            ),

            F.to_date(
                F.col(
                    "fc.date_embauche"
                )
            ).alias(
                "date_embauche"
            ),

            F.col("e.CompanyId")
            .cast("int")
            .alias(
                "client_id"
            ),

            F.col("e.Matricule")
            .cast("string")
            .alias(
                "matricule"
            )
        )

        .dropDuplicates(
            ["employee_id"]
        )
    )


    print(
        "Nombre employés :",
        df.count()
    )

    df.show(
        30,
        truncate=False
    )

    return df


# ==================================================================
# 14. DIM_CALENDAR
# ==================================================================

def build_dim_calendar(
    spark,
    payroll_df
):

    print("\n" + "=" * 90)
    print("3/5 - DIM_CALENDAR")
    print("=" * 90)


    periods_df = (

        payroll_df

        .select(

            F.col("Year")
            .cast("int")
            .alias("annee"),

            F.col("Month")
            .cast("int")
            .alias("mois")
        )

        .filter(

            F.col("annee").isNotNull()

            &

            F.col("mois").isNotNull()

            &

            F.col("mois").between(
                1,
                12
            )
        )

        .distinct()
    )


    month_dates_df = (

        periods_df

        .withColumn(

            "date_debut",

            F.to_date(

                F.concat(

                    F.col("annee")
                    .cast("string"),

                    F.lit("-"),

                    F.lpad(

                        F.col("mois")
                        .cast("string"),

                        2,

                        "0"
                    ),

                    F.lit("-01")
                )
            )
        )
    )


    if month_dates_df.count() == 0:

        df = spark.createDataFrame(

            [],

            """
            date date,
            jour int,
            mois int,
            trimestre int,
            annee int
            """
        )

    else:

        min_date = (

            month_dates_df

            .agg(
                F.min("date_debut")
            )

            .collect()[0][0]
        )


        max_date = (

            month_dates_df

            .agg(
                F.max("date_debut")
            )

            .collect()[0][0]
        )


        print(
            "Période calendrier :",
            min_date,
            "->",
            max_date
        )


        df = (

            spark.sql(

                f"""
                SELECT
                    explode(
                        sequence(
                            to_date('{min_date}'),
                            last_day(
                                to_date('{max_date}')
                            ),
                            interval 1 day
                        )
                    ) AS date
                """
            )

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

            .select(

                "date",
                "jour",
                "mois",
                "trimestre",
                "annee"
            )
        )


    print(
        "Nombre dates :",
        df.count()
    )

    df.show(
        30,
        truncate=False
    )

    return df


# ==================================================================
# 15. COMPOSANTS PAIE
# ==================================================================

COMPOSANTS_ATOMIQUES = {

    "SalaireBase":
        (
            "Salaire de base",
            "Brut",
            "Brut",
            1
        ),

    "Conges":
        (
            "Indemnité congés",
            "Brut",
            "Brut",
            1
        ),

    "JoursFeries":
        (
            "Jours fériés",
            "Brut",
            "Brut",
            1
        ),

    "HeuresSupp25":
        (
            "Heures supplémentaires 25%",
            "Heures sup",
            "Brut",
            1
        ),

    "HeuresSupp50":
        (
            "Heures supplémentaires 50%",
            "Heures sup",
            "Brut",
            1
        ),

    "HeuresSupp100":
        (
            "Heures supplémentaires 100%",
            "Heures sup",
            "Brut",
            1
        ),

    "PrimeAnciennete":
        (
            "Prime d'ancienneté",
            "Primes",
            "Brut",
            1
        ),

    "PrimeImposable1":
        (
            "Prime imposable 1",
            "Primes",
            "Brut",
            1
        ),

    "PrimeImposable2":
        (
            "Prime imposable 2",
            "Primes",
            "Brut",
            1
        ),

    "PrimeImposable3":
        (
            "Prime imposable 3",
            "Primes",
            "Brut",
            1
        ),

    "IndemniteRepresentation":
        (
            "Indemnité de représentation",
            "Primes",
            "Brut",
            1
        ),

    "PrimeTransport":
        (
            "Prime de transport",
            "Primes",
            "Brut",
            1
        ),

    "PrimePanier":
        (
            "Prime de panier",
            "Primes",
            "Brut",
            1
        ),

    "IndemniteDeplacement":
        (
            "Indemnité de déplacement",
            "Primes",
            "Brut",
            1
        ),

    "IndemniteCaisse":
        (
            "Indemnité de caisse",
            "Primes",
            "Brut",
            1
        ),

    "PrimeSalissure":
        (
            "Prime de salissure",
            "Primes",
            "Brut",
            1
        ),

    "GratificationsFamilial":
        (
            "Gratification familiale",
            "Primes",
            "Brut",
            1
        ),

    "PrimeVoyageMecque":
        (
            "Prime voyage Mecque",
            "Primes",
            "Brut",
            1
        ),

    "IndemniteLicenciement":
        (
            "Indemnité de licenciement",
            "Primes",
            "Brut",
            1
        ),

    "IndemniteKilometrique":
        (
            "Indemnité kilométrique",
            "Primes",
            "Brut",
            1
        ),

    "PrimeTourne":
        (
            "Prime de tournée",
            "Primes",
            "Brut",
            1
        ),

    "PrimeOutillage":
        (
            "Prime d'outillage",
            "Primes",
            "Brut",
            1
        ),

    "AideMedicale":
        (
            "Aide médicale",
            "Primes",
            "Brut",
            1
        ),

    "AutresPrimesNonImposable":
        (
            "Autres primes non imposables",
            "Primes",
            "Brut",
            0
        ),

    "CnssPartPatronale":
        (
            "CNSS - part patronale",
            "Charges patronales",
            "Charge",
            0
        ),

    "CimrPartPatronale":
        (
            "CIMR - part patronale",
            "Charges patronales",
            "Charge",
            0
        ),

    "AmoPartPatronale":
        (
            "AMO - part patronale",
            "Charges patronales",
            "Charge",
            0
        ),

    "MutuellePartPatronale":
        (
            "Mutuelle - part patronale",
            "Charges patronales",
            "Charge",
            0
        ),

    "CnssPartSalariale":
        (
            "CNSS - part salariale",
            "Charges salariales",
            "Déduction",
            0
        ),

    "CimrPartSalariale":
        (
            "CIMR - part salariale",
            "Charges salariales",
            "Déduction",
            0
        ),

    "AmoPartSalariale":
        (
            "AMO - part salariale",
            "Charges salariales",
            "Déduction",
            0
        ),

    "MutuellePartSalariale":
        (
            "Mutuelle - part salariale",
            "Charges salariales",
            "Déduction",
            0
        ),

    "AvanceSurSalaire":
        (
            "Avance sur salaire",
            "Déduction",
            "Déduction",
            0
        ),

    "ImpotRevenu":
        (
            "Impôt sur le revenu",
            "IR",
            "Déduction",
            0
        ),

    "FraisProfessionnels":
        (
            "Frais professionnels",
            "Abattement fiscal",
            "Déduction",
            0
        ),

    "InteretSurLogement":
        (
            "Intérêt sur logement",
            "Avantage en nature",
            "Brut",
            1
        ),
}


# ==================================================================
# 16. DIM_COMPOSANT_PAIE
# ==================================================================

def build_dim_composant_paie(
    spark,
    payroll_df
):

    print("\n" + "=" * 90)
    print("4/5 - DIM_COMPOSANT_PAIE")
    print("=" * 90)


    payroll_columns = set(
        payroll_df.columns
    )


    composants_valides = {

        code: values

        for code, values
        in COMPOSANTS_ATOMIQUES.items()

        if code in payroll_columns
    }


    print(
        "Composants détectés :"
    )


    for code, values in composants_valides.items():

        print(
            f"✓ {code:<35} "
            f"-> {values[0]}"
        )


    if not composants_valides:

        raise RuntimeError(
            "Aucun composant de paie "
            "trouvé dans PayrollResults."
        )


    rows = []


    for component_id, (
        code,
        values
    ) in enumerate(
        composants_valides.items(),
        start=1
    ):

        libelle, categorie, nature, imposable = values

        rows.append(

            (
                component_id,
                code,
                libelle,
                categorie,
                nature,
                imposable
            )
        )


    df = spark.createDataFrame(

        rows,

        """
        composant_id int,
        code_composant string,
        libelle_composant string,
        categorie string,
        nature string,
        is_imposable int
        """
    )


    print(
        "Nombre composants :",
        df.count()
    )


    df.show(
        100,
        truncate=False
    )


    return (
        df,
        composants_valides
    )


# ==================================================================
# 17. DEDOUBLONNAGE PAYROLL
# ==================================================================

def deduplicate_payroll(
    payroll_df
):

    print("\n" + "=" * 90)
    print("DEDOUBLONNAGE PAYROLLRESULTS")
    print("=" * 90)


    if "CreatedAt" in payroll_df.columns:

        payroll_with_ts = (

            payroll_df

            .withColumn(

                "CreatedAt_ts",

                F.coalesce(

                    F.to_timestamp(
                        F.col("CreatedAt")
                    ),

                    F.to_timestamp(
                        F.col("CreatedAt"),
                        "yyyy-MM-dd HH:mm:ss.SSSSSSS XXX"
                    )
                )
            )
        )

    else:

        payroll_with_ts = (

            payroll_df

            .withColumn(

                "CreatedAt_ts",

                F.lit(None)
                .cast("timestamp")
            )
        )


    required_columns = {

        "EmployeeId",
        "Month",
        "Year"
    }


    missing = (

        required_columns
        -
        set(payroll_df.columns)
    )


    if missing:

        raise RuntimeError(

            "Colonnes obligatoires absentes : "
            +
            ", ".join(
                sorted(missing)
            )
        )


    order_columns = [

        F.col(
            "CreatedAt_ts"
        ).desc_nulls_last()
    ]


    if "Id" in payroll_df.columns:

        order_columns.append(
            F.col("Id").desc()
        )


    window_dedup = (

        Window

        .partitionBy(
            "EmployeeId",
            "Month",
            "Year"
        )

        .orderBy(
            *order_columns
        )
    )


    df = (

        payroll_with_ts

        .withColumn(
            "rang",
            F.row_number().over(
                window_dedup
            )
        )

        .filter(
            F.col("rang") == 1
        )

        .drop(
            "rang",
            "CreatedAt_ts"
        )
    )


    print(
        "Payroll après dédoublonnage :",
        df.count()
    )


    return df


# ==================================================================
# 18. FACT_BULLETIN_LIGNE
# ==================================================================

def build_fact_bulletin_ligne(

    payroll_dedup,

    dim_composant_paie_df,

    composants_valides
):

    print("\n" + "=" * 90)
    print("FACT_BULLETIN_LIGNE")
    print("=" * 90)


    component_codes = list(
        composants_valides.keys()
    )


    stack_args = ", ".join(

        f"'{code}', `{code}`"

        for code
        in component_codes
    )


    stack_expr = (

        f"stack("
        f"{len(component_codes)},"
        f"{stack_args}"
        f") "
        f"AS (code_composant, montant)"
    )


    fact_base_df = (

        payroll_dedup

        .select(

            F.col("EmployeeId")
            .cast("int")
            .alias(
                "employee_id"
            ),

            F.col("CompanyId")
            .cast("int")
            .alias(
                "client_id"
            ),

            F.col("Month")
            .cast("int")
            .alias(
                "mois"
            ),

            F.col("Year")
            .cast("int")
            .alias(
                "annee"
            ),

            F.col("Id")
            .cast("int")
            .alias(
                "payroll_result_id"
            ),

            F.expr(
                stack_expr
            )
        )

        .filter(
            F.col("montant").isNotNull()
        )
    )


    df = (

        fact_base_df.alias("f")

        .join(

            dim_composant_paie_df.alias("cp"),

            F.col(
                "f.code_composant"
            )

            ==

            F.col(
                "cp.code_composant"
            ),

            "inner"
        )

        .select(

            F.col(
                "f.payroll_result_id"
            ),

            F.col(
                "f.employee_id"
            ),

            F.col(
                "f.client_id"
            ),

            F.col(
                "cp.composant_id"
            ),

            F.col(
                "f.mois"
            ),

            F.col(
                "f.annee"
            ),

            F.col(
                "f.montant"
            )
            .cast(
                "decimal(18,2)"
            )
            .alias(
                "montant"
            ),

            F.lit(
                "PayLang/PayrollResults"
            ).alias(
                "source"
            )
        )
    )


    print(
        "Nombre lignes :",
        df.count()
    )


    df.show(
        50,
        truncate=False
    )


    return df


# ==================================================================
# 19. CONTROLE GRAIN
# ==================================================================

def control_grain(
    fact_df
):

    print("\n" + "=" * 90)
    print("CONTROLE DU GRAIN")
    print("=" * 90)


    duplicates = (

        fact_df

        .groupBy(

            "employee_id",
            "client_id",
            "annee",
            "mois",
            "composant_id"
        )

        .count()

        .filter(
            F.col("count") > 1
        )
    )


    n = duplicates.count()


    if n == 0:

        print(
            "✓ Grain valide : "
            "1 composant × "
            "1 collaborateur × "
            "1 mois"
        )

    else:

        print(
            f"❌ {n} doublons de grain"
        )

        duplicates.show(
            30,
            truncate=False
        )


    return n


# ==================================================================
# 20. RECONCILIATION TOTALBRUT
# ==================================================================

def control_total_brut(

    payroll_dedup,

    fact_bulletin_ligne_df,

    dim_composant_paie_df,

    composants_valides
):

    print("\n" + "=" * 90)
    print("RECONCILIATION TOTALBRUT")
    print("=" * 90)


    brut_codes = [

        code

        for code, values
        in composants_valides.items()

        if values[1] in (
            "Brut",
            "Heures sup",
            "Primes"
        )
    ]


    brut_ids_df = (

        dim_composant_paie_df

        .filter(
            F.col(
                "code_composant"
            ).isin(
                brut_codes
            )
        )

        .select(
            "composant_id"
        )
    )


    brut_recalcule_df = (

        fact_bulletin_ligne_df

        .join(
            brut_ids_df,
            "composant_id",
            "inner"
        )

        .groupBy(
            "payroll_result_id"
        )

        .agg(

            F.sum(
                "montant"
            ).alias(
                "brut_recalcule"
            )
        )
    )


    if "TotalBrut" not in payroll_dedup.columns:

        print(
            "⚠️ TotalBrut absent : "
            "contrôle ignoré."
        )

        return


    reconciliation_df = (

        payroll_dedup

        .select(

            F.col("Id")
            .cast("int")
            .alias(
                "payroll_result_id"
            ),

            F.col("TotalBrut")
            .cast(
                "decimal(18,2)"
            )
            .alias(
                "TotalBrut"
            )
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

                F.col(
                    "brut_recalcule"
                ),

                F.lit(
                    0
                ).cast(
                    "decimal(18,2)"
                )
            )
        )
    )


    n_total = reconciliation_df.count()


    n_ecarts = (

        reconciliation_df

        .filter(

            F.col(
                "TotalBrut"
            ).isNotNull()

            &

            (
                F.abs(
                    F.col("ecart")
                )
                >
                F.lit(0.01)
            )
        )

        .count()
    )


    print(
        "Bulletins contrôlés :",
        n_total
    )

    print(
        "Bulletins avec écart :",
        n_ecarts
    )


    if n_ecarts == 0:

        print(
            "✓ Réconciliation TotalBrut OK"
        )

    else:

        print(
            "⚠️ Des écarts existent"
        )

        (

            reconciliation_df

            .filter(

                F.col(
                    "TotalBrut"
                ).isNotNull()

                &

                (
                    F.abs(
                        F.col("ecart")
                    )
                    >
                    F.lit(0.01)
                )
            )

            .show(
                20,
                truncate=False
            )
        )


# ==================================================================
# 21. AUTRES FAITS VIDES
# ==================================================================

def create_empty_facts(
    spark
):

    fact_echeance_df = spark.createDataFrame(

        [],

        """
        echeance_id int,
        client_id int,
        type_echeance string,
        date_echeance date,
        statut string,
        montant decimal(18,2),
        source string
        """
    )


    fact_absence_df = spark.createDataFrame(

        [],

        """
        absence_id int,
        employee_id int,
        client_id int,
        type_absence string,
        date_debut date,
        date_fin date,
        nombre_jours decimal(10,2),
        statut string,
        source string
        """
    )


    fact_variable_collecte_df = spark.createDataFrame(

        [],

        """
        variable_id int,
        employee_id int,
        client_id int,
        annee int,
        mois int,
        code_variable string,
        valeur decimal(18,2),
        source string
        """
    )


    fact_notes_frais_df = spark.createDataFrame(

        [],

        """
        note_frais_id int,
        employee_id int,
        client_id int,
        date_depense date,
        categorie string,
        montant decimal(18,2),
        statut string,
        source string
        """
    )


    fact_evaluation_df = spark.createDataFrame(

        [],

        """
        evaluation_id int,
        employee_id int,
        client_id int,
        date_evaluation date,
        score decimal(10,2),
        commentaire string,
        statut string,
        source string
        """
    )


    dim_echeance_legale_df = spark.createDataFrame(

        [],

        """
        echeance_id int,
        type_echeance string,
        periodicite string,
        regle_date_limite string
        """
    )


    budget_client_df = spark.createDataFrame(

        [],

        """
        budget_id int,
        client_id int,
        annee int,
        mois int,
        montant_budget decimal(18,2),
        source string
        """
    )


    return (

        fact_echeance_df,
        fact_absence_df,
        fact_variable_collecte_df,
        fact_notes_frais_df,
        fact_evaluation_df,
        dim_echeance_legale_df,
        budget_client_df
    )


# ==================================================================
# 22. CREATION TABLES CLICKHOUSE
# ==================================================================

def create_clickhouse_tables():

    print("\n" + "=" * 90)
    print("CREATION TABLES CLICKHOUSE")
    print("=" * 90)


    statements = [

        # ----------------------------------------------------------
        # DIM CLIENTS
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.dim_clients
        (
            client_id Int32,
            nom Nullable(String),
            secteur Nullable(String),
            taille Nullable(String),
            cabinet_id Nullable(Int32)
        )
        ENGINE = MergeTree
        ORDER BY client_id
        """,


        # ----------------------------------------------------------
        # DIM EMPLOYEES
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.dim_employees
        (
            employee_id Int32,
            nom Nullable(String),
            poste Nullable(String),
            niveau Nullable(String),
            service Nullable(String),
            anciennete_annees Nullable(Int32),
            date_embauche Nullable(Date),
            client_id Nullable(Int32),
            matricule Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY employee_id
        """,


        # ----------------------------------------------------------
        # DIM CALENDAR
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.dim_calendar
        (
            date Date,
            jour Int32,
            mois Int32,
            trimestre Int32,
            annee Int32
        )
        ENGINE = MergeTree
        ORDER BY date
        """,


        # ----------------------------------------------------------
        # DIM COMPOSANT PAIE
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.dim_composant_paie
        (
            composant_id Int32,
            code_composant String,
            libelle_composant String,
            categorie String,
            nature String,
            is_imposable UInt8
        )
        ENGINE = MergeTree
        ORDER BY composant_id
        """,


        # ----------------------------------------------------------
        # DIM ECHEANCE LEGALE
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.dim_echeance_legale
        (
            echeance_id Nullable(Int32),
            type_echeance Nullable(String),
            periodicite Nullable(String),
            regle_date_limite Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """,


        # ----------------------------------------------------------
        # FACT BULLETIN
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.fact_bulletin_ligne
        (
            payroll_result_id Nullable(Int32),
            employee_id Int32,
            client_id Int32,
            composant_id Int32,
            mois Int32,
            annee Int32,
            montant Nullable(Decimal(18,2)),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            employee_id,
            annee,
            mois,
            composant_id
        )
        """,


        # ----------------------------------------------------------
        # FACT ECHEANCE
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.fact_echeance
        (
            echeance_id Nullable(Int32),
            client_id Nullable(Int32),
            type_echeance Nullable(String),
            date_echeance Nullable(Date),
            statut Nullable(String),
            montant Nullable(Decimal(18,2)),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            date_echeance
        )
        """,


        # ----------------------------------------------------------
        # FACT ABSENCE
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.fact_absence
        (
            absence_id Nullable(Int32),
            employee_id Nullable(Int32),
            client_id Nullable(Int32),
            type_absence Nullable(String),
            date_debut Nullable(Date),
            date_fin Nullable(Date),
            nombre_jours Nullable(Decimal(10,2)),
            statut Nullable(String),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            employee_id,
            date_debut
        )
        """,


        # ----------------------------------------------------------
        # FACT VARIABLE
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.fact_variable_collecte
        (
            variable_id Nullable(Int32),
            employee_id Nullable(Int32),
            client_id Nullable(Int32),
            annee Nullable(Int32),
            mois Nullable(Int32),
            code_variable Nullable(String),
            valeur Nullable(Decimal(18,2)),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            annee,
            mois
        )
        """,


        # ----------------------------------------------------------
        # FACT NOTES FRAIS
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.fact_notes_frais
        (
            note_frais_id Nullable(Int32),
            employee_id Nullable(Int32),
            client_id Nullable(Int32),
            date_depense Nullable(Date),
            categorie Nullable(String),
            montant Nullable(Decimal(18,2)),
            statut Nullable(String),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            date_depense
        )
        """,


        # ----------------------------------------------------------
        # FACT EVALUATION
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.fact_evaluation
        (
            evaluation_id Nullable(Int32),
            employee_id Nullable(Int32),
            client_id Nullable(Int32),
            date_evaluation Nullable(Date),
            score Nullable(Decimal(10,2)),
            commentaire Nullable(String),
            statut Nullable(String),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            employee_id,
            date_evaluation
        )
        """,


        # ----------------------------------------------------------
        # BUDGET
        # ----------------------------------------------------------

        """
        CREATE TABLE payzen.budget_client
        (
            budget_id Nullable(Int32),
            client_id Nullable(Int32),
            annee Nullable(Int32),
            mois Nullable(Int32),
            montant_budget Nullable(Decimal(18,2)),
            source Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY
        (
            client_id,
            annee,
            mois
        )
        """
    ]


    for statement in statements:

        clickhouse_sql(
            statement
        )


    print(
        "✓ Toutes les tables ClickHouse créées"
    )


# ==================================================================
# 23. CHARGEMENT CLICKHOUSE
# ==================================================================

def load_to_clickhouse(
    df,
    table_name
):

    print(
        f"\nCHARGEMENT : "
        f"{table_name}"
    )


    count = df.count()


    print(
        "Lignes à charger :",
        count
    )


    if count == 0:

        print(
            f"⚠️ {table_name} "
            f"reste vide."
        )

        return


    (

        df.write

        .format("jdbc")

        .mode("append")

        .option(
            "url",
            CLICKHOUSE_JDBC_URL
        )

        .option(
            "dbtable",
            table_name
        )

        .option(
            "user",
            CLICKHOUSE_PROPERTIES["user"]
        )

        .option(
            "password",
            CLICKHOUSE_PROPERTIES["password"]
        )

        .option(
            "driver",
            CLICKHOUSE_PROPERTIES["driver"]
        )

        .option(
            "batchsize",
            "5000"
        )

        .save()
    )


    print(
        f"✓ {table_name} chargée"
    )


# ==================================================================
# 24. CONTROLES FINAUX
# ==================================================================

FINAL_TABLES = [

    "dim_clients",

    "dim_employees",

    "dim_calendar",

    "dim_composant_paie",

    "dim_echeance_legale",

    "fact_bulletin_ligne",

    "fact_echeance",

    "fact_absence",

    "fact_variable_collecte",

    "fact_notes_frais",

    "fact_evaluation",

    "budget_client"
]


def final_controls():

    print("\n" + "=" * 90)
    print("CONTROLE FINAL CLICKHOUSE")
    print("=" * 90)


    for table_name in FINAL_TABLES:

        result = clickhouse_sql(

            f"""
            SELECT count()
            FROM payzen.{table_name}
            """
        )

        print(
            f"{table_name:<30} : "
            f"{result.strip()}"
        )


    # --------------------------------------------------------------
    # FACT BULLETIN
    # --------------------------------------------------------------

    print(
        "\nCONTROLE FACT_BULLETIN_LIGNE"
    )


    result = clickhouse_sql(

        """
        SELECT

            count() AS lignes,

            uniqExact(
                employee_id
            ) AS employees,

            uniqExact(
                client_id
            ) AS clients,

            uniqExact(
                composant_id
            ) AS composants

        FROM payzen.fact_bulletin_ligne
        """
    )


    print(result)


    # --------------------------------------------------------------
    # CATEGORIES
    # --------------------------------------------------------------

    print(
        "\nCONTROLE CATEGORIES"
    )


    result = clickhouse_sql(

        """
        SELECT

            cp.categorie,

            cp.nature,

            count() AS nb_lignes,

            sum(
                f.montant
            ) AS montant_total

        FROM payzen.fact_bulletin_ligne f

        INNER JOIN
            payzen.dim_composant_paie cp

        ON
            f.composant_id
            =
            cp.composant_id

        GROUP BY

            cp.categorie,
            cp.nature

        ORDER BY

            cp.categorie
        """
    )


    print(result)


    # --------------------------------------------------------------
    # HIERARCHIE
    # --------------------------------------------------------------

    print(
        "\nCONTROLE HIERARCHIE BI"
    )


    result = clickhouse_sql(

        """
        SELECT

            c.client_id,

            c.nom AS client,

            e.service,

            cp.categorie,

            cp.libelle_composant,

            sum(
                f.montant
            ) AS montant

        FROM payzen.fact_bulletin_ligne f

        LEFT JOIN
            payzen.dim_clients c

        ON
            f.client_id
            =
            c.client_id

        LEFT JOIN
            payzen.dim_employees e

        ON
            f.employee_id
            =
            e.employee_id

        LEFT JOIN
            payzen.dim_composant_paie cp

        ON
            f.composant_id
            =
            cp.composant_id

        GROUP BY

            c.client_id,
            c.nom,
            e.service,
            cp.categorie,
            cp.libelle_composant

        ORDER BY

            client,
            service,
            cp.categorie,
            cp.libelle_composant
        """
    )


    print(result)


# ==================================================================
# 25. MAIN
# ==================================================================

def main():

    spark = None

    try:

        # ----------------------------------------------------------
        # CLICKHOUSE
        # ----------------------------------------------------------

        check_clickhouse()

        reset_clickhouse()


        # ----------------------------------------------------------
        # SPARK
        # ----------------------------------------------------------

        spark = create_spark()


        # ----------------------------------------------------------
        # SOURCES
        # ----------------------------------------------------------

        print(
            "\n"
            + "=" * 90
        )

        print(
            "EXTRACTION SOURCES SQL SERVER"
        )

        print(
            "=" * 90
        )


        required_tables = [

            "Companies",

            "Employees",

            "Departements",

            "JobPositions",

            "EmployeeContracts",

            "PayrollResults"
        ]


        missing_tables = [

            table

            for table
            in required_tables

            if not sqlserver_table_exists(
                spark,
                table
            )
        ]


        if missing_tables:

            raise RuntimeError(

                "Tables SQL Server absentes : "

                +

                ", ".join(
                    missing_tables
                )
            )


        companies_df = read_sqlserver(
            spark,
            "Companies"
        )


        employees_df = read_sqlserver(
            spark,
            "Employees"
        )


        departements_df = read_sqlserver(
            spark,
            "Departements"
        )


        job_positions_df = read_sqlserver(
            spark,
            "JobPositions"
        )


        employee_contracts_df = read_sqlserver(
            spark,
            "EmployeeContracts"
        )


        payroll_df = read_sqlserver(
            spark,
            "PayrollResults"
        )


        # ----------------------------------------------------------
        # DIMENSIONS
        # ----------------------------------------------------------

        dim_clients_df = build_dim_clients(
            companies_df
        )


        dim_employees_df = build_dim_employees(

            employees_df,

            departements_df,

            job_positions_df,

            employee_contracts_df
        )


        dim_calendar_df = build_dim_calendar(

            spark,

            payroll_df
        )


        (
            dim_composant_paie_df,

            composants_valides

        ) = build_dim_composant_paie(

            spark,

            payroll_df
        )


        # ----------------------------------------------------------
        # PAYROLL
        # ----------------------------------------------------------

        payroll_dedup = deduplicate_payroll(
            payroll_df
        )


        # ----------------------------------------------------------
        # FACT BULLETIN
        # ----------------------------------------------------------

        fact_bulletin_ligne_df = (

            build_fact_bulletin_ligne(

                payroll_dedup,

                dim_composant_paie_df,

                composants_valides
            )
        )


        # ----------------------------------------------------------
        # CONTROLES
        # ----------------------------------------------------------

        control_grain(
            fact_bulletin_ligne_df
        )


        control_total_brut(

            payroll_dedup,

            fact_bulletin_ligne_df,

            dim_composant_paie_df,

            composants_valides
        )


        # ----------------------------------------------------------
        # AUTRES TABLES
        # ----------------------------------------------------------

        (

            fact_echeance_df,

            fact_absence_df,

            fact_variable_collecte_df,

            fact_notes_frais_df,

            fact_evaluation_df,

            dim_echeance_legale_df,

            budget_client_df

        ) = create_empty_facts(
            spark
        )


        # ----------------------------------------------------------
        # CLICKHOUSE TABLES
        # ----------------------------------------------------------

        create_clickhouse_tables()


        # ----------------------------------------------------------
        # CHARGEMENT DIMENSIONS
        # ----------------------------------------------------------

        print(
            "\n"
            + "=" * 90
        )

        print(
            "CHARGEMENT DES DIMENSIONS"
        )

        print(
            "=" * 90
        )


        load_to_clickhouse(
            dim_clients_df,
            "dim_clients"
        )


        load_to_clickhouse(
            dim_employees_df,
            "dim_employees"
        )


        load_to_clickhouse(
            dim_calendar_df,
            "dim_calendar"
        )


        load_to_clickhouse(
            dim_composant_paie_df,
            "dim_composant_paie"
        )


        load_to_clickhouse(
            dim_echeance_legale_df,
            "dim_echeance_legale"
        )


        # ----------------------------------------------------------
        # CHARGEMENT FAITS
        # ----------------------------------------------------------

        print(
            "\n"
            + "=" * 90
        )

        print(
            "CHARGEMENT DES FAITS"
        )

        print(
            "=" * 90
        )


        load_to_clickhouse(

            fact_bulletin_ligne_df,

            "fact_bulletin_ligne"
        )


        load_to_clickhouse(

            fact_echeance_df,

            "fact_echeance"
        )


        load_to_clickhouse(

            fact_absence_df,

            "fact_absence"
        )


        load_to_clickhouse(

            fact_variable_collecte_df,

            "fact_variable_collecte"
        )


        load_to_clickhouse(

            fact_notes_frais_df,

            "fact_notes_frais"
        )


        load_to_clickhouse(

            fact_evaluation_df,

            "fact_evaluation"
        )


        load_to_clickhouse(

            budget_client_df,

            "budget_client"
        )


        # ----------------------------------------------------------
        # CONTROLES FINAUX
        # ----------------------------------------------------------

        final_controls()


        print(
            "\n"
            + "=" * 90
        )

        print(
            "✓ ETL PAYZENHR SOCLE BI V3 TERMINE"
        )

        print(
            "=" * 90
        )


    finally:

        if spark is not None:

            spark.stop()

            print(
                "\n✓ Spark arrêté"
            )


# ==================================================================
# 26. EXECUTION
# ==================================================================

if __name__ == "__main__":

    main()