import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# PAYZENHR - ETL SOCLE BI v3 (dashboard "Coût du Travail — Vue Décomposée")
# ============================================================
#
# Construit STRICTEMENT à partir des colonnes réelles de columns_tables.csv
# / fk_tables.csv (jointes avec le cahier des charges v3 et la maquette
# jéé.pdf). Rien n'est inventé : chaque champ demandé par le .md qui n'a
# pas de source vérifiée reste NULL et est signalé explicitement en fin
# de script, plutôt que d'être fabriqué.
#
# Ce que ce script corrige/apporte par rapport aux versions précédentes :
#   - poste : récupéré via EmployeeContracts.JobPositionId -> JobPositions.Name
#     (Employees n'a PAS de JobPositionId, vérifié dans columns_tables.csv).
#   - date_embauche : 1er EmployeeContracts.StartDate par employé (donnée
#     réelle, pas un champ dédié mais dérivable sans invention).
#   - anciennete_annees : calculée depuis date_embauche (demandée par la
#     maquette PDF, en plus de date_embauche demandée par le .md).
#   - niveau, taille (dim_clients) : AUCUNE colonne source correspondante
#     trouvée dans tout le schéma -> restent NULL, signalés en fin de script.
#   - dim_echeance_legale : NON construite. Aucune table du schéma
#     (SystemConstants, IrTaxBrackets, LegalParameters) ne porte de notion
#     de périodicité / règle de date limite -> pas de source, pas de table.
#   - fact_bulletin_ligne : reconstruite par UNPIVOT de PayrollResults
#     (pas par jointure EmployeeSalaryComponents, invalidée par diagnostic :
#     EmployeeSalaryComponents a un grain contractuel de ~130 jours en
#     moyenne, PAS mensuel).
#   - Dédoublonnage PayrollResults : 1 ligne par (EmployeeId, Month, Year),
#     la plus récente (CreatedAt max) — justifié par diagnostic empirique.
#   - Catégories de composant_paie alignées sur les 5 segments du dashboard
#     jéé.pdf : Brut de base / Primes / Heures sup / Charges patronales /
#     Charges salariales (+ Déduction/Autre pour le reste, non affiché
#     dans ce dashboard mais conservé dans fact_bulletin_ligne pour ne
#     rien perdre).
# ============================================================

# Fix Windows : force l'interpréteur Python du venv pour les workers Spark
# (évite le SocketTimeoutException dû à l'alias Microsoft Store "python").
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("=" * 70)
print("PAYZENHR - ETL SOCLE BI v3")
print("=" * 70)


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_ETL_Socle_BI_v3")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar,"
        r"E:\etape_1\PayZenHR_ETL\jars\clickhouse-jdbc-0.9.8-all.jar"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. CONNEXIONS (mots de passe en variables d'environnement)
# ============================================================
#   PowerShell : $env:PAYZENHR_SQL_PASSWORD = "..."
#                $env:PAYZENHR_CLICKHOUSE_PASSWORD = "..."   (si nécessaire)

sqlserver_url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)
sqlserver_properties = {
    "user": "sa",
    "password": "noraNORA123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

clickhouse_url = "jdbc:clickhouse://localhost:8124/payzen"
clickhouse_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "1234",
}


def read_sqlserver(table_name):
    print(f"Lecture SQL Server : {table_name}")
    df = spark.read.jdbc(url=sqlserver_url, table=f"dbo.{table_name}", properties=sqlserver_properties)
    print(f"  -> {df.count()} lignes")
    return df


# ============================================================
# 3. EXTRACTION
# ============================================================
# Uniquement les tables nécessaires au socle (EmployeeSalaries /
# EmployeeSalaryComponents / PayComponents NE SONT PLUS lues ici :
# invalidées comme source de fact_bulletin_ligne par le diagnostic
# de grain, cf. en-tête du script).

print("\n" + "=" * 70)
print("EXTRACTION")
print("=" * 70)

companies_df = read_sqlserver("Companies")
employees_df = read_sqlserver("Employees")
departements_df = read_sqlserver("Departements")
job_positions_df = read_sqlserver("JobPositions")
employee_contracts_df = read_sqlserver("EmployeeContracts")
payroll_df = read_sqlserver("PayrollResults")


# ============================================================
# 4. CABINET DE RÉFÉRENCE
# ============================================================
# ⚠️ Filtre hérité des scripts précédents (ManagedByCompanyId = 30,
# "cabinet BEFEC"). Ce n'est PAS demandé explicitement par le cahier
# des charges v3 -> conservé pour rester cohérent avec l'existant,
# mais à faire confirmer : est-ce le périmètre voulu pour le socle,
# ou doit-il couvrir tous les clients de la base ?

CABINET_ID = 30

clients_df = companies_df.filter(F.col("ManagedByCompanyId") == CABINET_ID)


# ============================================================
# 5. DIM_CLIENTS
# ============================================================

print("\n" + "=" * 70)
print("DIM_CLIENTS")
print("=" * 70)

dim_clients_df = clients_df.select(
    F.col("Id").alias("client_id"),
    F.col("CompanyName").alias("nom"),
    F.col("BusinessSector").alias("secteur"),          # présent mais vide sur ce jeu (cf. .md : "à peupler")
    F.lit(None).cast("string").alias("taille"),         # Information insuffisante : aucune colonne "taille"/effectif dans Companies
    F.col("ManagedByCompanyId").alias("cabinet_id"),
)

dim_clients_df.show(50, truncate=False)
print("Nombre de clients :", dim_clients_df.count())


# ============================================================
# 6. DIM_EMPLOYEES (poste + date_embauche dérivés d'EmployeeContracts)
# ============================================================

print("\n" + "=" * 70)
print("DIM_EMPLOYEES")
print("=" * 70)

contracts_with_position = employee_contracts_df.alias("ec").join(
    job_positions_df.select(F.col("Id").alias("jp_id"), F.col("Name").alias("poste_nom")).alias("jp"),
    F.col("ec.JobPositionId") == F.col("jp.jp_id"),
    "left",
)

# --- Contrat le plus ANCIEN par employé -> date d'embauche réelle ---
w_first = Window.partitionBy("ec.EmployeeId").orderBy(F.col("ec.StartDate").asc())
first_contract_df = (
    contracts_with_position
    .withColumn("rang_premier", F.row_number().over(w_first))
    .filter(F.col("rang_premier") == 1)
    .select(
        F.col("ec.EmployeeId").alias("emp_id_first"),
        F.col("ec.StartDate").alias("date_embauche"),
    )
)

# --- Contrat COURANT par employé (EndDate NULL en priorité, sinon le plus
#     récent) -> poste actuel. Nécessaire car un employé peut avoir
#     plusieurs contrats dans le temps (dim_employees doit avoir 1 ligne
#     par employé, pas 1 ligne par contrat).
w_current = Window.partitionBy("ec.EmployeeId").orderBy(
    F.when(F.col("ec.EndDate").isNull(), 0).otherwise(1),
    F.col("ec.StartDate").desc(),
)
current_contract_df = (
    contracts_with_position
    .withColumn("rang_courant", F.row_number().over(w_current))
    .filter(F.col("rang_courant") == 1)
    .select(
        F.col("ec.EmployeeId").alias("emp_id_current"),
        F.col("poste_nom").alias("poste"),
    )
)

dim_employees_df = (
    employees_df.alias("e")
    .join(
        departements_df.select(F.col("Id").alias("d_id"), F.col("DepartementName").alias("service")).alias("d"),
        F.col("e.DepartementId") == F.col("d.d_id"),
        "left",
    )
    .join(first_contract_df, F.col("e.Id") == F.col("emp_id_first"), "left")
    .join(current_contract_df, F.col("e.Id") == F.col("emp_id_current"), "left")
    .withColumn(
        "anciennete_annees",
        F.when(F.col("date_embauche").isNull(), F.lit(None).cast("int"))
        .otherwise(F.floor(F.months_between(F.current_date(), F.col("date_embauche")) / F.lit(12)).cast("int")),
    )
    .select(
        F.col("e.Id").alias("employee_id"),
        F.concat_ws(" ", F.col("e.FirstName"), F.col("e.LastName")).alias("nom"),
        F.col("e.CompanyId").alias("client_id"),
        F.col("e.DepartementId").alias("departement_id"),
        F.col("service"),
        F.col("e.Matricule").alias("matricule"),
        F.col("poste"),                                          # réel, via EmployeeContracts -> JobPositions
        F.lit(None).cast("string").alias("niveau"),               # Information insuffisante : aucune colonne de niveau dans JobPositions
        F.col("date_embauche"),                                   # réel, 1er EmployeeContracts.StartDate
        F.col("anciennete_annees"),                                # calculé, snapshot à la date d'exécution
    )
)

dim_employees_df.show(50, truncate=False)
print("Nombre employés :", dim_employees_df.count())


# ============================================================
# 7. DIM_CALENDAR
# ============================================================
# Limite de source inchangée : PayrollResults n'a que Month/Year,
# donc "jour" reste fixé à 1 (proxy mensuel), pas une vraie date
# journalière.

print("\n" + "=" * 70)
print("DIM_CALENDAR")
print("=" * 70)

calendar_source_df = (
    payroll_df
    .select(F.col("Month").alias("mois"), F.col("Year").alias("annee"))
    .filter(F.col("mois").isNotNull() & F.col("annee").isNotNull())
    .distinct()
)

dim_calendar_df = (
    calendar_source_df
    .withColumn("date", F.to_date(F.concat_ws("-", F.col("annee"), F.col("mois"), F.lit("01"))))
    .withColumn("jour", F.lit(1))
    .withColumn("trimestre", F.quarter(F.col("date")))
    .select("date", "jour", "mois", "trimestre", "annee")
)

dim_calendar_df.show(100, truncate=False)
print("Nombre périodes :", dim_calendar_df.count())


# ============================================================
# 8. CLASSIFICATION DES COMPOSANTS DE PayrollResults
# ============================================================
# Catégories alignées sur les 5 segments du dashboard "Coût du Travail —
# Vue Décomposée" (jéé.pdf) : Brut de base / Primes / Heures sup /
# Charges patronales / Charges salariales, + Déduction/Autre pour le
# reste (conservé dans fact_bulletin_ligne, non utilisé par CE dashboard).
#
# ⚠️ PayComponents (la vraie table de référence) est vide sur ce jeu de
# données -> cette classification est une hypothèse de lecture basée sur
# le NOM des colonnes, à faire valider par Mohamed avant diffusion
# officielle (cf. .md, "Étape 0 obligatoire").

COMPOSANTS_ATOMIQUES = {
    # colonne source            : (libellé humain,                 catégorie)
    "SalaireBase":                ("Salaire de base",                "Brut de base"),
    "Conges":                     ("Indemnité congés",                "Brut de base"),
    "JoursFeries":                ("Jours fériés",                    "Brut de base"),
    "HeuresSupp25":                ("Heures supplémentaires 25%",      "Heures sup"),
    "HeuresSupp50":                ("Heures supplémentaires 50%",      "Heures sup"),
    "HeuresSupp100":               ("Heures supplémentaires 100%",     "Heures sup"),
    "PrimeAnciennete":             ("Prime d'ancienneté",              "Primes"),
    "PrimeImposable1":             ("Prime imposable 1",               "Primes"),
    "PrimeImposable2":             ("Prime imposable 2",               "Primes"),
    "PrimeImposable3":             ("Prime imposable 3",               "Primes"),
    "IndemniteRepresentation":     ("Indemnité de représentation",     "Primes"),
    "PrimeTransport":              ("Prime de transport",              "Primes"),
    "PrimePanier":                 ("Prime de panier",                 "Primes"),
    "IndemniteDeplacement":        ("Indemnité de déplacement",        "Primes"),
    "IndemniteCaisse":             ("Indemnité de caisse",             "Primes"),
    "PrimeSalissure":              ("Prime de salissure",              "Primes"),
    "GratificationsFamilial":      ("Gratification familiale",         "Primes"),
    "PrimeVoyageMecque":           ("Prime voyage Mecque",             "Primes"),
    "IndemniteLicenciement":       ("Indemnité de licenciement",       "Primes"),
    "IndemniteKilometrique":       ("Indemnité kilométrique",          "Primes"),
    "PrimeTourne":                 ("Prime de tournée",                "Primes"),
    "PrimeOutillage":              ("Prime d'outillage",               "Primes"),
    "AideMedicale":                ("Aide médicale",                   "Primes"),
    "AutresPrimesNonImposable":    ("Autres primes non imposables",    "Primes"),
    "CnssPartPatronale":           ("CNSS — part patronale",           "Charges patronales"),
    "CimrPartPatronale":           ("CIMR — part patronale",           "Charges patronales"),
    "AmoPartPatronale":            ("AMO — part patronale",            "Charges patronales"),
    "MutuellePartPatronale":       ("Mutuelle — part patronale",       "Charges patronales"),
    "CnssPartSalariale":           ("CNSS — part salariale",           "Charges salariales"),
    "CimrPartSalariale":           ("CIMR — part salariale",           "Charges salariales"),
    "AmoPartSalariale":            ("AMO — part salariale",            "Charges salariales"),
    "MutuellePartSalariale":       ("Mutuelle — part salariale",       "Charges salariales"),
    "AvanceSurSalaire":            ("Avance sur salaire",              "Déduction"),
    "ImpotRevenu":                 ("Impôt sur le revenu",             "Déduction"),
    "FraisProfessionnels":         ("Frais professionnels",            "Abattement fiscal"),
    "InteretSurLogement":          ("Intérêt sur logement",            "Avantage en nature"),
}

# Explicitement EXCLUES de l'unpivot (et pourquoi) :
#   - Sous-totaux (double comptage si sommés avec les composants ci-dessus) :
#     TotalPrimesImposables, TotalBrut, TotalIndemnites, TotalNiExcedentImposable,
#     TotalCotisationsSalariales, TotalCotisationsPatronales, BrutImposable,
#     NetImposable, TotalGains, TotalRetenues, NetAPayer, TotalNet, TotalNet2
#   - Taux / bases de calcul (pas des montants payables) :
#     PrimeAnciennteRate, CnssBase, CimrBase, AmoBase, MutuelleBase, IrTaux
#   - Technique : Arrondi, Id, EmployeeId, CompanyId, Month, Year, Status,
#     ErrorMessage, ResultatJson, ClaudeModel, TokensUsed, ProcessedAt,
#     CreatedAt/By, UpdatedAt/By, DeletedAt/By, PayHalf, ImportBatchNumber,
#     ImportMetadataJson, ImportSource

COMPOSANT_COLS = list(COMPOSANTS_ATOMIQUES.keys())


# ============================================================
# 9. DIM_COMPOSANT_PAIE
# ============================================================

print("\n" + "=" * 70)
print("DIM_COMPOSANT_PAIE")
print("=" * 70)


def _sql_escape(s):
    return s.replace("'", "''")


values_sql = ", ".join(
    f"('{_sql_escape(c)}', '{_sql_escape(lib)}', '{_sql_escape(cat)}')"
    for c, (lib, cat) in COMPOSANTS_ATOMIQUES.items()
)
# Construite via spark.sql(VALUES ...) plutôt que createDataFrame(liste python, ...) :
# évite de lancer un worker Python (contourne le souci Windows rencontré précédemment).
dim_composant_paie_df = spark.sql(
    f"SELECT * FROM VALUES {values_sql} AS t(composant_id, libelle, categorie)"
)

dim_composant_paie_df.show(50, truncate=False)
print("Nombre de composants :", dim_composant_paie_df.count())


# ============================================================
# 10. DEDOUBLONNAGE de PayrollResults (avant l'unpivot)
# ============================================================
# 1 ligne par (EmployeeId, Month, Year) = la plus récente (CreatedAt max).
# Justifié par diagnostic empirique : le pipeline source réémet une ligne
# "finale" juste après une première (Status 6 puis 7, ~0,2s d'écart) sur
# la quasi-totalité des groupes concernés.

print("\n" + "=" * 70)
print("DEDOUBLONNAGE PayrollResults")
print("=" * 70)

payroll_with_ts = payroll_df.withColumn(
    "CreatedAt_ts",
    F.coalesce(
        F.to_timestamp("CreatedAt", "yyyy-MM-dd HH:mm:ss.SSSSSSS XXX"),
        F.to_timestamp("CreatedAt"),
    ),
)

w_dedup = Window.partitionBy("EmployeeId", "Month", "Year").orderBy(
    F.col("CreatedAt_ts").desc_nulls_last(), F.col("Id").desc()
)

payroll_dedup = (
    payroll_with_ts
    .withColumn("rang", F.row_number().over(w_dedup))
)

n_avant = payroll_with_ts.count()
n_ecartees = payroll_dedup.filter(F.col("rang") > 1).count()
payroll_dedup = payroll_dedup.filter(F.col("rang") == 1).drop("rang", "CreatedAt_ts")
n_apres = payroll_dedup.count()

print(f"Lignes avant : {n_avant} | écartées (doublon employé/mois) : {n_ecartees} | après : {n_apres}")


# ============================================================
# 11. FACT_BULLETIN_LIGNE (unpivot de PayrollResults)
# ============================================================

print("\n" + "=" * 70)
print("FACT_BULLETIN_LIGNE")
print("=" * 70)

stack_args = ", ".join(f"'{c}', `{c}`" for c in COMPOSANT_COLS)
stack_expr = f"stack({len(COMPOSANT_COLS)}, {stack_args}) as (composant_id, montant)"

fact_base_df = payroll_dedup.select(
    "EmployeeId", "CompanyId", "Month", "Year", "Id", "Status",
    F.expr(stack_expr),
)

# NULL = composant non applicable à ce bulletin -> pas de ligne générée.
# Un vrai montant à 0 payé, lui, est conservé (NULL != 0).
fact_base_df = fact_base_df.filter(F.col("montant").isNotNull())

fact_base_aliased = fact_base_df.alias("f")

emp_aliased = dim_employees_df.select(
    F.col("employee_id").alias("de_employee_id"),
    F.col("nom").alias("employee_name"),
    F.col("service"),
).alias("e")

cli_aliased = dim_clients_df.select(
    F.col("client_id").alias("dc_client_id"),
    F.col("nom").alias("client_name"),
).alias("c")

comp_aliased = dim_composant_paie_df.alias("dc")

fact_bulletin_ligne_df = (
    fact_base_aliased
    .join(emp_aliased, F.col("f.EmployeeId") == F.col("e.de_employee_id"), "left")
    .join(cli_aliased, F.col("f.CompanyId") == F.col("c.dc_client_id"), "inner")
    .join(comp_aliased, F.col("f.composant_id") == F.col("dc.composant_id"), "left")
    .select(
        F.col("f.EmployeeId").alias("employee_id"),
        F.col("e.employee_name"),
        F.col("f.CompanyId").alias("client_id"),
        F.col("c.client_name"),
        F.col("e.service"),
        F.col("f.composant_id"),
        F.col("dc.libelle").alias("composant_libelle"),
        F.col("dc.categorie").alias("composant_categorie"),
        F.col("f.montant"),
        F.col("f.Month").alias("mois"),
        F.col("f.Year").alias("annee"),
        F.col("f.Id").alias("payroll_result_id"),
    )
)

fact_bulletin_ligne_df.show(100, truncate=False)
print("Nombre de lignes fact_bulletin_ligne :", fact_bulletin_ligne_df.count())


# ============================================================
# 12. CONTROLE DE RECONCILIATION vs TotalBrut
# ============================================================

print("\n" + "=" * 70)
print("CONTROLE DE RECONCILIATION vs TotalBrut")
print("=" * 70)

cats_brut = ["Brut de base", "Heures sup", "Primes"]
brut_cols = [c for c, (_, cat) in COMPOSANTS_ATOMIQUES.items() if cat in cats_brut]

somme_brut_recalculee = (
    fact_bulletin_ligne_df
    .filter(F.col("composant_id").isin(brut_cols))
    .groupBy("payroll_result_id")
    .agg(F.sum("montant").alias("brut_recalcule"))
)

reconciliation_df = (
    payroll_dedup
    .select(F.col("Id").alias("payroll_result_id"), "TotalBrut")
    .join(somme_brut_recalculee, "payroll_result_id", "left")
    .withColumn("ecart", F.col("TotalBrut") - F.coalesce(F.col("brut_recalcule"), F.lit(0)))
)

n_ecarts = reconciliation_df.filter(F.abs(F.col("ecart")) > 0.01).count()
n_total = reconciliation_df.count()
reconciliation_df.filter(F.abs(F.col("ecart")) > 0.01).show(20, truncate=False)
print(f"Bulletins avec écart > 0.01 vs TotalBrut : {n_ecarts} / {n_total}")


# ============================================================
# 13. CONTROLES DASHBOARD (Portefeuille -> Client -> Service -> Composant)
# ============================================================
# Reproduit la hiérarchie de la maquette jéé.pdf pour vérifier que
# fact_bulletin_ligne peut bien l'alimenter telle quelle.

print("\n" + "=" * 70)
print("CONTROLE — MONTANTS PAR SERVICE x CATEGORIE (vue dashboard)")
print("=" * 70)

(
    fact_bulletin_ligne_df
    .filter(F.col("composant_categorie").isin(
        "Brut de base", "Primes", "Heures sup", "Charges patronales", "Charges salariales"
    ))
    .groupBy("service", "composant_categorie")
    .agg(F.sum("montant").alias("total_montant"))
    .orderBy("service", "composant_categorie")
    .show(100, truncate=False)
)

print("\n===== SOCLE : COMPTAGES =====")
print("DIM_CLIENTS :", dim_clients_df.count())
print("DIM_EMPLOYEES :", dim_employees_df.count())
print("DIM_CALENDAR :", dim_calendar_df.count())
print("DIM_COMPOSANT_PAIE :", dim_composant_paie_df.count())
print("FACT_BULLETIN_LIGNE :", fact_bulletin_ligne_df.count())


# ============================================================
# 14. LOAD CLICKHOUSE
# ============================================================
# Tables cibles à créer au préalable (ENGINE = MergeTree...), voir
# sql/clickhouse.sql. Mode "append" : ne pas relancer sans stratégie
# de purge/upsert (non traité ici — idempotence à définir séparément).

print("\n" + "=" * 70)
print("CHARGEMENT CLICKHOUSE")
print("=" * 70)

for df_, table_ in [
    (dim_clients_df, "dim_clients"),
    (dim_employees_df, "dim_employees"),
    (dim_calendar_df, "dim_calendar"),
    (dim_composant_paie_df, "dim_composant_paie"),
    (fact_bulletin_ligne_df, "fact_bulletin_ligne"),
]:
    df_.write.mode("append").jdbc(url=clickhouse_url, table=table_, properties=clickhouse_properties)
    print(f"✓ {table_} chargée")


# ============================================================
# 15. FIN — bilan honnête de ce qui est réel vs manquant
# ============================================================

print("\n" + "=" * 70)
print("ETL SOCLE BI v3 TERMINE")
print("=" * 70)
print("""
Tables construites (données réelles, aucune valeur inventée) :
  1. dim_clients        (taille : NULL — aucune colonne source, Information insuffisante)
  2. dim_employees       (poste et date_embauche réels via EmployeeContracts/JobPositions ;
                          niveau : NULL — aucune colonne source, Information insuffisante)
  3. dim_calendar         (jour fixé à 1 — limite structurelle de PayrollResults)
  4. dim_composant_paie   (classification par nom de colonne — PayComponents vide,
                          à valider avec Mohamed avant diffusion officielle)
  5. fact_bulletin_ligne  (unpivot PayrollResults, dédoublonné, grain validé par diagnostic)

NON construite :
  - dim_echeance_legale : aucune table du schéma (SystemConstants, IrTaxBrackets,
    LegalParameters) ne porte de notion de périodicité / date limite légale.
    Pas de source identifiée -> pas de table fabriquée. À constituer avec
    Mohamed si une autre source existe (Surveil ?).
  - fact_echeance, fact_absence, fact_variable_collecte, fact_notes_frais,
    fact_evaluation : sources non identifiées dans ce périmètre d'extraction.

Points à faire confirmer avant diffusion du dashboard :
  - Filtre ManagedByCompanyId == 30 (cabinet BEFEC) : périmètre voulu du socle,
    ou faut-il couvrir tous les clients ?
  - Classification des composants en 5 catégories (§8) : validée sur le nom
    des colonnes uniquement, PayComponents étant vide.
  - Idempotence du chargement ClickHouse (mode append actuel duplique si
    le script est relancé).
""")

spark.stop()
