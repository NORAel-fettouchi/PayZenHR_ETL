import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# PAYZENHR - ETL SOCLE BI (fact_bulletin_ligne v2 — autonome)
# ============================================================
#
# Version corrigée du bloc DIM_COMPOSANT_PAIE + FACT_BULLETIN_LIGNE,
# rendue AUTONOME (inclut la session Spark + l'extraction nécessaire)
# pour être lancée seule, sans dépendre de etl_socle_bi_corrige.py.
#
# Changement de fond, justifié empiriquement par les deux diagnostics
# précédents :
#   - EmployeeSalaryComponents = structure contractuelle (durée moyenne
#     129,6 jours, seulement 29,5% <= 31 jours) -> PAS un grain mensuel.
#   - PayrollResults a déjà (EmployeeId, Month, Year) comme grain naturel,
#     mais avec 573 lignes pour 312 groupes distincts -> 261 doublons.
#   - Les doublons s'expliquent par un pipeline en 2 étapes (Status 6 puis 7,
#     quasi simultanés, même fichier d'import) : on garde la ligne la plus
#     RÉCENTE par groupe (CreatedAt max), sans avoir besoin de connaître le
#     sens exact des codes Status (point encore non confirmé par Mohamed).
#
# fact_bulletin_ligne est donc reconstruite par UNPIVOT des colonnes de
# montants de PayrollResults (pas par jointure EmployeeSalaryComponents).
#
# ⚠️ Ce script ne corrige PAS les autres points déjà signalés séparément :
#    - filtre cabinet ManagedByCompanyId==30 sur dim_clients (non documenté
#      dans le cahier des charges, à confirmer) — conservé ici à l'identique
#      pour rester cohérent avec le reste du pipeline existant ;
#    - contournement de la couche Staging (lecture directe de la source) ;
#    - idempotence du chargement ClickHouse (non traité ici, pas d'écriture
#      ClickHouse dans ce script — seulement extraction + contrôle).
# ============================================================

print("=" * 70)
print("PAYZENHR - FACT_BULLETIN_LIGNE v2 (autonome)")
print("=" * 70)


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_FactBulletinLigne_v2")
    .config("spark.jars", r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. SQL SERVER — connexion (mot de passe en variable d'environnement)
# ============================================================
#   PowerShell : $env:PAYZENHR_SQL_PASSWORD = "votre_mot_de_passe"

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


def read_sqlserver(table_name):
    print(f"\nLecture SQL Server : {table_name}")
    df = spark.read.jdbc(url=sqlserver_url, table=f"dbo.{table_name}", properties=sqlserver_properties)
    print(f"{table_name} : {df.count()} lignes")
    return df


# ============================================================
# 3. EXTRACTION (uniquement les tables nécessaires à ce fait)
# ============================================================

print("\n" + "=" * 70)
print("EXTRACTION")
print("=" * 70)

companies_df = read_sqlserver("Companies")
employees_df = read_sqlserver("Employees")
departements_df = read_sqlserver("Departements")
payroll_df = read_sqlserver("PayrollResults")


# ============================================================
# 4. DIM_CLIENTS (minimal, requis pour la jointure de fact_bulletin_ligne)
# ============================================================
# Filtre cabinet=30 identique au script d'origine, NON corrigé ici
# (point déjà signalé séparément — hors périmètre de cette étape).

clients_df = companies_df.filter(F.col("ManagedByCompanyId") == 30).select(
    F.col("Id"), F.col("CompanyName")
)


# ============================================================
# 5. Classification des colonnes de PayrollResults
# ============================================================
# Construite à partir du schéma réel (columns_tables.csv), pas d'un filtre
# par mot-clé. Le libellé/catégorie de chaque composant est une hypothèse
# de lecture basée sur le NOM des colonnes (PayComponents, la vraie table
# de référence, est vide) — à faire valider par Mohamed avant diffusion
# dans un dashboard officiel.

COMPOSANTS_ATOMIQUES = {
    # colonne source            : (libellé humain,                  catégorie)
    "SalaireBase":                ("Salaire de base",                 "Brut"),
    "HeuresSupp25":                ("Heures supplémentaires 25%",       "Brut"),
    "HeuresSupp50":                ("Heures supplémentaires 50%",       "Brut"),
    "HeuresSupp100":               ("Heures supplémentaires 100%",      "Brut"),
    "Conges":                      ("Indemnité congés",                 "Brut"),
    "JoursFeries":                 ("Jours fériés",                     "Brut"),
    "PrimeAnciennete":             ("Prime d'ancienneté",               "Prime"),
    "PrimeImposable1":             ("Prime imposable 1",                "Prime"),
    "PrimeImposable2":             ("Prime imposable 2",                "Prime"),
    "PrimeImposable3":             ("Prime imposable 3",                "Prime"),
    "FraisProfessionnels":         ("Frais professionnels",             "Abattement fiscal"),
    "IndemniteRepresentation":     ("Indemnité de représentation",      "Indemnité"),
    "PrimeTransport":              ("Prime de transport",               "Indemnité"),
    "PrimePanier":                 ("Prime de panier",                  "Indemnité"),
    "IndemniteDeplacement":        ("Indemnité de déplacement",         "Indemnité"),
    "IndemniteCaisse":             ("Indemnité de caisse",              "Indemnité"),
    "PrimeSalissure":              ("Prime de salissure",               "Indemnité"),
    "GratificationsFamilial":      ("Gratification familiale",          "Indemnité"),
    "PrimeVoyageMecque":           ("Prime voyage Mecque",              "Indemnité"),
    "IndemniteLicenciement":       ("Indemnité de licenciement",        "Indemnité"),
    "IndemniteKilometrique":       ("Indemnité kilométrique",           "Indemnité"),
    "PrimeTourne":                 ("Prime de tournée",                 "Indemnité"),
    "PrimeOutillage":              ("Prime d'outillage",                "Indemnité"),
    "AideMedicale":                ("Aide médicale",                    "Indemnité"),
    "AutresPrimesNonImposable":    ("Autres primes non imposables",     "Indemnité"),
    "AvanceSurSalaire":            ("Avance sur salaire",               "Déduction"),
    "InteretSurLogement":          ("Intérêt sur logement",             "Avantage en nature"),
    "CnssPartSalariale":           ("CNSS — part salariale",            "Charge salariale"),
    "CimrPartSalariale":           ("CIMR — part salariale",            "Charge salariale"),
    "AmoPartSalariale":            ("AMO — part salariale",             "Charge salariale"),
    "MutuellePartSalariale":       ("Mutuelle — part salariale",        "Charge salariale"),
    "CnssPartPatronale":           ("CNSS — part patronale",            "Charge patronale"),
    "CimrPartPatronale":           ("CIMR — part patronale",            "Charge patronale"),
    "AmoPartPatronale":            ("AMO — part patronale",             "Charge patronale"),
    "MutuellePartPatronale":       ("Mutuelle — part patronale",        "Charge patronale"),
    "ImpotRevenu":                 ("Impôt sur le revenu",              "Déduction"),
}

# Explicitement EXCLUES de l'unpivot (et pourquoi) — ne jamais les ajouter
# à COMPOSANTS_ATOMIQUES sans re-vérifier le raisonnement :
#   - Sous-totaux (double comptage si sommés avec les composants atomiques) :
#     TotalPrimesImposables, TotalBrut, TotalIndemnites, TotalNiExcedentImposable,
#     TotalCotisationsSalariales, TotalCotisationsPatronales, BrutImposable,
#     NetImposable, TotalGains, TotalRetenues, NetAPayer, TotalNet, TotalNet2
#   - Taux / bases de calcul (pas des montants payables) :
#     PrimeAnciennteRate, CnssBase, CimrBase, AmoBase, MutuelleBase, IrTaux
#   - Technique (rien à voir avec un composant de paie) :
#     Arrondi, Id, EmployeeId, CompanyId, Month, Year, Status, ErrorMessage,
#     ResultatJson, ClaudeModel, TokensUsed, ProcessedAt, CreatedAt, CreatedBy,
#     UpdatedAt, UpdatedBy, DeletedAt, DeletedBy, PayHalf, ImportBatchNumber,
#     ImportMetadataJson, ImportSource

COMPOSANT_COLS = list(COMPOSANTS_ATOMIQUES.keys())


# ============================================================
# 6. DEDOUBLONNAGE de PayrollResults AVANT tout calcul
# ============================================================
# Règle : 1 ligne par (EmployeeId, Month, Year) = la plus récente
# (CreatedAt max). Justifiée par le diagnostic : Status 6 et 7 sont
# systématiquement en paire, quasi simultanés (~0,2s d'écart), le
# pipeline source réémet une ligne "finale" juste après la première.
# CreatedAt est StringType() côté source -> on le parse en timestamp
# pour un tri chronologique fiable (pas un tri alphabétique du texte).

print("\n" + "=" * 70)
print("DEDOUBLONNAGE PayrollResults")
print("=" * 70)

payroll_with_ts = payroll_df.withColumn(
    "CreatedAt_ts",
    F.coalesce(
        F.to_timestamp("CreatedAt", "yyyy-MM-dd HH:mm:ss.SSSSSSS XXX"),
        F.to_timestamp("CreatedAt"),  # repli sur le parsing générique Spark
    ),
)

n_avant = payroll_with_ts.count()
n_ts_manquant = payroll_with_ts.filter(
    F.col("CreatedAt").isNotNull() & F.col("CreatedAt_ts").isNull()
).count()
if n_ts_manquant:
    print(f"⚠️ {n_ts_manquant} ligne(s) avec CreatedAt non parsable — "
          f"le dédoublonnage utilisera l'ordre de Id en repli pour ces lignes (à auditer).")

w_dedup = Window.partitionBy("EmployeeId", "Month", "Year").orderBy(
    F.col("CreatedAt_ts").desc_nulls_last(), F.col("Id").desc()
)

payroll_dedup = payroll_with_ts.withColumn("rang", F.row_number().over(w_dedup))

lignes_ecartees = payroll_dedup.filter(F.col("rang") > 1)
print(f"Lignes PayrollResults avant dédoublonnage : {n_avant}")
print(f"Lignes écartées (doublon employé/mois, gardé le plus récent) : {lignes_ecartees.count()}")
print("Répartition de Status parmi les lignes écartées :")
lignes_ecartees.groupBy("Status").count().orderBy("Status").show()

payroll_dedup = payroll_dedup.filter(F.col("rang") == 1).drop("rang", "CreatedAt_ts")
n_apres = payroll_dedup.count()
print(f"Lignes PayrollResults après dédoublonnage : {n_apres} "
      f"(attendu = nb de groupes distincts EmployeeId/Month/Year)")


# ============================================================
# 7. DIM_COMPOSANT_PAIE (reconstruite à partir des vraies colonnes)
# ============================================================

print("\n" + "=" * 70)
print("DIM_COMPOSANT_PAIE (v2)")
print("=" * 70)

dim_composant_paie_df = spark.createDataFrame(
    [(col_name, libelle, categorie) for col_name, (libelle, categorie) in COMPOSANTS_ATOMIQUES.items()],
    schema=["composant_id", "libelle", "categorie"],
)
dim_composant_paie_df.show(50, truncate=False)
print("Nombre de composants :", dim_composant_paie_df.count())


# ============================================================
# 8. FACT_BULLETIN_LIGNE (unpivot de PayrollResults dédoublonné)
# ============================================================

print("\n" + "=" * 70)
print("FACT_BULLETIN_LIGNE (v2)")
print("=" * 70)

stack_args = ", ".join(f"'{c}', `{c}`" for c in COMPOSANT_COLS)
stack_expr = f"stack({len(COMPOSANT_COLS)}, {stack_args}) as (composant_id, montant)"

fact_base_df = payroll_dedup.select(
    "EmployeeId", "CompanyId", "Month", "Year", "Id", "Status",
    F.expr(stack_expr),
)

# NULL = composant non applicable à ce bulletin -> ligne non générée
# (on ne fabrique pas de 0 ; un vrai montant à 0 payé, lui, est conservé).
fact_base_df = fact_base_df.filter(F.col("montant").isNotNull())

# Alias systématiques + F.col("alias.colonne") à chaque référence :
# élimine toute ambiguïté de résolution de colonne après jointures enchaînées.
fact_base_aliased = fact_base_df.alias("f")

emp_aliased = employees_df.select(
    F.col("Id").alias("e_id"),
    F.concat_ws(" ", F.col("FirstName"), F.col("LastName")).alias("employee_name"),
    F.col("DepartementId").alias("e_departement_id"),
).alias("e")

cli_aliased = clients_df.select(
    F.col("Id").alias("c_id"),
    F.col("CompanyName").alias("client_name"),
).alias("c")

dep_aliased = departements_df.select(
    F.col("Id").alias("d_id"),
    F.col("DepartementName").alias("service"),
).alias("d")

comp_aliased = dim_composant_paie_df.alias("dc")

fact_bulletin_ligne_df = (
    fact_base_aliased
    .join(emp_aliased, F.col("f.EmployeeId") == F.col("e.e_id"), "left")
    .join(cli_aliased, F.col("f.CompanyId") == F.col("c.c_id"), "inner")
    .join(dep_aliased, F.col("e.e_departement_id") == F.col("d.d_id"), "left")
    .join(comp_aliased, F.col("f.composant_id") == F.col("dc.composant_id"), "left")
    .select(
        F.col("f.EmployeeId").alias("employee_id"),
        F.col("e.employee_name").alias("employee_name"),
        F.col("f.CompanyId").alias("client_id"),
        F.col("c.client_name").alias("client_name"),
        F.col("d.service").alias("service"),
        F.col("f.composant_id").alias("composant_id"),
        F.col("dc.libelle").alias("composant_libelle"),
        F.col("dc.categorie").alias("composant_categorie"),
        F.col("f.montant").alias("montant"),
        F.col("f.Month").alias("mois"),
        F.col("f.Year").alias("annee"),
        F.col("f.Id").alias("payroll_result_id"),
    )
)

fact_bulletin_ligne_df.show(100, truncate=False)
print("Nombre de lignes fact_bulletin_ligne :", fact_bulletin_ligne_df.count())


# ============================================================
# 9. CONTROLE DE RECONCILIATION vs TotalBrut
# ============================================================
# Vérifie que la somme des composants atomiques d'un bulletin est cohérente
# avec les sous-totaux exclus de l'unpivot. Sert de garde-fou : si l'écart
# est important, la classification en §5 est fausse quelque part.

print("\n" + "=" * 70)
print("CONTROLE DE RECONCILIATION vs TotalBrut")
print("=" * 70)

brut_cols = [c for c, (_, cat) in COMPOSANTS_ATOMIQUES.items() if cat in ("Brut", "Prime")]
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

reconciliation_df.filter(F.abs(F.col("ecart")) > 0.01).show(20, truncate=False)
n_ecarts = reconciliation_df.filter(F.abs(F.col("ecart")) > 0.01).count()
n_total_bulletins = reconciliation_df.count()
print(f"Bulletins avec écart > 0.01 entre TotalBrut et la somme recalculée : "
      f"{n_ecarts} / {n_total_bulletins}")
if n_ecarts > 0:
    print("⚠️ Écarts détectés : la classification des composants (§5) est probablement "
          "incomplète (composant manquant, ou colonne encore non identifiée) — "
          "à examiner avant de publier ce fait en dashboard.")
else:
    print("✅ Aucun écart significatif : la classification 'Brut'/'Prime' est cohérente "
          "avec TotalBrut sur ce jeu de données.")


# ============================================================
# 10. FIN
# ============================================================

print("\n" + "=" * 70)
print("TERMINE — fact_bulletin_ligne construite (pas encore chargée en ClickHouse)")
print("=" * 70)

spark.stop()