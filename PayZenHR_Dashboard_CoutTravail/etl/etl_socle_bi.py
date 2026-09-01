import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    year,
    month,
    quarter,
    to_date,
    lit,
    when,   # CORRECTION #2 : import manquant, provoquait un NameError
)

# ============================================================
# PAYZENHR - ETL SOCLE BI (version corrigée)
# ============================================================
#
# Corrections apportées par rapport au script original :
#   1. Parenthèse fermante manquante section "FACT PAR EMPLOYE"
#      -> SyntaxError, tout le script s'arrêtait avant ClickHouse.
#   2. Import de `when` ajouté (NameError sinon).
#   3. Colonne EmployeeId ambiguë après jointure esc + es
#      -> on sélectionne explicitement les colonnes de chaque
#         DataFrame avant la jointure, plus de doublon de nom.
#   4. lit(None) casté explicitement en string (évite un échec
#      d'écriture JDBC dû à un type NullType).
#   5. dim_employees : colonnes "poste", "niveau", "date_embauche"
#      ajoutées en NULL explicite, car absentes de la source
#      actuelle (JobPositionId non présent dans Employees).
#      -> Schéma conforme au cahier des charges, donnée non
#         inventée : à peupler quand la source sera disponible.
#      -> Information non précisée dans le cahier des charges :
#         confirmer avec Mohamed la table source de "poste"/"niveau".
#   6. Mot de passe sorti du code en dur -> variable d'environnement.
#   7. Écriture ClickHouse : mode "append" au lieu de "overwrite"
#      -> les tables doivent être créées au préalable avec le
#         script sql/clickhouse.sql (ENGINE = MergeTree...).
#         Voir note en bas du script.
#   8. dim_calendar[jour] reste fixé à 1 (proxy mensuel) : c'est
#      une limite de la source (PayrollResults n'a que Month/Year),
#      pas une correction possible sans nouvelle donnée source.
# ============================================================

print("=" * 70)
print("PAYZENHR - ETL SOCLE BI (corrigé)")
print("=" * 70)


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("PayZenHR_ETL_Socle_BI")
    .config(
        "spark.jars",
        r"E:\etape_1\PayZenHR_ETL\jars\mssql-jdbc-13.4.0.jre8.jar,"
        r"E:\etape_1\PayZenHR_ETL\jars\clickhouse-jdbc-0.9.8-all.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. SQL SERVER
# ============================================================

sqlserver_url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=PayZenHR_Source;"
    "encrypt=false;"
    "trustServerCertificate=true;"
)

# CORRECTION #6 : plus de mot de passe en clair dans le code.
# Définir la variable d'environnement avant de lancer le script, ex (PowerShell) :
#   $env:PAYZENHR_SQL_PASSWORD = "votre_mot_de_passe"
sqlserver_properties = {
    "user": "sa",
    "password": os.environ["PAYZENHR_SQL_PASSWORD"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# ============================================================
# 3. CLICKHOUSE
# ============================================================

clickhouse_url = "jdbc:clickhouse://localhost:8124/payzen"

clickhouse_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": os.environ.get("PAYZENHR_CLICKHOUSE_PASSWORD", ""),
}


# ============================================================
# 4. FONCTION EXTRACTION SQL SERVER
# ============================================================

def read_sqlserver(table_name):
    print(f"\nLecture SQL Server : {table_name}")
    df = spark.read.jdbc(
        url=sqlserver_url,
        table=f"dbo.{table_name}",
        properties=sqlserver_properties
    )
    print(f"{table_name} : {df.count()} lignes")
    return df


# ============================================================
# 5. EXTRACTION
# ============================================================

print("\n" + "=" * 70)
print("EXTRACTION")
print("=" * 70)

companies_df = read_sqlserver("Companies")
employees_df = read_sqlserver("Employees")
departements_df = read_sqlserver("Departements")
job_positions_df = read_sqlserver("JobPositions")
payroll_df = read_sqlserver("PayrollResults")
employee_salaries_df = read_sqlserver("EmployeeSalaries")
employee_salary_components_df = read_sqlserver("EmployeeSalaryComponents")
pay_components_df = read_sqlserver("PayComponents")

print("\n===== COLONNES EMPLOYEES =====")
print(employees_df.columns)
print("\n===== COLONNES JOB POSITIONS =====")
print(job_positions_df.columns)
print("\n===== COLONNES EmployeeSalaryComponents =====")
print(employee_salary_components_df.columns)
print("\n===== COLONNES EmployeeSalaries =====")
print(employee_salaries_df.columns)
print("\n===== COLONNES PayComponents =====")
print(pay_components_df.columns)


# ============================================================
# 6. DIM_CLIENTS
# ============================================================

print("\n" + "=" * 70)
print("DIM_CLIENTS")
print("=" * 70)

# Cabinet BEFEC = ManagedByCompanyId 30
clients_df = companies_df.filter(col("ManagedByCompanyId") == 30)

# CORRECTION #4 : cast explicite de la colonne "taille" quand absente,
# pour éviter un type NullType qui casse l'écriture JDBC ClickHouse.
taille_col = (
    col("CompanySize").cast("string")
    if "CompanySize" in companies_df.columns
    else lit(None).cast("string")
).alias("taille")

dim_clients_df = (
    clients_df
    .select(
        col("Id").alias("client_id"),
        col("CompanyName").alias("nom"),
        col("BusinessSector").alias("secteur"),
        taille_col,
        col("ManagedByCompanyId").alias("cabinet_id")
    )
)

dim_clients_df.show(50, truncate=False)
print("Nombre de clients :", dim_clients_df.count())


# ============================================================
# 7. DIM_EMPLOYEES
# ============================================================

print("\n" + "=" * 70)
print("DIM_EMPLOYEES")
print("=" * 70)

# IMPORTANT (inchangé du script original) :
# Employees ne possède PAS JobPositionId dans la source actuelle.
# On ne peut donc pas renseigner "poste" ni "niveau" pour l'instant.
#
# CORRECTION #5 : on ajoute quand même les colonnes attendues par le
# cahier des charges (poste, niveau, date_embauche), en NULL explicite,
# pour que le SCHÉMA de la table soit conforme au .md dès maintenant.
# Aucune donnée n'est inventée -> à peupler une fois la source
# identifiée avec Mohamed (JobPositions ? autre table ?).

dim_employees_df = (
    employees_df
    .join(
        departements_df,
        employees_df.DepartementId == departements_df.Id,
        "left"
    )
    .select(
        employees_df.Id.alias("employee_id"),
        concat_ws(" ", employees_df.FirstName, employees_df.LastName).alias("nom"),
        employees_df.CompanyId.alias("client_id"),
        employees_df.DepartementId.alias("departement_id"),
        departements_df.DepartementName.alias("service"),
        employees_df.Matricule.alias("matricule"),
        employees_df.CreatedAt.alias("date_creation"),
        employees_df.DeletedAt.alias("date_suppression"),
        # --- colonnes attendues par le cahier des charges, non disponibles côté source ---
        lit(None).cast("string").alias("poste"),          # Information non précisée : source à confirmer avec Mohamed
        lit(None).cast("string").alias("niveau"),          # Information non précisée : source à confirmer avec Mohamed
        lit(None).cast("timestamp").alias("date_embauche"),  # Information non précisée : source à confirmer avec Mohamed
    )
)

dim_employees_df.show(50, truncate=False)
print("Nombre employés :", dim_employees_df.count())


# ============================================================
# 8. DIM_CALENDAR
# ============================================================

print("\n" + "=" * 70)
print("DIM_CALENDAR")
print("=" * 70)

# Le cahier des charges demande : date complète, jour, mois, trimestre, année.
# PayrollResults ne fournit que Month + Year -> "jour" est fixé à 1
# (premier jour du mois). C'est une limite de la source, pas une
# correction possible sans nouvelle donnée (voir note en fin de fichier).

calendar_source_df = (
    payroll_df
    .select(col("Month").alias("mois"), col("Year").alias("annee"))
    .filter(col("mois").isNotNull() & col("annee").isNotNull())
    .distinct()
)

dim_calendar_df = (
    calendar_source_df
    .withColumn("date", to_date(concat_ws("-", col("annee"), col("mois"), lit("01"))))
    .withColumn("jour", lit(1))
    .withColumn("trimestre", quarter(col("date")))
    .select("date", "jour", "mois", "trimestre", "annee")
)

dim_calendar_df.show(100, truncate=False)
print("Nombre périodes :", dim_calendar_df.count())


# ============================================================
# 9. DIM_COMPOSANT_PAIE
# ============================================================

print("\n" + "=" * 70)
print("DIM_COMPOSANT_PAIE")
print("=" * 70)

# PayComponents contient 0 ligne actuellement -> on utilise ComponentType
# comme identifiant technique temporaire (documenté dans le .md, section
# "Étape 0 obligatoire" : à valider avec Mohamed si PayLang expose une
# vraie table de composants).

dim_composant_paie_df = (
    employee_salary_components_df
    .select(
        col("ComponentType").cast("string").alias("composant_id"),
        col("ComponentType").cast("string").alias("libelle"),
        when(col("IsSocial") == True, lit("Charge"))
        .when(col("IsTaxable") == True, lit("Brut/Déduction"))
        .otherwise(lit("Autre"))
        .alias("categorie"),
    )
    .filter(col("composant_id").isNotNull())
    .dropDuplicates(["composant_id"])
)

dim_composant_paie_df.show(100, truncate=False)
print("Nombre composants :", dim_composant_paie_df.count())


# ============================================================
# 10. FACT_BULLETIN_LIGNE
# ============================================================

print("\n" + "=" * 70)
print("FACT_BULLETIN_LIGNE")
print("=" * 70)

# ============================================================
# Relation :
#   EmployeeSalaryComponents -> EmployeeSalaryId -> EmployeeSalaries
#   EmployeeSalaries         -> EmployeeId       -> Employees
# ============================================================

# CORRECTION #3 : on sélectionne explicitement les colonnes utiles de
# chaque DataFrame AVANT la jointure esc/es, pour éliminer tout risque
# de colonne "EmployeeId" ambiguë (EmployeeSalaries a probablement
# elle aussi une colonne EmployeeId).

esc_df = employee_salary_components_df.select(
    col("Id").alias("esc_id"),
    col("EmployeeSalaryId"),
    col("ComponentType"),
    col("Amount").alias("montant"),
    col("EffectiveDate").alias("date_effet"),
    col("EndDate").alias("date_fin"),
    col("IsTaxable").alias("is_taxable"),
    col("IsSocial").alias("is_social"),
    col("IsCIMR").alias("is_cimr"),
)

es_df = employee_salaries_df.select(
    col("Id").alias("employee_salary_id"),
    col("EmployeeId").alias("es_employee_id"),
)

salary_components_df = esc_df.join(
    es_df,
    esc_df.EmployeeSalaryId == es_df.employee_salary_id,
    "inner"
)

fact_bulletin_ligne_df = (
    salary_components_df
    .join(
        employees_df.select(
            col("Id").alias("e_id"),
            col("FirstName"),
            col("LastName"),
            col("CompanyId"),
            col("DepartementId"),
        ),
        salary_components_df.es_employee_id == col("e_id"),
        "left"
    )
    .join(
        clients_df.select(col("Id").alias("c_id"), col("CompanyName").alias("client_name")),
        col("CompanyId") == col("c_id"),
        "inner"
    )
    .join(
        departements_df.select(col("Id").alias("d_id"), col("DepartementName").alias("service")),
        col("DepartementId") == col("d_id"),
        "left"
    )
    .select(
        col("e_id").alias("employee_id"),
        concat_ws(" ", col("FirstName"), col("LastName")).alias("employee_name"),
        col("c_id").alias("client_id"),
        col("client_name"),
        col("service"),
        col("ComponentType").cast("string").alias("composant_id"),
        col("ComponentType").cast("string").alias("composant_libelle"),
        col("montant"),
        col("employee_salary_id"),
        col("date_effet"),
        col("date_fin"),
        col("is_taxable"),
        col("is_social"),
        col("is_cimr"),
    )
    .withColumn("mois", month(col("date_effet")))
    .withColumn("annee", year(col("date_effet")))
)

fact_bulletin_ligne_df.show(100, truncate=False)
print("Nombre lignes fact_bulletin_ligne :", fact_bulletin_ligne_df.count())


# ============================================================
# 11. CONTROLES SOCLE
# ============================================================

print("\n" + "=" * 70)
print("CONTROLES DU SOCLE")
print("=" * 70)

print("\nDIM_CLIENTS :", dim_clients_df.count())
print("DIM_EMPLOYEES :", dim_employees_df.count())
print("DIM_CALENDAR :", dim_calendar_df.count())
print("DIM_COMPOSANT_PAIE :", dim_composant_paie_df.count())
print("FACT_BULLETIN_LIGNE :", fact_bulletin_ligne_df.count())


# ============================================================
# 12. CONTROLE DES MONTANTS
# ============================================================

print("\n===== MONTANTS PAR COMPOSANT =====")
(
    fact_bulletin_ligne_df
    .groupBy("composant_id", "composant_libelle")
    .agg({"montant": "sum"})
    .orderBy("composant_id")
    .show(100, truncate=False)
)


# ============================================================
# 13. CONTROLE CLIENTS
# ============================================================

print("\n===== FACT PAR CLIENT =====")
(
    fact_bulletin_ligne_df
    .groupBy("client_id", "client_name")
    .count()
    .orderBy("client_id")
    .show(100, truncate=False)
)


# ============================================================
# 14. CONTROLE EMPLOYES
# ============================================================

print("\n===== FACT PAR EMPLOYE =====")
(
    fact_bulletin_ligne_df
    .groupBy("employee_id", "employee_name")
    .count()
    .orderBy("employee_id")
    .show(100, truncate=False)
)   # CORRECTION #1 : parenthèse fermante ajoutée (manquait dans l'original)


# ============================================================
# 15. LOAD CLICKHOUSE
# ============================================================
#
# CORRECTION #7 : mode "append" au lieu de "overwrite".
# Le connecteur JDBC générique de Spark, en mode "overwrite", recrée
# la table SANS clause ENGINE (ex: MergeTree) attendue par ClickHouse.
# -> Créer les tables au préalable avec le script sql/clickhouse.sql
#    (CREATE TABLE ... ENGINE = MergeTree() ORDER BY (...)), puis
#    lancer ce script en "append". Voir étape 7 du cahier des charges.

print("\n" + "=" * 70)
print("CHARGEMENT CLICKHOUSE")
print("=" * 70)

dim_clients_df.write.mode("append").jdbc(
    url=clickhouse_url, table="dim_clients", properties=clickhouse_properties
)
print("✓ dim_clients chargée")

dim_employees_df.write.mode("append").jdbc(
    url=clickhouse_url, table="dim_employees", properties=clickhouse_properties
)
print("✓ dim_employees chargée")

dim_calendar_df.write.mode("append").jdbc(
    url=clickhouse_url, table="dim_calendar", properties=clickhouse_properties
)
print("✓ dim_calendar chargée")

dim_composant_paie_df.write.mode("append").jdbc(
    url=clickhouse_url, table="dim_composant_paie", properties=clickhouse_properties
)
print("✓ dim_composant_paie chargée")

fact_bulletin_ligne_df.write.mode("append").jdbc(
    url=clickhouse_url, table="fact_bulletin_ligne", properties=clickhouse_properties
)
print("✓ fact_bulletin_ligne chargée")


# ============================================================
# 16. FIN
# ============================================================

print("\n" + "=" * 70)
print("ETL SOCLE BI TERMINE")
print("=" * 70)

print("""
Tables construites :
1. dim_clients
2. dim_employees   (poste / niveau / date_embauche : NULL, source à confirmer avec Mohamed)
3. dim_calendar    (jour fixé à 1 : proxy mensuel, limite de la source PayrollResults)
4. dim_composant_paie  (composant_id basé sur ComponentType : à valider avec Mohamed, PayComponents vide)
5. fact_bulletin_ligne

ATTENTION :
dim_echeance_legale et les autres facts
(fact_echeance, fact_absence, fact_variable_collecte,
fact_notes_frais, fact_evaluation)
ne sont pas encore construits car les sources
correspondantes n'ont pas encore été identifiées
dans les extractions actuelles.

Avant de lancer ce script :
- exécuter sql/clickhouse.sql pour créer les tables cibles (ENGINE = MergeTree)
- définir la variable d'environnement PAYZENHR_SQL_PASSWORD
""")

spark.stop()
