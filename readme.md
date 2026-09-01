# PayZenHR ETL -- SQL Server -> PySpark -> ClickHouse

Pipeline PySpark qui extrait `PayZenHR_Source` (SQL Server), nettoie et
transforme les donnees, genere les cles techniques (SK) et charge le
modele en etoile dans ClickHouse (`payzenhr_dw`), conformement a :

- `Audit_Structurel_BDD.md` (profilage reel des 103 tables source)
- `Modele_DW_Groupes_ABCD.md` (selection des tables retenues)
- `Catalogue_KPI.md` (KPI reellement calculables)
- `PayZenHR_Conception_DW.md` (modele conceptuel, plan de nettoyage, jointures ETL, controles qualite)
- `payzenhr_dw_clickhouse_schema.sql` **(source de verite du schema cible -- copie dans ce projet)**

## Principe directeur

Le fichier `target_schema.py` est **genere automatiquement** depuis
`payzenhr_dw_clickhouse_schema.sql` (via `tools/extract_target_schema.py`)
et sert de reference stricte : chaque DataFrame final (dimension ou fait)
est valide par `utils.schema_validation.validate_schema()` avant tout
chargement -- toute colonne manquante, en trop, ou tout ecart avec le SQL
cible fait echouer le pipeline immediatement plutot que de charger un
schema divergent dans ClickHouse.

Aucune colonne ni relation ne provient d'ailleurs que des fichiers
d'analyse et du profilage source deja produits.

## Structure du projet

```
payzenhr_pyspark/
├── config.py                  # Connexions SQL Server / ClickHouse (variables d'environnement)
├── spark_session.py           # Creation de la SparkSession + drivers JDBC
├── target_schema.py           # Schema cible AUTO-GENERE depuis le SQL ClickHouse (source de verite)
├── main.py                    # Orchestrateur : ordre de dependance, controles qualite, chargement
├── payzenhr_dw_clickhouse_schema.sql   # Copie du DDL cible (reference)
├── utils/
│   ├── extract.py             # Lecture JDBC SQL Server (filtre soft-delete DeletedAt)
│   ├── cleaning.py            # Trim / Title Case / cast (Partie 2 de la conception)
│   ├── keys.py                # Generation des SK, resolution des FK, dedup (regle D2)
│   ├── quality_checks.py      # Controles qualite generiques (Partie 5 de la conception)
│   ├── schema_validation.py   # Validation stricte contre target_schema.py
│   └── load_clickhouse.py     # TRUNCATE + INSERT JDBC dans ClickHouse
├── dimensions/                # Un module par dimension (16 fichiers, cf. Conception 1.2)
├── facts/                     # Un module par fait (12 fichiers, cf. Conception 1.1)
│   └── _monthly_fact_common.py   # Builder partage par les 4 faits mensuels (structure identique)
└── tools/
    └── extract_target_schema.py  # Regenere target_schema.py si le SQL cible evolue
```

## Prerequis

```bash
pip install -r requirements.txt
```

Variables d'environnement a definir (cf. `config.py`) :

```
PAYZENHR_SQLSERVER_HOST, PAYZENHR_SQLSERVER_PORT, PAYZENHR_SQLSERVER_DB,
PAYZENHR_SQLSERVER_USER, PAYZENHR_SQLSERVER_PASSWORD
PAYZENHR_CLICKHOUSE_HOST, PAYZENHR_CLICKHOUSE_HTTP_PORT, PAYZENHR_CLICKHOUSE_DB,
PAYZENHR_CLICKHOUSE_USER, PAYZENHR_CLICKHOUSE_PASSWORD
```

## Execution

1. Creer le schema cible dans ClickHouse (une seule fois, ou apres toute evolution) :
   ```bash
   clickhouse-client --multiquery < payzenhr_dw_clickhouse_schema.sql
   ```
2. Lancer le pipeline :
   ```bash
   spark-submit \
     --packages com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11,com.clickhouse:clickhouse-jdbc:0.6.3:all \
     main.py
   ```

## Ordre de dependance applique par `main.py`

```
dim_date, dim_country, dim_status, dim_alert_definition   (independantes)
   -> dim_city (dim_country)
      -> dim_company (dim_city)
         -> dim_department, dim_job_position, dim_contract_type, dim_leave_type, dim_working_calendar (dim_company)
         -> dim_holiday (dim_company, dim_country)
            -> dim_leave_type_legal_rule (dim_leave_type)
   -> dim_employee (dim_status, dim_company, dim_department, dim_city)
      -> dim_employee_contract (dim_employee, dim_company, dim_job_position, dim_contract_type)
      -> dim_employee_salary (dim_employee)
         -> fact_salary_component (dim_employee_salary)
   -> fact_payroll (dim_employee, dim_company, dim_date, dim_employee_contract)
      -> fact_payroll_premium, fact_payroll_tax_snapshot (fact_payroll, denormalisation)
   -> fact_absence_monthly, fact_leave_monthly, fact_worked_days_monthly,
      fact_overtime_monthly, fact_absence_event (dim_employee, dim_date)
   -> fact_leave_balance, fact_leave_request (dim_employee, dim_leave_type, dim_company, dim_date)
   -> fact_alert (dim_company, dim_alert_definition, dim_date)
```

## Reserves connues (a ne pas "corriger" silencieusement dans le code)

- `fact_payroll` : unicite (EmployeeId, Month, Year) NON confirmee par le profilage source -- controle qualite explicite et non-bloquant par defaut, a analyser avant mise en production.
- `fact_leave_request`, `fact_absence_event`, `fact_overtime_monthly` : volumetrie tres faible dans l'extrait profile (2, 2, 4 lignes) -- chargees mais signalees "sous le seuil de representativite" par `check_volumetry_floor`.
- `dim_employee.marital_status_label`, `dim_employee.sk_department` : NULL conserve tel quel (85%/90% NULL au profilage) -- jamais de valeur inventee.
- `fact_payroll_premium.amount` ne doit jamais etre additionne a `fact_payroll.total_primes_imposables` (double comptage documente) -- a traiter au niveau du modele semantique Power BI, pas dans l'ETL.